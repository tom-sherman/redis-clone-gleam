import argv
import birl
import birl/duration
import gleam/bit_array
import gleam/bytes_builder
import gleam/erlang/atom
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/option.{type Option, None, Some}
import gleam/order
import gleam/otp/actor
import gleam/result
import gleam/string
import glisten.{Packet}
import mug
import redis/ets
import resp

type Role {
  ReplicaOf(host: String, port: Int)
  Master(id: String, offset: Int)
}

type Args {
  Args(port: Int, role: Role)
}

fn arg_parser() -> Result(Args, Nil) {
  arg_parser_loop(
    argv.load().arguments,
    Args(
      port: 6379,
      role: Master(id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", offset: 0),
    ),
  )
}

fn arg_parser_loop(args: List(String), props: Args) -> Result(Args, Nil) {
  case args {
    ["--port", port_value, ..rest] | ["-p", port_value, ..rest] -> {
      int.parse(port_value)
      |> result.map(fn(port) { Args(..props, port: port) })
      |> result.try(fn(new_props) { arg_parser_loop(rest, new_props) })
    }
    ["--replicaof", master_host, master_port_value, ..rest] -> {
      int.parse(master_port_value)
      |> result.map(fn(master_port) {
        Args(..props, role: ReplicaOf(host: master_host, port: master_port))
      })
      |> result.try(fn(new_props) { arg_parser_loop(rest, new_props) })
    }
    _ -> Ok(props)
  }
}

type Context {
  Context(table: ets.Set(BitArray, TableValue), role: Role)
}

type State {
  Default(ctx: Context)
}

pub fn main() {
  let assert Ok(args) = arg_parser()

  let ctx =
    Context(table: ets.new(atom.create_from_string("redis")), role: args.role)

  let initial_state = case args.role {
    Master(_, _) -> Default(ctx)

    ReplicaOf(master_host, master_port) -> {
      // Replication handshake

      let assert Ok(socket) =
        mug.new(master_host, master_port)
        |> mug.connect()
      io.println("Connected to master")

      let assert Ok(_) =
        resp.Array([resp.BulkString(<<"PING":utf8>>)])
        |> resp.to_bit_array
        |> mug.send(socket, _)
      io.println("Sent PING")

      let assert Ok(packet) = mug.receive(socket, timeout_milliseconds: 500)
      let assert Ok(resp.SimpleString("PONG")) =
        packet
        |> resp.from_bit_array
      io.println("Received PONG")

      let assert Ok(_) =
        resp.Array([
          resp.BulkString(<<"REPLCONF":utf8>>),
          resp.BulkString(<<"listening-port":utf8>>),
          resp.BulkString(
            args.port
            |> int.to_string
            |> bit_array.from_string,
          ),
        ])
        |> resp.to_bit_array
        |> mug.send(socket, _)
      io.println("Sent REPLCONF listening-port")

      let assert Ok(packet) = mug.receive(socket, timeout_milliseconds: 500)
      let assert Ok(resp.SimpleString("OK")) =
        packet
        |> resp.from_bit_array
      io.println("Received first OK")

      let assert Ok(_) =
        resp.Array([
          resp.BulkString(<<"REPLCONF":utf8>>),
          resp.BulkString(<<"capa":utf8>>),
          resp.BulkString(<<"psync2":utf8>>),
        ])
        |> resp.to_bit_array
        |> mug.send(socket, _)
      io.println("Sent REPLCONF capa")

      let assert Ok(packet) = mug.receive(socket, timeout_milliseconds: 500)
      let assert Ok(resp.SimpleString("OK")) =
        packet
        |> resp.from_bit_array
      io.println("Received second OK")

      Default(ctx)
    }
  }

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(initial_state, None) }, handle_message)
    |> glisten.serve(args.port)

  io.println(
    "Running on port: "
    <> args.port
    |> int.to_string,
  )
  process.sleep_forever()
}

fn handle_message(msg, state: State, conn) {
  let assert Packet(msg) = msg

  let assert Ok(value) = resp.from_bit_array(msg)

  let result = {
    use command <- result.try(parse_command(value))
    handle_command(command, state.ctx)
  }

  let output =
    result
    |> result.map(resp.to_bit_array)
    |> unwrap_or_else(fn(error) { resp.error_to_bit_array(error) })

  let assert Ok(_) = glisten.send(conn, bytes_builder.from_bit_array(output))

  actor.continue(state)
}

fn unwrap_or_else(result, f) {
  case result {
    Ok(value) -> value
    Error(error) -> f(error)
  }
}

type ReplconfArgs {
  ListeningPort(Int)
  Capa(String)
}

type Command {
  Ping
  Echo(BitArray)
  Set(key: BitArray, value: BitArray, px: Option(Int))
  Get(key: BitArray)
  Info(section: String)
  Replconf(args: ReplconfArgs)
}

fn parse_command(value) {
  io.debug(value)
  case value {
    resp.Array([resp.BulkString(command_name), ..args]) -> {
      use command_name <- result.try(
        bit_array.to_string(command_name)
        |> result.map_error(fn(_) {
          resp.SimpleError("ERR invalid command name")
        }),
      )

      case string.uppercase(command_name) {
        "PING" ->
          case args {
            [] -> Ok(Ping)
            _ ->
              Error(resp.SimpleError(
                "ERR wrong number of arguments for 'ping' command",
              ))
          }

        "ECHO" ->
          case args {
            [resp.BulkString(value)] -> Ok(Echo(value))
            _ ->
              Error(resp.SimpleError(
                "ERR wrong number of arguments for 'echo' command",
              ))
          }

        "SET" ->
          case args {
            [resp.BulkString(key), resp.BulkString(value), ..options] ->
              case options {
                [] -> Ok(Set(key, value, px: None))

                [resp.BulkString(maybe_px_option), resp.BulkString(px)] -> {
                  use px_option <- result.try(
                    bit_array.to_string(maybe_px_option)
                    |> result.map_error(fn(_) {
                      resp.SimpleError("ERR invalid command name")
                    }),
                  )

                  use px <- result.try(
                    px
                    |> bit_array.to_string
                    |> result.then(int.parse)
                    |> result.map_error(fn(_) {
                      resp.SimpleError("ERR invalid px value")
                    }),
                  )

                  case string.uppercase(px_option) {
                    "PX" -> Ok(Set(key, value, px: Some(px)))
                    _ ->
                      Error(resp.SimpleError(
                        "ERR expected px option for 'set' command",
                      ))
                  }
                }

                _ ->
                  Error(resp.SimpleError(
                    "ERR expected px option for 'set' command",
                  ))
              }

            _ ->
              Error(resp.SimpleError(
                "ERR wrong number of arguments for 'set' command",
              ))
          }

        "GET" ->
          case args {
            [resp.BulkString(key)] -> Ok(Get(key))
            _ ->
              Error(resp.SimpleError(
                "ERR wrong number of arguments for 'get' command",
              ))
          }

        "INFO" ->
          case args {
            // Ignore section for now
            [] -> Ok(Info("all"))
            [resp.BulkString(section_bits)] -> {
              use section <- result.try(
                bit_array.to_string(section_bits)
                |> result.map_error(fn(_) {
                  resp.SimpleError("ERR invalid section")
                }),
              )

              Ok(Info(section))
            }
            _ ->
              Error(resp.SimpleError(
                "ERR wrong number of arguments for 'info' command",
              ))
          }

        "REPLCONF" ->
          case args {
            [resp.BulkString(arg1), resp.BulkString(arg2)] ->
              {
                use arg1 <- result.try(bit_array.to_string(arg1))
                use arg2 <- result.try(bit_array.to_string(arg2))
                case arg1 {
                  "listening-port" -> {
                    use port <- result.try(int.parse(arg2))
                    Ok(Replconf(ListeningPort(port)))
                  }

                  "capa" -> Ok(Replconf(Capa(arg2)))

                  _ -> Error(Nil)
                }
              }
              |> result.map_error(fn(_) {
                resp.SimpleError("ERR invalid argument")
              })

            _ ->
              Error(resp.SimpleError(
                "ERR wrong number of arguments for 'replconf' command",
              ))
          }

        _ ->
          Error(resp.SimpleError(
            "ERR unknown command name: " <> string.uppercase(command_name),
          ))
      }
    }

    _ -> Error(resp.SimpleError("ERR unknown command"))
  }
}

type TableValue {
  TableValue(content: BitArray, expiry: Option(birl.Time))
}

fn handle_command(cmd, ctx: Context) {
  case cmd {
    Ping -> Ok(resp.SimpleString("PONG"))

    Echo(value) -> Ok(resp.BulkString(value))

    Set(key, value, px) -> {
      let expiry =
        px
        |> option.map(duration.milli_seconds)
        |> option.map(birl.add(birl.now(), _))

      ets.insert(ctx.table, key, TableValue(content: value, expiry: expiry))
      Ok(resp.SimpleString("OK"))
    }

    Get(key) -> {
      Ok(
        ets.lookup(ctx.table, key)
        |> result.map(fn(v) {
          case v.expiry {
            None -> resp.BulkString(v.content)
            Some(expiry) ->
              case birl.compare(expiry, birl.now()) {
                order.Lt -> {
                  ets.delete(ctx.table, key)
                  resp.Null
                }
                _ -> resp.BulkString(v.content)
              }
          }
        })
        |> result.unwrap(resp.Null),
      )
    }

    Info("all") | Info("replication") ->
      Ok(
        resp.BulkString(case ctx.role {
          ReplicaOf(_, _) -> <<"role:slave":utf8>>
          Master(id, offset) ->
            bit_array.from_string(
              "role:master"
              <> "\n"
              <> "master_replid:"
              <> id
              <> "\n"
              <> "master_repl_offset:"
              <> int.to_string(offset),
            )
        }),
      )
    Info(_) -> Ok(resp.BulkString(<<>>))

    Replconf(args) ->
      case args {
        ListeningPort(_) -> Ok(resp.SimpleString("OK"))
        Capa(_) -> Ok(resp.SimpleString("OK"))
      }
  }
}
