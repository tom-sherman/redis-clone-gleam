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
import redis/ets
import resp

type Context {
  Context(table: ets.Set(BitArray, TableValue))
}

type State {
  Default(ctx: Context)
}

pub fn main() {
  let port =
    case argv.load().arguments {
      ["--port", port] -> port
      _ -> "6379"
    }
    |> int.parse
    |> result.unwrap(6379)

  let initial_state =
    Default(Context(table: ets.new(atom.create_from_string("redis"))))

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(initial_state, None) }, handle_message)
    |> glisten.serve(port)

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

type Command {
  Ping
  Echo(BitArray)
  Set(key: BitArray, value: BitArray, px: Option(Int))
  Get(key: BitArray)
  Info(section: String)
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
      Ok(resp.BulkString(<<"role:master":utf8>>))
    Info(_) -> Ok(resp.BulkString(<<>>))
  }
}
