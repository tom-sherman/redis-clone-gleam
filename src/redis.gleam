import argv
import birl
import birl/duration
import gleam/bit_array
import gleam/bool.{guard}
import gleam/bytes_builder
import gleam/erlang/atom
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/option.{type Option, None, Some}
import gleam/order
import gleam/otp/actor
import gleam/result
import glisten.{Packet}
import mug
import redis/command
import redis/ets
import redis/resp

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

    ReplicaOf(host, port) -> {
      let assert Ok(_) =
        replication_handshake(
          master_host: host,
          master_port: port,
          port: args.port,
        )

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

fn replication_handshake(
  master_host master_host,
  master_port master_port,
  port port,
) -> Result(Nil, Nil) {
  use socket <- result.try(
    mug.new(master_host, master_port)
    |> mug.connect()
    |> result.nil_error,
  )

  use response <- result.try(
    resp.Array([resp.BulkString(<<"PING":utf8>>)])
    |> fetch_value(socket),
  )

  use <- guard(when: response != resp.SimpleString("PONG"), return: Error(Nil))

  use response <- result.try(
    resp.Array([
      resp.BulkString(<<"REPLCONF":utf8>>),
      resp.BulkString(<<"listening-port":utf8>>),
      resp.BulkString(
        port
        |> int.to_string
        |> bit_array.from_string,
      ),
    ])
    |> fetch_value(socket),
  )

  use <- guard(when: response != resp.SimpleString("OK"), return: Error(Nil))

  use response <- result.try(
    resp.Array([
      resp.BulkString(<<"REPLCONF":utf8>>),
      resp.BulkString(<<"capa":utf8>>),
      resp.BulkString(<<"psync2":utf8>>),
    ])
    |> fetch_value(socket),
  )

  use <- guard(when: response != resp.SimpleString("OK"), return: Error(Nil))

  Ok(Nil)
}

fn fetch_value(value, socket) {
  use _ <- result.try(
    value
    |> resp.to_bit_array
    |> mug.send(socket, _)
    |> result.nil_error,
  )

  use packet <- result.try(
    mug.receive(socket, timeout_milliseconds: 500)
    |> result.nil_error,
  )

  resp.from_bit_array(packet)
  |> result.nil_error
}

fn handle_message(msg, state: State, conn) {
  let assert Packet(msg) = msg

  let assert Ok(value) = resp.from_bit_array(msg)

  io.debug(value)

  let result = {
    use command <- result.try(command.parse_command(value))
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

type TableValue {
  TableValue(content: BitArray, expiry: Option(birl.Time))
}

fn handle_command(cmd, ctx: Context) {
  case cmd {
    command.Ping -> Ok(resp.SimpleString("PONG"))

    command.Echo(value) -> Ok(resp.BulkString(value))

    command.Set(key, value, px) -> {
      let expiry =
        px
        |> option.map(duration.milli_seconds)
        |> option.map(birl.add(birl.now(), _))

      ets.insert(ctx.table, key, TableValue(content: value, expiry: expiry))
      Ok(resp.SimpleString("OK"))
    }

    command.Get(key) -> {
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

    command.Info("all") | command.Info("replication") ->
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
    command.Info(_) -> Ok(resp.BulkString(<<>>))

    command.Replconf(args) ->
      case args {
        command.ListeningPort(_) -> Ok(resp.SimpleString("OK"))
        command.Capa(_) -> Ok(resp.SimpleString("OK"))
      }
  }
}
