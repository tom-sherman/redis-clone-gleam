import gleam/bit_array
import gleam/bytes_builder
import gleam/erlang/atom
import gleam/erlang/process
import gleam/io
import gleam/option.{None}
import gleam/otp/actor
import gleam/result
import gleam/string
import redis/ets
import resp

import glisten.{Packet}

pub fn main() {
  let table = ets.new(atom.create_from_string("redis"))

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, state, conn) {
      handle_message(msg, state, conn, table)
    })
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn handle_message(msg, state, conn, table) {
  let assert Packet(msg) = msg

  let assert Ok(value) = resp.from_bit_array(msg)

  let result = {
    use command <- result.try(parse_command(value))
    handle_command(command, table)
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
  Set(key: BitArray, value: BitArray)
  Get(key: BitArray)
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
            [resp.BulkString(key), resp.BulkString(value)] ->
              Ok(Set(key, value))
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

        _ -> Error(resp.SimpleError("ERR unknown command name"))
      }
    }

    _ -> Error(resp.SimpleError("ERR unknown command"))
  }
}

fn handle_command(cmd, table) {
  case cmd {
    Ping -> Ok(resp.SimpleString("PONG"))
    Echo(value) -> Ok(resp.BulkString(value))
    Set(key, value) -> {
      ets.insert(table, key, value)
      Ok(resp.SimpleString("OK"))
    }
    Get(key) ->
      Ok(
        ets.lookup(table, key)
        |> result.map(resp.BulkString)
        |> result.unwrap(resp.Null),
      )
  }
}
