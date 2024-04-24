import gleam/bytes_builder
import gleam/erlang/process
import gleam/list
import gleam/option.{None}
import gleam/otp/actor

import glisten.{Packet}

pub fn main() {
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, handle_message)
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn handle_message(msg, state, conn) {
  let assert Packet(msg) = msg

  let output =
    parse_message(msg)
    |> list.map(handle_command)
    |> list.fold("", fn(acc, cmd) { acc <> cmd <> "\r\n" })

  let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(output))

  actor.continue(state)
}

type Command {
  Ping
}

fn handle_command(cmd) -> String {
  case cmd {
    Ping -> "+PONG"
  }
}

fn parse_message(_msg: BitArray) -> List(Command) {
  [Ping]
  // let assert Ok(str) = bit_array.to_string(msg)

  // str
  // |> string.trim
  // |> string.split("\n")
  // |> list.map(fn(line) {
  //   io.debug("line: " <> line)
  //   case line {
  //     "PING" -> Ping
  //     _ -> panic
  //   }
  // })
}
