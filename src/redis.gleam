import gleam/bytes_builder
import gleam/erlang/process
import gleam/option.{None}
import gleam/otp/actor
import glisten

pub fn main() {
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, handle_message)
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn handle_message(_msg, state, conn) {
  let assert Ok(_) = glisten.send(conn, bytes_builder.from_string("+PONG\r\n"))

  actor.continue(state)
}
