import gleam/dict
import gleeunit
import gleeunit/should
import redis/resp

pub fn main() {
  gleeunit.main()
}

fn string_to_bit_array(s) {
  <<s:utf8>>
}

pub fn simple_string_test() {
  "+hello\r\n"
  |> string_to_bit_array
  |> resp.from_bit_array
  |> should.be_ok
  |> should.equal(resp.SimpleString("hello"))
}

pub fn empty_array_test() {
  "*0\r\n"
  |> string_to_bit_array
  |> resp.from_bit_array
  |> should.be_ok
  |> should.equal(resp.Array([]))
}

pub fn array_test() {
  let hello_world =
    resp.Array([resp.SimpleString("hello"), resp.SimpleString("world")])

  "*2\r\n+hello\r\n+world\r\n"
  |> string_to_bit_array
  |> resp.from_bit_array
  |> should.be_ok
  |> should.equal(hello_world)

  // Nested arrays
  "*2\r\n*2\r\n+hello\r\n+world\r\n*2\r\n+hello\r\n+world\r\n"
  |> string_to_bit_array
  |> resp.from_bit_array
  |> should.be_ok
  |> should.equal(resp.Array([hello_world, hello_world]))
}

pub fn ping_command_test() {
  "*1\r\n$4\r\nping\r\n"
  |> string_to_bit_array
  |> resp.from_bit_array
  |> should.be_ok
  |> should.equal(resp.Array([resp.BulkString(<<"ping":utf8>>)]))
}

pub fn integer_test() {
  ":1234\r\n"
  |> string_to_bit_array
  |> resp.from_bit_array
  |> should.be_ok
  |> should.equal(resp.Integer(1234))

  ":-1234\r\n"
  |> string_to_bit_array
  |> resp.from_bit_array
  |> should.be_ok
  |> should.equal(resp.Integer(-1234))

  ":-0\r\n"
  |> string_to_bit_array
  |> resp.from_bit_array
  |> should.be_ok
  |> should.equal(resp.Integer(0))

  ":+0\r\n"
  |> string_to_bit_array
  |> resp.from_bit_array
  |> should.be_ok
  |> should.equal(resp.Integer(0))
}

pub fn map_test() {
  "%2\r\n+first\r\n:1\r\n:2\r\n+second\r\n"
  |> string_to_bit_array
  |> resp.from_bit_array
  |> should.be_ok
  |> should.equal(resp.Map(
    [
      #(resp.SimpleString("first"), resp.Integer(1)),
      #(resp.Integer(2), resp.SimpleString("second")),
    ]
    |> dict.from_list,
  ))
}

// to_bit_array tests

pub fn simple_string_to_bit_array_test() {
  resp.SimpleString("hello")
  |> resp.to_bit_array
  |> should.equal(<<"+hello\r\n":utf8>>)
}

pub fn empty_array_to_bit_array_test() {
  resp.Array([])
  |> resp.to_bit_array
  |> should.equal(<<"*0\r\n":utf8>>)
}

pub fn array_with_bulk_string_to_bit_array_test() {
  resp.Array([resp.BulkString(<<"hello":utf8>>)])
  |> resp.to_bit_array
  |> should.equal(<<"*1\r\n$5\r\nhello\r\n":utf8>>)
}
