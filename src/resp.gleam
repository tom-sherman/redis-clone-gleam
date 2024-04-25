import gleam/bit_array
import gleam/bool
import gleam/dict
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{None, Some}
import gleam/pair
import gleam/result
import gleam/set
import gleam/string

@external(erlang, "binary", "split")
fn erl_binary_split(subject: BitArray, pattern: BitArray) -> List(BitArray)

// split_first(<<1,2,3,4,5>>, <<3,4>>) => Ok((<<1,2>>, <<5>>))
fn split_first(
  arr: BitArray,
  delimiter: BitArray,
) -> Result(#(BitArray, BitArray), Nil) {
  case erl_binary_split(arr, delimiter) {
    [head, tail] -> Ok(#(head, tail))
    _ -> Error(Nil)
  }
}

fn unfold(state, f) {
  case f(state) {
    None -> []
    Some(#(x, new_state)) -> list.append([x], unfold(new_state, f))
  }
}

pub type Value {
  Null
  Int(Int)
  BulkString(BitArray)
  Array(List(Value))
  SimpleString(String)
  Okay
  Map(dict.Dict(Value, Value))
  Set(set.Set(Value))
  Double(Float)
  Boolean(Bool)
  BigNumber(Int)
  // TODO: Push
}

pub type Error {
  SimpleError(String)
  BulkError(BitArray)
}

pub fn from_bit_array(msg: BitArray) -> Result(Value, Error) {
  use #(value, rest) <- result.try(
    msg
    |> partial_from_bit_array,
  )

  case rest {
    <<>> -> Ok(value)
    _ ->
      Error(SimpleError(
        "Failed to parse entire message: "
        <> {
          rest
          |> bit_array.to_string
          |> result.unwrap("Failed to convert to string")
        },
      ))
  }
}

fn partial_from_bit_array(msg) {
  case msg {
    <<"+":utf8, _:bits>> -> parse_simple_string(msg)
    <<"*":utf8, _:bits>> -> parse_array(msg)
    <<"$":utf8, _:bits>> -> parse_bulk_string(msg)
    _ ->
      Error(SimpleError(
        "Failed to parse partial message: "
        <> {
          msg
          |> bit_array.to_string
          |> result.unwrap("<Failed to convert to string>")
        },
      ))
  }
}

pub fn error_to_bit_array(error) -> BitArray {
  case error {
    SimpleError(s) -> simple_string_inner_to_bit_array(s)

    BulkError(b) -> bulk_string_inner_to_bit_array(b)
  }
}

fn simple_string_inner_to_bit_array(s: String) -> BitArray {
  { "+" <> s <> "\r\n" }
  |> bit_array.from_string
}

fn bulk_string_inner_to_bit_array(b: BitArray) -> BitArray {
  bit_array.concat([
    <<"$":utf8>>,
    {
      b
      |> bit_array.byte_size
      |> int.to_string
      |> bit_array.from_string
    },
    <<"\r\n":utf8>>,
    b,
    <<"\r\n":utf8>>,
  ])
}

pub fn to_bit_array(value: Value) -> BitArray {
  case value {
    SimpleString(s) -> simple_string_inner_to_bit_array(s)

    BulkString(b) -> bulk_string_inner_to_bit_array(b)

    Array(values) ->
      values
      |> list.map(to_bit_array)
      |> fn(arrays) {
        let values = bit_array.concat(arrays)

        bit_array.concat([
          <<"*":utf8>>,
          {
            arrays
            |> list.length
            |> int.to_string
            |> bit_array.from_string
          },
          <<"\r\n":utf8>>,
          values,
        ])
      }

    Null -> <<"_\r\n":utf8>>

    _ -> {
      io.debug("Failed to convert value to string: " <> inspect(value))
      panic
    }
  }
}

pub fn inspect(value: Value) -> String {
  case value {
    Null -> "null"
    Int(i) -> int.to_string(i)
    BulkString(b) ->
      "BulkString("
      <> {
        b
        |> bit_array.inspect
      }
      <> ")"
    Array(a) ->
      "Array("
      <> a
      |> list.flat_map(fn(v) { [inspect(v), ", "] })
      |> string.concat
      <> ")"
    SimpleString(s) -> "SimpleString(" <> s <> ")"
    Okay -> "Okay"
    Map(m) ->
      "Map("
      <> m
      |> dict.to_list
      |> list.flat_map(fn(entry) {
        let #(k, v) = entry
        [inspect(k), " => ", inspect(v), ", "]
      })
      |> string.concat
      <> ")"
    Set(s) ->
      "Set("
      <> s
      |> set.to_list
      |> list.flat_map(fn(v) { [inspect(v), ", "] })
      |> string.concat
      <> ")"
    Double(f) -> "Double(" <> float.to_string(f) <> ")"
    Boolean(b) -> "Boolean(" <> bool.to_string(b) <> ")"
    BigNumber(i) -> "BigNumber(" <> int.to_string(i) <> ")"
  }
}

fn parse_simple_string(input: BitArray) -> Result(#(Value, BitArray), Error) {
  case input {
    <<"+":utf8, rest:bits>> -> {
      use #(value, rest) <- result.try(
        rest
        |> split_first(<<"\r\n":utf8>>)
        |> result.map_error(fn(_) {
          SimpleError("Couldn't find CRLF in simple string")
        }),
      )
      use value <- result.try(
        value
        |> bit_array.to_string
        |> result.map_error(fn(_) {
          SimpleError("Failed to parse simple string value")
        }),
      )

      Ok(#(SimpleString(value), rest))
    }

    _ -> Error(SimpleError("Expected simple string to start with +"))
  }
}

fn parse_array(input: BitArray) -> Result(#(Value, BitArray), Error) {
  use rest <- result.try(case input {
    <<"*":utf8, rest:bits>> -> Ok(rest)
    _ -> Error(SimpleError("Passed a non array"))
  })

  use #(length, rest) <- result.try(
    rest
    |> split_first(<<"\r\n":utf8>>)
    |> result.map_error(fn(_) { SimpleError("Failed to parse array length") }),
  )

  use length <- result.try(
    length
    |> bit_array.to_string
    |> result.map(int.parse)
    |> result.flatten
    |> result.map_error(fn(_) { SimpleError("Failed to parse array length") }),
  )

  case length {
    0 -> Ok(#(Array([]), rest))
    _ -> {
      // This unfold nonsense is annoying. Surely there's a better way to do this?
      let frames =
        unfold(#(length, rest), fn(state) {
          case state {
            #(0, _) -> None
            #(i, array) ->
              case partial_from_bit_array(array) {
                Ok(#(value, rest)) ->
                  Some(#(#(Ok(value), rest), #(i - 1, rest)))
                Error(e) -> Some(#(#(Error(e), rest), #(0, rest)))
              }
          }
        })

      use #(_, rest) <- result.try(
        frames
        |> list.last
        |> result.map_error(fn(_) { SimpleError("Failed to parse array") }),
      )

      use values <- result.try(
        frames
        |> list.map(pair.first)
        |> result.all,
      )

      Ok(#(Array(values), rest))
    }
  }
}

fn parse_bulk_string(input: BitArray) -> Result(#(Value, BitArray), Error) {
  use rest <- result.try(case input {
    <<"$":utf8, rest:bits>> -> Ok(rest)
    _ -> Error(SimpleError("Passed a non bulk string"))
  })

  use #(length, rest) <- result.try(
    rest
    |> split_first(<<"\r\n":utf8>>)
    |> result.map_error(fn(_) { SimpleError("Failed to parse array length") }),
  )

  use length <- result.try(
    length
    |> bit_array.to_string
    |> result.map(int.parse)
    |> result.flatten
    |> result.map_error(fn(_) { SimpleError("Failed to parse array length") }),
  )

  use #(data, rest) <- result.try(case rest {
    <<head:bytes-size(length), tail:bits>> -> Ok(#(head, tail))
    _ -> Error(SimpleError("Failed to parse bulk string data"))
  })

  case rest {
    <<"\r\n":utf8, rest:bits>> -> Ok(#(BulkString(data), rest))
    _ -> Error(SimpleError("Failed to parse bulk string CRLF"))
  }
}
