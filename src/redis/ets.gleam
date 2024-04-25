import gleam/dynamic
import gleam/erlang/atom
import gleam/io

pub type Props {
  Private
  Protected
  Public
}

pub opaque type Table(k, v) {
  Table(name: atom.Atom)
}

pub fn new(name: atom.Atom) {
  [
    atom.create_from_string("set")
      |> dynamic.from,
    atom.create_from_string("named_table")
      |> dynamic.from,
    atom.create_from_string("public")
      |> dynamic.from,
  ]
  |> new_table(name, _)

  // Set(Table(tbl))
  Set(Table(name))
}

pub type Set(k, v) {
  Set(table: Table(k, v))
}

@external(erlang, "ets", "insert")
fn insert_ext(table: atom.Atom, tuple: #(k, v)) -> Nil

@external(erlang, "ets", "lookup")
fn lookup_ext(table: atom.Atom, key: k) -> List(#(k, v))

@external(erlang, "ets", "delete")
fn delete_key_ext(table: atom.Atom, key: k) -> Nil

@external(erlang, "ets", "new")
fn new_table(table: atom.Atom, props: List(dynamic.Dynamic)) -> atom.Atom

/// Insert a value into the ets table.
pub fn insert(set: Set(k, v), key: k, value: v) -> Set(k, v) {
  io.debug(set)
  insert_ext(set.table.name, #(key, value))
  set
}

/// Retrieve a value from the ets table. Return an error if the value could
/// not be found.
pub fn lookup(set: Set(k, v), key: k) -> Result(v, Nil) {
  case lookup_ext(set.table.name, key) {
    [] -> Error(Nil)
    [value, ..] -> Ok(value.1)
  }
}

/// Delete all objects with key `key` from the table.
pub fn delete(set: Set(k, v), key: k) -> Set(k, v) {
  delete_key_ext(set.table.name, key)
  set
}
