import gleam/bit_array
import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import redis/resp

pub type ReplconfArgs {
  ListeningPort(Int)
  Capa(String)
}

pub type Command {
  Ping
  Echo(BitArray)
  Set(key: BitArray, value: BitArray, px: Option(Int))
  Get(key: BitArray)
  Info(section: String)
  Replconf(args: ReplconfArgs)
  Psync(replication_id: String, offset: Int)
}

pub fn parse_command(value) {
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
