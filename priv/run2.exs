other = "127.0.0.1:1234"

{:ok, pid} =
  ExP2P.Dispatcher.start_link(
    "127.0.0.1:0",
    [],
    ExP2P.Connection,
    [
      fn a, b, c ->
        IO.inspect({a, b, c})
        :ok
      end
    ],
    [other]
  )

num_commands = 100

{:ok, endpoint} = ExP2P.Dispatcher.endpoint(pid)
{:ok, connection} = ExP2P.connect(endpoint, [other])
{:ok, ret} = ExP2P.bidirectional(endpoint, connection, "hello", 10_000)
IO.inspect(ret)

Benchee.run(
  %{
    "client" => fn num_commands ->
      Enum.map(1..num_commands, fn _ ->
        {:ok, ret} = ExP2P.bidirectional(endpoint, connection, "hello", 10_000)
      end)
    end
  },
  inputs: %{
    "small value #{num_commands}" => num_commands
  },
  memory_time: 3
)

# ExP2P.unidirectional(endpoint, connection, String.duplicate("h", 100), 1000)
