other = "127.0.0.1:1234"

{:ok, pid} =
  ExP2P.Dispatcher.start_link(
    bind_addr: "127.0.0.1:0",
    bootstrap_nodes: [other],
    connection_mod: ExP2P.Connection,
    connection_mod_args: [callback: fn _endpoint, _msg, _ret, _from -> :ok end],
    opts: []
  )

num_commands = 100

{:ok, endpoint, external_ip} = ExP2P.Dispatcher.endpoint(pid)
{:ok, connection} = ExP2P.connect(endpoint, [other])
{:ok, ret} = ExP2P.bidirectional(endpoint, connection, "hello", 10_000)

Benchee.run(
  %{
    "client" => fn num_commands ->
      Enum.map(1..num_commands, fn _ ->
        Task.async(fn ->
          {:ok, ret} = ExP2P.bidirectional(endpoint, connection, "hello", 10_000)
        end)
      end)
      |> Task.await_many()
    end
  },
  inputs: %{
    "small value #{num_commands}" => num_commands
  },
  memory_time: 3
)

# ExP2P.unidirectional(endpoint, connection, String.duplicate("h", 100), 1000)
