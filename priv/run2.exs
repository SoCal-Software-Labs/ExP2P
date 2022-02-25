other = "127.0.0.1:1234"

{:ok, pid} =
  ExP2P.Dispatcher.start_link(
    bind_addr: "127.0.0.1:0",
    bootstrap_nodes: [other],
    connection_mod: ExP2P.Connection,
    connection_mod_args: %{
      new_state: fn _conn -> :ok end,
      callback: fn _endpoint, _connection, _msg, _ret, _from, _state -> :ok end
    },
    opts: []
  )

num_commands = 100

{:ok, endpoint, external_ip} = ExP2P.Dispatcher.endpoint(pid)
{:ok, connection} = ExP2P.connect(endpoint, [other])
{:ok, ret} = ExP2P.bidirectional(endpoint, connection, "MARCO", 10_000)
IO.puts("#{other} said #{ret}")
