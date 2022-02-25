{:ok, pid} =
  ExP2P.Dispatcher.start_link(
    bind_addr: "127.0.0.1:1234",
    bootstrap_nodes: [],
    connection_mod: ExP2P.Connection,
    connection_args: %{
      new_state: fn _conn -> :ok end,
      callback: fn endpoint, _connection, msg, sender, from, state ->
        IO.puts("#{from} said #{msg}")

        if msg == "MARCO" do
          :ok = ExP2P.stream_response(endpoint, sender, "POLO", 10_000)
        else
          :ok = ExP2P.stream_response(endpoint, sender, "POLO", 10_000)
        end
      end
    },
    opts: []
  )

# keep dispatcher alive
Process.sleep(1000 * 60 * 60 * 10)
