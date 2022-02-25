{:ok, db} = SortedSetKV.open("data/testdb")
:ok = SortedSetKV.zadd(db, "mycollection", "hello", String.duplicate("world", 1), 43, false)

{:ok, pid} =
  ExP2P.Dispatcher.start_link(
    "127.0.0.1:1234",
    [],
    ExP2P.Connection,
    [
      fn endpoint, b, c, from ->
        if c != nil do
          Task.start(fn ->
            {k, _} = SortedSetKV.zgetbykey(db, "mycollection", b, 0)
            :ok = ExP2P.stream_response(endpoint, c, k, 10_000)
          end)
        end

        :ok
      end
    ],
    []
  )

Process.sleep(1000 * 60 * 60 * 10)
