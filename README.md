# ExP2p

Fast, multiplexed, Peer to Peer communication for Elixir based on [qP2P](https://github.com/maidsafe/qp2p).

Features (copied from QP2P):


* Establishing encrypted, uni- and bidirectional connections (using QUIC with TLS 1.3).
* Connection pooling, for reusing already opened connections.
* Fault-tolerance, including retries and racing connections against sets of peers.
* Configuring UPnP and validating external connectivity.
* Sending discrete messages (using QUIC streams as 'framing').


## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ex_p2p` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_p2p, "~> 0.1.0"}
  ]
end
```

## Example

On one instance run:

```elixir
{:ok, pid} =
  ExP2P.Dispatcher.start_link(
    bind_addr: "127.0.0.1:1234",
    bootstrap_nodes: [],
    connection_mod: ExP2P.Connection,
    connection_mod_args: %{
      new_state: fn _conn -> :ok end,
      callback: fn endpoint, _connection, msg, sender, from, state ->
        IO.puts("#{from} said #{msg}")

        if msg == "MARCO" do
          :ok = ExP2P.stream_send(endpoint, sender, "POLO", 10_000)
        else
          :ok = ExP2P.stream_send(endpoint, sender, "POLO", 10_000)
        end
      end
    },
    opts: []
  )
```

On the other other instance run:

```elixir
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

{:ok, endpoint, _external_ip} = ExP2P.Dispatcher.endpoint(pid)
{:ok, connection} = ExP2P.connect(endpoint, [other])
{:ok, ret} = ExP2P.bidirectional(endpoint, connection, "MARCO", 10_000)
IO.puts("#{other} said #{ret}")
```