defmodule ExP2P.Dispatcher do
  use GenServer

  require Logger

  def start_link(opts) do
    bind_addr = Keyword.fetch!(opts, :bind_addr)
    other_opts = Keyword.fetch!(opts, :opts)
    bootstrap = Keyword.fetch!(opts, :bootstrap_nodes)
    client_mode = Keyword.fetch!(opts, :client_mode)
    port_forward = Keyword.fetch!(opts, :port_forward)
    connection_mod = Keyword.fetch!(opts, :connection_mod)
    connection_args = Keyword.fetch!(opts, :connection_mod_args)

    GenServer.start_link(
      __MODULE__,
      [bind_addr, bootstrap, connection_mod, connection_args, client_mode, port_forward],
      other_opts
    )
  end

  def endpoint(pid), do: GenServer.call(pid, :endpoint, :infinity)

  def init([bind_addr, bootstrap, connection_mod, connection_args, client_mode, port_forward]) do
    {:ok, pid} = DynamicSupervisor.start_link(strategy: :one_for_one)
    :ok = ExP2P.start(self(), bind_addr, bootstrap, client_mode, port_forward)

    receive do
      {:new_endpoint, endpoint, bind_addr} ->
        Logger.info("Listening on #{bind_addr}")

        {:ok,
         %{
           endpoint: endpoint,
           bind_addr: bind_addr,
           supervisor: pid,
           connection_mod: connection_mod,
           connection_args: connection_args
         }}

      {:error, e} ->
        {:error, e}
    end
  end

  def handle_call(
        :endpoint,
        _,
        %{endpoint: endpoint, bind_addr: bind_addr} = state
      ) do
    {:reply, {:ok, endpoint, bind_addr}, state}
  end

  def handle_info(
        {:new_connection_to_accept, connection, from, responder},
        %{
          connection_mod: connection_mod,
          endpoint: endpoint,
          supervisor: supervisor,
          connection_args: connection_args
        } = state
      ) do
    {:ok, pid} =
      DynamicSupervisor.start_child(
        supervisor,
        {connection_mod,
         Map.merge(connection_args, %{endpoint: endpoint, connection: connection, from: from})}
      )

    ExP2P.set_controlling_pid(responder, pid)
    {:noreply, state}
  end
end
