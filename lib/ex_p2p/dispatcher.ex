defmodule ExP2P.Dispatcher do
  use GenServer

  require Logger

  def start_link(bind_addr, bootstrap, connection_mod, connection_args, opts) do
    GenServer.start_link(
      __MODULE__,
      [bind_addr, bootstrap, connection_mod, connection_args],
      opts
    )
  end

  def endpoint(pid), do: GenServer.call(pid, :endpoint, :infinity)

  def init([bind_addr, bootstrap, connection_mod, connection_args]) do
    {:ok, pid} = DynamicSupervisor.start_link(strategy: :one_for_one)
    :ok = ExP2P.start(self(), bind_addr, bootstrap)

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
        %{endpoint: endpoint} = state
      ) do
    {:reply, {:ok, endpoint}, state}
  end

  def handle_info(
        {:new_connection_to_accept, connection, responder},
        %{
          connection_mod: connection_mod,
          endpoint: endpoint,
          supervisor: supervisor,
          connection_args: connection_args
        } = state
      ) do
    IO.inspect({:new_conn, connection})

    {:ok, pid} =
      DynamicSupervisor.start_child(
        supervisor,
        {connection_mod, connection_args ++ [endpoint, connection]}
      )

    ExP2P.set_controlling_pid(responder, pid)
    {:noreply, state}
  end
end
