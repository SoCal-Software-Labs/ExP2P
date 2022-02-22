defmodule ExP2P.Connection do
  use GenServer

  require Logger

  def start_link(opts) do
    IO.inspect(opts)

    GenServer.start_link(__MODULE__, opts, [])
  end

  def init([handle, endpoint, connection]) do
    {:ok,
     %{
       handle: handle,
       endpoint: endpoint,
       connection: connection
     }}
  end

  def handle_info(
        {:new_message, msg, resource, from},
        %{endpoint: endpoint, handle: handle} = state
      ) do
    :ok = handle.(endpoint, msg, resource, from)
    {:noreply, state}
  end
end
