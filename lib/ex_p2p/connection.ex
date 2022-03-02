defmodule ExP2P.Connection do
  use GenServer, restart: :temporary

  require Logger

  def start_link(%{new_state: _, callback: _, endpoint: _, connection: _, from: _} = state) do
    GenServer.start_link(__MODULE__, state, [])
  end

  def init(state) do
    {:ok, Map.put(state, :user_state, state.new_state.(state.connection))}
  end

  def handle_info(
        {:new_message, msg, resource},
        %{
          endpoint: endpoint,
          user_state: user_state,
          from: from,
          connection: connection,
          callback: callback
        } = state
      ) do
    :ok = callback.(endpoint, connection, msg, resource, from, user_state)
    {:noreply, state}
  end

  def handle_info(
        {:error, error},
        state
      ) do
    Logger.error("Connection error #{error}")
    {:stop, :normal, state}
  end

  def handle_info(
        :connection_stopped,
        state
      ) do
    {:stop, :normal, state}
  end
end
