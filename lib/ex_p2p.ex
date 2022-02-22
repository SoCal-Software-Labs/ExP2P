defmodule ExP2P do
  use Rustler,
    otp_app: :ex_p2p,
    crate: :exp2p

  def start(_waiting, _bind, _bootstrap), do: :erlang.nif_error(:nif_not_loaded)
  def set_controlling_pid(_resp_channel, _pid), do: :erlang.nif_error(:nif_not_loaded)

  def send_stream_response(_endpoint, _stream, _resp, _waiting, _timeout),
    do: :erlang.nif_error(:nif_not_loaded)

  def send_bidirectional(_endpoint, _connection, _resp, _waiting, _timeout),
    do: :erlang.nif_error(:nif_not_loaded)

  def send_unidirectional(_endpoint, _connection, _resp, _waiting, _timeout),
    do: :erlang.nif_error(:nif_not_loaded)

  def send_unidirectional_many(_endpoint, _addrs, _resp, _waiting, _timeout),
    do: :erlang.nif_error(:nif_not_loaded)

  def connect_to_peer(_endpoint, _peers, _waiting, _timeout),
    do: :erlang.nif_error(:nif_not_loaded)

  @moduledoc """
  Documentation for `ExP2p`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> ExP2p.hello()
      :world

  """
  def connect(endpoint, peers, timeout \\ 10000) do
    :ok = connect_to_peer(endpoint, peers, self(), timeout - 100)

    receive do
      {:new_connection, conn} -> {:ok, conn}
    after
      timeout ->
        {:error, :timeout}
    end
  end

  def unidirectional(endpoint, connection, msg, timeout \\ 10000) do
    :ok = send_unidirectional(endpoint, connection, msg, self(), timeout - 100)

    receive do
      :ok -> :ok
      {:error, err} -> {:error, err}
    after
      timeout ->
        {:error, :timeout}
    end
  end

  def bidirectional(endpoint, connection, msg, timeout \\ 10000) do
    :ok = send_bidirectional(endpoint, connection, msg, self(), timeout - 100)

    receive do
      {:message_reply, reply} -> {:ok, reply}
      {:error, err} -> {:error, err}
    after
      timeout ->
        {:error, :timeout}
    end
  end

  def stream_response(endpoint, stream, msg, timeout \\ 10000) do
    :ok = send_stream_response(endpoint, stream, msg, self(), timeout - 100)

    receive do
      :ok -> :ok
      {:error, err} -> {:error, err}
    after
      timeout ->
        {:error, :timeout}
    end
  end
end
