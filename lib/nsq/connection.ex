defmodule NSQ.Connection do
  import NSQ.Protocol
  use Connection
  require Logger


  @initial_state %{
    consumer: nil,
    socket: nil,
    config: %{},
    messages_in_flight: 0,
    nsqd: nil,
    topic: nil,
    channel: nil,
    backoff_counter: 0,
    backoff_duration: 0,
    rdy_count: 0,
    last_rdy: 0,
    max_rdy: 2500,
    last_msg_timestamp: :calendar.datetime_to_gregorian_seconds(:calendar.universal_time),
    reconnect_attempts: 0
  }


  def start_monitor(consumer, nsqd, config, topic, channel \\ nil) do
    state = %{@initial_state | consumer: consumer, nsqd: nsqd, config: config, topic: topic, channel: channel}
    {:ok, pid} = Connection.start(__MODULE__, state)
    ref = Process.monitor(pid)
    {:ok, {pid, ref}}
  end


  def init(state) do
    {:connect, nil, state}
  end


  def connect(_info, %{nsqd: {host, port}} = state) do
    if state.reconnect_attempts == 0 || state.reconnect_attempts < state.config.max_reconnect_attempts do
      opts = [:binary, active: false, deliver: :term, packet: :raw]
      case :gen_tcp.connect(to_char_list(host), port, opts) do
        {:ok, socket} ->
          {:ok, state} = do_handshake(socket, state.topic, state.channel, state)
          {:ok, %{state | socket: socket, reconnect_attempts: 0}}
        {:error, reason} ->
          if using_nsqlookupd?(state) do
            Logger.debug("(#{inspect self}) connect failed, give up; discovery loop should respawn")
            {:stop, reason, state}
          else
            if state.config.max_reconnect_attempts > 0 do
              delay = reconnect_delay(state)
              Logger.debug("(#{inspect self}) connect failed, try again in #{delay / 1000}s")
              {:backoff, delay, %{state | reconnect_attempts: state.reconnect_attempts + 1}}
            else
              Logger.debug("(#{inspect self}) connect failed, reconnect turned off")
              {:stop, reason, state}
            end
          end
      end
    else
      Logger.debug("(#{inspect self}) too many reconnect attempts, giving up")
      {:stop, :too_many_reconnects, state}
    end
  end


  def disconnect(info, %{socket: socket} = state) do
    :ok = :gen_tcp.close(socket)
    case info do
      {:close, from} ->
        Connection.reply(from, :ok)
      {:error, :closed} ->
        :error_Logger.format("connection closed~n", [])
      {:error, reason} ->
        reason = :inet.format_error(reason)
        :error_Logger.format("connection error: ~s~n", [reason])
    end
    {:connect, :reconnect, %{state | reconnect_attempts: state.reconnect_attempts + 1, socket: nil}}
  end


  defp reconnect_delay(state) do
    interval = state.config.lookupd_poll_interval
    jitter = round(interval * state.config.lookupd_poll_jitter * :random.uniform)
    interval + jitter
  end


  defp using_nsqlookupd?(state) do
    length(state.config.nsqlookupds) > 0
  end


  def handle_call({:message_done, _message}, _from, state) do
    {:reply, :ack, %{state | messages_in_flight: state.messages_in_flight - 1}}
  end


  def handle_call({:rdy, count}, _from, state) do
    if state.socket do
      :ok = :gen_tcp.send(state.socket, encode({:rdy, count}))
      {:reply, :ok, %{state | rdy_count: count, last_rdy: count}}
    else
      {:reply, {:error, :no_socket}, state}
    end
  end


  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end


  def handle_info({:tcp, socket, raw_data}, state) do
    raw_messages = messages_from_data(raw_data)
    Enum.each raw_messages, fn(raw_message) ->
      case decode(raw_message) do
        {:response, "_heartbeat_"} ->
          :gen_tcp.send(socket, "nop\n")

        {:response, data} ->
          IO.inspect {"handle response", data}

        {:error, data} ->
          IO.inspect {"handle error", data}

        {:message, data} ->
          message = NSQ.Message.from_data(data)
          state = %{state |
            rdy_count: state.rdy_count - 1,
            messages_in_flight: state.messages_in_flight + 1,
            last_msg_timestamp: now
          }
          NSQ.Message.process(
            message,
            state.config.message_handler,
            state.config.max_attempts,
            socket
          )

        anything ->
          IO.inspect {"unhandled", anything}
      end
    end

    {:noreply, state}
  end


  def handle_info({:tcp_closed, socket}, state) do
    {:connect, :tcp_closed, %{state | reconnect_attempts: state.reconnect_attempts + 1}}
  end


  defp do_handshake(socket, topic, channel, state) do
    Logger.debug("(#{inspect self}) connecting...")
    :ok = :gen_tcp.send(socket, encode(:magic_v2))

    Logger.debug "(#{inspect self}) subscribe to #{topic} #{channel}"
    :gen_tcp.send(socket, encode({:sub, topic, channel}))

    Logger.debug "(#{inspect self}) wait for subscription acknowledgment"
    {:ok, ack} = :gen_tcp.recv(socket, 0)
    unless ok_msg?(ack), do: raise "expected OK, got #{inspect ack}"

    # set mode to active: true so we receive tcp messages as erlang messages.
    :inet.setopts(socket, active: true)

    Logger.debug("(#{inspect self}) connected, set rdy 1")
    :gen_tcp.send(socket, encode({:rdy, 1}))

    {:ok, %{state | rdy_count: 1, last_rdy: 1}}
  end


  def handle_call({:state, prop}, _from, state) do
    {:reply, state[prop], state}
  end


  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  def get_state({pid, _ref}, prop) do
    GenServer.call(pid, {:state, prop})
  end


  def get_state({pid, _ref}) do
    GenServer.call(pid, :state)
  end


  def close(conn) do
    expected_response = "CLOSE_WAIT"
    resp_length = byte_size(expected_response)
    :gen_tcp.send(conn.socket, encode(:cls))
    {:ok, expected_response} = :gen_tcp.recv(conn.socket, resp_length)
  end


  defp now do
    :calendar.datetime_to_gregorian_seconds(:calendar.universal_time)
  end
end
