defmodule NSQ.Connection do
  @moduledoc """
  Sets up a TCP connection to NSQD. Both consumers and producers use this.

  This implements the Connection behaviour, which lets us reconnect or backoff
  under certain conditions. For more info, check out the module:
  https://github.com/fishcakez/connection. The module docs are especially
  helpful:
  https://github.com/fishcakez/connection/blob/master/lib/connection.ex.
  """

  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  require Logger
  use Connection
  import NSQ.Protocol

  # ------------------------------------------------------- #
  # Module Attributes                                       #
  # ------------------------------------------------------- #
  @project ElixirNsq.Mixfile.project
  @user_agent "#{@project[:app]}/#{@project[:version]}"
  @socket_opts [:binary, active: false, deliver: :term, packet: :raw]
  @initial_state %{
    parent: nil,
    socket: nil,
    config: %{},
    msg_sup_pid: nil,
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
    reconnect_attempts: 0,
    stop_flag: false
  }

  # ------------------------------------------------------- #
  # Behaviour Implementation                                #
  # ------------------------------------------------------- #
  def init(conn_state) do
    {:ok, msg_sup_pid} = Task.Supervisor.start_link(strategy: :one_for_one)
    {:connect, nil, %{conn_state | msg_sup_pid: msg_sup_pid}}
  end

  @doc """
  This is code that runs _to connect_. Refer to the Connection docs for more
  info.
  """
  def connect(_info, %{nsqd: {host, port}} = state) do
    if should_attempt_connection?(state) do
      case :gen_tcp.connect(to_char_list(host), port, @socket_opts) do
        {:ok, socket} ->
          state = %{state | socket: socket}
          {:ok, state} = do_handshake(self, state)
          {:ok, reset_reconnects(state)}
        {:error, reason} ->
          if length(state.config.nsqlookupds) > 0 do
            log_connect_failed_and_stop({reason, "discovery loop should respawn"}, state)
          else
            if state.config.max_reconnect_attempts > 0 do
              log_connect_failed_and_reconnect(reason, state)
            else
              log_connect_failed_and_stop({reason, "reconnect turned off"}, state)
            end
          end
      end
    else
      Logger.debug("(#{inspect self}) too many reconnect attempts, giving up")
      {:stop, :too_many_reconnects, state}
    end
  end

  @doc """
  This is code that runs _on disconnect_, not code _to disconnect_. Refer to
  the Connection docs for more info.
  """
  def disconnect(info, %{socket: socket} = state) do
    :ok = :gen_tcp.close(socket)
    case info do
      {:close, from} ->
        Connection.reply(from, :ok)
        {:stop, :normal, state}
      {:error, :closed} ->
        Logger.error("connection closed")
        {:connect, :reconnect, increment_reconnects(state)}
      {:error, reason} ->
        Logger.error("connection error: #{inspect reason}")
        {:connect, :reconnect, increment_reconnects(state)}
    end
  end

  @doc """
  Publish data to a topic and wait for acknowledgment. This lets us use
  backpressure.
  """
  def handle_call({:pub, topic, data}, _from, state) do
    pub(state.socket, topic, data)
    {:reply, :ok, state}
  end

  @doc """
  Publish data to a topic without acknowledgment. Maybe it didn't get there?
  But it's fast!
  """
  def handle_call({:pub_async, topic, data}, _from, state) do
    pub_async(state.socket, topic, data)
    {:reply, :ok, state}
  end

  def handle_call({:rdy, count}, _from, state) do
    if state.socket do
      :ok = :gen_tcp.send(state.socket, encode({:rdy, count}))
      {:reply, :ok, update_rdy_count(state, count)}
    else
      {:reply, {:error, :no_socket}, state}
    end
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  def handle_info({:tcp, socket, raw_data}, state) do
    raw_messages = messages_from_data(socket, raw_data)
    Enum.each raw_messages, fn(raw_message) ->
      case decode(raw_message) do
        {:response, "_heartbeat_"} ->
          :gen_tcp.send(socket, encode(:noop))

        {:response, data} ->
          Logger.debug "response #{inspect data}"

        {:error, data} ->
          Logger.error "error #{inspect data}"

        {:message, data} ->
          message = NSQ.Message.from_data(data)
          state = received_message(state)
          message = %NSQ.Message{message |
            connection: self,
            socket: socket,
            config: state.config
          }
          Task.Supervisor.start_child(
            state.msg_sup_pid, NSQ.Message, :process, [message]
          )
      end
    end

    {:noreply, state}
  end

  def messages_from_data(socket, data) do
    messages_from_data(socket, data, [])
  end

  @doc """
  If MPUB is used, a single TCP response can have data for several messages.
  In that case, the given data follows the same format, but it means we need
  to be sensitive to the given message sizes.

      iex> raw_data = <<0, 0, 0, 42, 0, 0, 0, 2, 20, 24, 221, 165, 125, 107, 65, 242, 0, 1>> <> "093cac1231a00224HTTP message" <> <<0, 0, 0, 42, 0, 0, 0, 2, 20, 24, 221, 165, 125, 107, 68, 234, 0, 1>> <> "093cac1231a00225HTTP message"
      ...> NSQ.Connection.messages_from_data(raw_data)
      [<<0, 0, 0, 2, 20, 24, 221, 165, 125, 107, 65, 242, 0, 1>> <> "093cac1231a00224HTTP message", <<0, 0, 0, 2, 20, 24, 221, 165, 125, 107, 68, 234, 0, 1>> <> "093cac1231a00225HTTP message"]
  """
  def messages_from_data(socket, data, acc) do
    # if we're processing many messages in a packet, it's possible
    # (though unlikely) that a recv stops in the middle of receiving the
    # message size itself. if our data is less than 4 bytes, we assume this is
    # the case and get the rest.
    bytes_left = 4 - byte_size(data)
    if bytes_left > 0 do
      data = recv_rest_of_msg(socket, data, min: bytes_left)
    end

    <<stated_msg_size :: size(32)>> <> rest = data

    # when receiving tcp messages from erlang, we're not guaranteed we have the
    # entire message when we start processing.
    bytes_left = stated_msg_size - byte_size(rest)
    if bytes_left > 0 do
      rest = recv_rest_of_msg(socket, rest, min: bytes_left)
    end

    # split our data blob into head/tail, i.e. "message"/"data blob". we're
    # basically transforming this block into a well-formed list of binary
    # messages.
    msg = binary_part(rest, 0, stated_msg_size)
    blob_after_msg = binary_part(rest, byte_size(msg), byte_size(rest) - byte_size(msg))

    if byte_size(blob_after_msg) > 0 do
      messages_from_data(socket, blob_after_msg, [msg | acc])
    else
      # We want the return messages to be in the order they were received,
      # but we've been pushing onto the front of our list.
      Enum.reverse([msg | acc])
    end
  end

  def recv_rest_of_msg(socket, rest, opts \\ []) do
    :inet.setopts(socket, active: false)

    # `min` is the minimum bytes we should try to recv before returning.
    if (min = opts[:min]) && min > 0 do
      {:ok, min_data} = :gen_tcp.recv(socket, min, 5000)
      rest = rest <> min_data
    end

    # if more data happens to be available, return that to.
    {:ok, extra_data} = :gen_tcp.recv(socket, 0)

    :inet.setopts(socket, active: true)
    rest <> extra_data
  end

  # When a task is done, it automatically messages the return value to the
  # calling process. we can use that opportunity to update the messages in
  # flight.
  def handle_info({_ref, {:message_done, _message}}, state) do
    state = state |> increment_rdy_count |> decrement_messages_in_flight
    {:noreply, state}
  end

  def handle_info({:tcp_closed, _socket}, state) do
    {:connect, :tcp_closed, increment_reconnects(state)}
  end

  # ------------------------------------------------------- #
  # API Definitions                                         #
  # ------------------------------------------------------- #
  def start_link(parent, nsqd, config, topic, channel, opts \\ []) do
    state = %{@initial_state |
      parent: parent,
      nsqd: nsqd,
      config: config,
      topic: topic,
      channel: channel
    }
    {:ok, _pid} = Connection.start_link(__MODULE__, state, opts)
  end

  def get_state({_child_id, pid} = _connection) do
    GenServer.call(pid, :state)
  end

  def close(conn, conn_state \\ nil) do
    conn_state = conn_state || NSQ.Connection.get_state(conn)
    wait_for_recv conn.socket, "CLOSE_WAIT", fn ->
      :gen_tcp.send(conn.socket, encode(:cls))
    end
    {:ok, conn_state}
  end

  def nsqds_from_lookupds(lookupds, topic) do
    responses = Enum.map(lookupds, &query_lookupd(&1, topic))
    nsqds = Enum.map responses, fn(response) ->
      Enum.map response["producers"] || [], fn(producer) ->
        if producer do
          {producer["broadcast_address"], producer["tcp_port"]}
        else
          nil
        end
      end
    end
    nsqds |>
      List.flatten |>
      Enum.uniq |>
      Enum.reject(fn(v) -> v == nil end)
  end

  def query_lookupd({host, port}, topic) do
    lookupd_url = "http://#{host}:#{port}/lookup?topic=#{topic}"
    headers = [{"Accept", "application/vnd.nsq; version=1.0"}]
    try do
      case HTTPotion.get(lookupd_url, headers: headers) do
        %HTTPotion.Response{status_code: 200, body: body, headers: headers} ->
          if body == nil || body == "" do
            body = "{}"
          end

          if headers[:"X-Nsq-Content-Type"] == "nsq; version=1.0" do
            Poison.decode!(body)
          else
            %{status_code: 200, status_txt: "OK", data: body}
          end
        %HTTPotion.Response{status_code: status, body: body} ->
          Logger.error "Unexpected status code from #{lookupd_url}: #{status}"
          %{status_code: status, status_txt: nil, data: body}
      end
    rescue
      e in HTTPotion.HTTPError ->
        Logger.error "Error connecting to #{lookupd_url}: #{inspect e}"
        %{status_code: nil, status_txt: nil, data: nil}
    end
  end


  def do_handshake(conn, conn_state \\ nil) do
    conn_state = conn_state || NSQ.Connection.get_state(conn)
    %{socket: socket, topic: topic, channel: channel} = conn_state

    Logger.debug("(#{inspect self}) connecting...")
    :ok = :gen_tcp.send(socket, encode(:magic_v2))

    Logger.debug("(#{inspect self}) identifying...")
    identify_obj = encode({:identify, identify_props(conn_state)})
    :ok = :gen_tcp.send(socket, identify_obj)
    {:ok, _resp} = :gen_tcp.recv(socket, 0)

    if channel do
      Logger.debug "(#{inspect self}) subscribe to #{topic} #{channel}"
      :gen_tcp.send(socket, encode({:sub, topic, channel}))

      Logger.debug "(#{inspect self}) wait for subscription acknowledgment"
      expected = ok_msg
      {:ok, ^expected} = :gen_tcp.recv(socket, 0)

      Logger.debug("(#{inspect self}) connected, set rdy 1")
      :gen_tcp.send(socket, encode({:rdy, 1}))

      conn_state = initial_rdy_count(conn_state)
    end

    # set mode to active: true so we receive tcp messages as erlang messages.
    :inet.setopts(socket, active: true)
    {:ok, conn_state}
  end

  def connection_id(parent, {host, port}) do
    "parent:#{inspect parent}:conn:#{host}:#{port}"
  end

  # ------------------------------------------------------- #
  # Private Functions                                       #
  # ------------------------------------------------------- #
  defp identify_props(%{nsqd: {host, port}, config: config} = conn_state) do
    %{
      client_id: "#{host}:#{port} (#{inspect conn_state.parent})",
      hostname: to_string(:net_adm.localhost),
      feature_negotiation: true,
      heartbeat_interval: config.heartbeat_interval,
      output_buffer: config.output_buffer_size,
      output_buffer_timeout: config.output_buffer_timeout,
      tls_v1: false,
      snappy: false,
      deflate: false,
      sample_rate: 0,
      user_agent: config.user_agent || @user_agent,
      msg_timeout: config.msg_timeout
    }
  end

  defp reconnect_delay(conn_state) do
    interval = conn_state.config.lookupd_poll_interval
    jitter = round(interval * conn_state.config.lookupd_poll_jitter * :random.uniform)
    interval + jitter
  end

  defp using_nsqlookupd?(state) do
    length(state.config.nsqlookupds) > 0
  end

  defp now do
    :calendar.datetime_to_gregorian_seconds(:calendar.universal_time)
  end

  defp reset_reconnects(state), do: %{state | reconnect_attempts: 0}

  defp increment_reconnects(state) do
    %{state | reconnect_attempts: state.reconnect_attempts + 1, socket: nil}
  end

  defp increment_rdy_count(state) do
    %{state | rdy_count: state.rdy_count + 1}
  end

  defp received_message(state) do
    %{state |
      rdy_count: state.rdy_count - 1,
      messages_in_flight: state.messages_in_flight + 1,
      last_msg_timestamp: now
    }
  end

  defp decrement_messages_in_flight(state) do
    %{state | messages_in_flight: state.messages_in_flight - 1}
  end

  defp initial_rdy_count(state) do
    %{state | rdy_count: 1, last_rdy: 1}
  end

  defp update_rdy_count(state, rdy_count) do
    %{state | rdy_count: rdy_count, last_rdy: rdy_count}
  end

  defp should_attempt_connection?(state) do
    state.reconnect_attempts == 0 ||
      state.reconnect_attempts < state.config.max_reconnect_attempts
  end

  defp log_connect_failed_and_stop({reason, note}, state) do
    Logger.debug("(#{inspect self}) connect failed; #{reason}; #{note}")
    {:stop, reason, state}
  end

  defp log_connect_failed_and_reconnect(reason, state) do
    delay = reconnect_delay(state)
    Logger.debug("(#{inspect self}) connect failed; #{reason}; try again in #{delay / 1000}s")
    {:backoff, delay, increment_reconnects(state)}
  end

  defp pub_async(socket, topic, data) do
    :gen_tcp.send(socket, encode({:pub, topic, data}))
  end

  # Setting active: false lets us handle the next TCP response via recv instead
  # of as an Erlang message.
  defp pub(socket, topic, data) do
    wait_for_recv socket, "OK", fn ->
      pub_async(socket, topic, data)
    end
  end

  # Use this whenever we need to have an immediate response to a socket send.
  def wait_for_recv(socket, body, fun) do
    # active: false means we will only receive tcp messages via :gen_tcp.recv
    # synchronously, not as erlang messages.
    :inet.setopts(socket, active: false)

    fun.()
    {:ok, resp} = :gen_tcp.recv(socket, 0)

    # If body is not specified, then we don't do any validation. It's assumed
    # that more complex validation will be done elsewhere.
    if body, do: ^resp = response_msg(body)

    # active: false means we will receive tcp messages as erlang messages
    # asynchronously, to be handle via handle_info.
    :inet.setopts(socket, active: true)

    {:ok, resp}
  end
end
