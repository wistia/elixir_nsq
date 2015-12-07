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
  alias NSQ.ConnInfo, as: ConnInfo

  # ------------------------------------------------------- #
  # Type Definitions                                        #
  # ------------------------------------------------------- #
  @typedoc """
  A tuple with a host and a port.
  """
  @type host_with_port :: {String.t, integer}

  @typedoc """
  A tuple with a string ID (used to target the connection in
  NSQ.ConnectionSupervisor) and a PID of the connection.
  """
  @type connection :: {String.t, pid}

  @typedoc """
  A map, but we can be more specific by asserting some entries that should be
  set for a connection's state map.
  """
  @type conn_state :: %{parent: pid, socket: pid, config: NSQ.Config.t, nsqd: host_with_port}

  # ------------------------------------------------------- #
  # Module Attributes                                       #
  # ------------------------------------------------------- #
  @project ElixirNsq.Mixfile.project
  @user_agent "#{@project[:app]}/#{@project[:version]}"
  @socket_opts [:binary, active: false, deliver: :term, packet: :raw]
  @initial_state %{
    parent: nil,
    socket: nil,
    cmd_resp_queue: :queue.new,
    cmd_queue: :queue.new,
    config: %{},
    reader_pid: nil,
    msg_sup_pid: nil,
    messages_in_flight: 0,
    nsqd: nil,
    topic: nil,
    channel: nil,
    backoff_counter: 0,
    backoff_duration: 0,
    max_rdy: 2500,
    reconnect_attempts: 0,
    stop_flag: false,
    conn_info_pid: nil,
    msg_timeout: nil
  }

  # ------------------------------------------------------- #
  # Behaviour Implementation                                #
  # ------------------------------------------------------- #
  @spec init(map) :: {:connect, nil, conn_state}
  def init(conn_state) do
    {:ok, msg_sup_pid} = Task.Supervisor.start_link(strategy: :one_for_one)
    init_conn_info(conn_state)
    {:connect, nil, %{conn_state | msg_sup_pid: msg_sup_pid}}
  end

  @doc """
  This is code that runs _to connect_. It's called whenever a handler returns
  `:connect` as its first argument. Refer to the Connection docs for more info.
  """
  @spec connect(any, %{nsqd: host_with_port}) ::
    {:ok, conn_state} |
    {:stop, term, map}
  def connect(_info, %{nsqd: {host, port}} = state) do
    if should_attempt_connection?(state) do
      socket_opts = @socket_opts ++ [send_timeout: state.config.write_timeout]
      case :gen_tcp.connect(to_char_list(host), port, socket_opts, state.config.dial_timeout) do
        {:ok, socket} ->
          state = %{state | socket: socket}
          {:ok, state} = do_handshake(state)
          {:ok, state} = start_receiving_messages(socket, state)
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
  @spec disconnect(any, conn_state) ::
    {:stop, :normal, conn_state} | {:connect, :reconnect, conn_state}
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

  @spec handle_call({:cmd, tuple, atom}, {pid, reference}, conn_state) ::
    {:reply, {:ok, reference}, conn_state} |
    {:reply, {:queued, :nosocket}, conn_state}
  def handle_call({:cmd, cmd, kind}, {_, ref} = from, state) do
    if state.socket do
      state = send_data_and_queue_resp(state, cmd, from, kind)
      state = update_state_from_cmd(cmd, state)
      {:reply, {:ok, ref}, state}
    else
      # Not connected currently; add this call onto a queue to be run as soon
      # as we reconnect.
      state = %{state | cmd_queue: :queue.in({cmd, from, kind}, state.cmd_queue)}
      {:reply, {:queued, :no_socket}, state}
    end
  end

  @spec handle_call(:stop, {pid, reference}, conn_state) ::
    {:stop, :normal, conn_state}
  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end

  @spec handle_call(:state, {pid, reference}, conn_state) ::
    {:reply, conn_state, conn_state}
  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  @spec handle_cast(:flush_cmd_queue, conn_state) :: {:noreply, conn_state}
  def handle_cast(:flush_cmd_queue, state) do
    {:noreply, flush_cmd_queue(state)}
  end

  @spec handle_cast({:nsq_msg, binary}, conn_state) :: {:noreply, conn_state}
  def handle_cast({:nsq_msg, msg}, %{socket: socket, cmd_resp_queue: cmd_resp_queue} = state) do
    case msg do
      {:response, "_heartbeat_"} ->
        :gen_tcp.send(socket, encode(:noop))

      {:response, data} ->
        {item, cmd_resp_queue} = :queue.out(cmd_resp_queue)
        case item do
          {:value, {_cmd, {pid, ref}, :reply}} ->
            send(pid, {ref, data})
          :empty -> :ok
        end
        state = %{state | cmd_resp_queue: cmd_resp_queue}

      {:error, data} ->
        Logger.error "error: #{inspect data}"

      {:error, reason, data} ->
        Logger.error "error: #{reason}\n#{inspect data}"

      {:message, data} ->
        message = NSQ.Message.from_data(data)
        state = received_message(state)
        message = %NSQ.Message{message |
          connection: self,
          consumer: state.parent,
          socket: socket,
          config: state.config,
          msg_timeout: state.msg_timeout
        }
        GenServer.cast(state.parent, {:maybe_update_rdy, state.nsqd})
        Task.Supervisor.start_child(
          state.msg_sup_pid, NSQ.Message, :process, [message]
        )
    end

    {:noreply, state}
  end

  # When a task is done, it automatically messages the return value to the
  # calling process. we can use that opportunity to update the messages in
  # flight.
  @spec handle_info({reference, {:message_done, NSQ.Message.t}}, conn_state) ::
    {:noreply, conn_state}
  def handle_info({_ref, {:message_done, _msg}}, state) do
    state = state |> decrement_messages_in_flight
    {:noreply, state}
  end

  # ------------------------------------------------------- #
  # API Definitions                                         #
  # ------------------------------------------------------- #
  @spec start_link(pid, host_with_port, NSQ.Config.t, String.t, String.t, pid, list) ::
    {:ok, pid}
  def start_link(parent, nsqd, config, topic, channel, conn_info_pid, opts \\ []) do
    state = %{@initial_state |
      parent: parent,
      nsqd: nsqd,
      config: config,
      topic: topic,
      channel: channel,
      conn_info_pid: conn_info_pid
    }
    {:ok, _pid} = Connection.start_link(__MODULE__, state, opts)
  end

  @spec get_state(connection) :: {:ok, conn_state}
  def get_state({_conn_id, pid} = _connection) do
    GenServer.call(pid, :state)
  end

  @spec close(pid, conn_state) :: any
  def close(conn, conn_state \\ nil) do
    conn_state = conn_state || get_state(conn)

    # send a CLS command and expect CLOSE_WAIT in response
    {:ok, "CLOSE_WAIT"} = cmd(conn, :cls)

    # grace period: poll once per second until zero are in flight
    result = wait_for_zero_in_flight_with_timeout(
      conn_state.conn_info_pid,
      ConnInfo.conn_id(conn_state),
      conn_state.msg_timeout
    )

    # either way, we're exiting
    case result do
      :ok ->
        Logger.warn "No more messages in flight. Exiting."
      :timeout ->
        Logger.error "Timed out waiting for messages to finish. Exiting anyway."
    end

    Process.exit(self, :normal)
  end

  @spec nsqds_from_lookupds([host_with_port], String.t) :: [host_with_port]
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

  @spec query_lookupd(host_with_port, String.t) :: map
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
        %HTTPotion.Response{status_code: 404} ->
          %{}
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

  @doc """
  This is the recv loop that we kick off in a separate process immediately
  after the handshake. We send each incoming NSQ message as an erlang message
  back to the connection for handling.
  """
  def recv_nsq_messages(sock, conn, timeout) do
    case :gen_tcp.recv(sock, 4, timeout) do
      {:error, :timeout} ->
        # If publishing is quiet, we won't receive any messages in the timeout.
        # This is fine. Let's just try again!
        recv_nsq_messages(sock, conn, timeout)
      {:ok, <<msg_size :: size(32)>>} ->
        # Got a message! Decode it and let the connection know. We just
        # received data on the socket to get the size of this message, so if we
        # timeout in here, that's probably indicative of a problem.
        {:ok, raw_msg_data} = :gen_tcp.recv(sock, msg_size, timeout)
        decoded = decode(raw_msg_data)
        GenServer.cast(conn, {:nsq_msg, decoded})
        recv_nsq_messages(sock, conn, timeout)
    end
  end

  @doc """
  Immediately after connecting to the NSQ socket, both consumers and producers
  follow this protocol.
  """
  @spec do_handshake(conn_state) :: {:ok, conn_state}
  def do_handshake(conn_state) do
    conn_state = Task.async(fn ->
      %{socket: socket, channel: channel} = conn_state

      socket |> send_magic_v2
      {:ok, conn_state} = socket |> identify(conn_state)

      # Producers don't have a channel, so they won't do this.
      if channel do
        socket |> subscribe(conn_state)
      end

      conn_state
    end) |> Task.await(conn_state.config.dial_timeout)

    {:ok, conn_state}
  end

  @doc """
  Calls the command and waits for a response. If a command shouldn't have a
  response, use cmd_noreply.
  """
  @spec cmd(pid, tuple, integer) :: {:ok, binary} | {:error, String.t}
  def cmd(conn_pid, cmd, timeout \\ 5000) do
    {:ok, ref} = GenServer.call(conn_pid, {:cmd, cmd, :reply})
    receive do
      {^ref, data} ->
        {:ok, data}
    after
      timeout ->
        {:error, "Command #{cmd} took longer than timeout #{timeout}"}
    end
  end

  @doc """
  Calls the command but doesn't generate a reply back to the caller.
  """
  @spec cmd_noreply(pid, tuple) :: {:ok, reference} | {:queued, :nosocket}
  def cmd_noreply(conn_pid, cmd) do
    GenServer.call(conn_pid, {:cmd, cmd, :noreply})
  end

  @doc """
  Calls the command but doesn't expect any response.
  """
  @spec cmd_noreply(pid, tuple) :: {:ok, reference} | {:queued, :nosocket}
  def cmd_noresponse(conn, cmd) do
    GenServer.call(conn, {:cmd, cmd, :noresponse})
  end

  # ------------------------------------------------------- #
  # Private Functions                                       #
  # ------------------------------------------------------- #
  @spec send_magic_v2(pid) :: :ok
  defp send_magic_v2(socket) do
    Logger.debug("(#{inspect self}) sending magic v2...")
    :ok = :gen_tcp.send(socket, encode(:magic_v2))
  end

  @spec identify(pid, conn_state) :: {:ok, binary}
  defp identify(socket, conn_state) do
    Logger.debug("(#{inspect self}) identifying...")
    identify_obj = encode({:identify, identify_props(conn_state)})
    :ok = :gen_tcp.send(socket, identify_obj)
    {:response, json} = recv_nsq_response(socket, conn_state)
    {:ok, _conn_state} = update_from_identify_response(conn_state, json)
  end

  @spec update_from_identify_response(map, binary) :: map
  defp update_from_identify_response(conn_state, json) do
    {:ok, parsed} = Poison.decode(json)

    # respect negotiated max_rdy_count
    if parsed["max_rdy_count"] do
      ConnInfo.update conn_state, %{max_rdy: parsed["max_rdy_count"]}
    end

    # respect negotiated msg_timeout
    if parsed["msg_timeout"] do
      conn_state = %{conn_state | msg_timeout: parsed["msg_timeout"]}
    else
      conn_state = %{conn_state | msg_timeout: conn_state.config.msg_timeout}
    end

    {:ok, conn_state}
  end

  @spec recv_nsq_response(pid, map) :: {:response, binary}
  defp recv_nsq_response(socket, conn_state) do
    {:ok, <<msg_size :: size(32)>>} = :gen_tcp.recv(
      socket, 4, conn_state.config.read_timeout
    )
    {:ok, raw_msg_data} = :gen_tcp.recv(
      socket, msg_size, conn_state.config.read_timeout
    )
    {:response, _response} = decode(raw_msg_data)
  end

  @spec subscribe(pid, conn_state) :: {:ok, binary}
  defp subscribe(socket, %{topic: topic, channel: channel} = conn_state) do
    Logger.debug "(#{inspect self}) subscribe to #{topic} #{channel}"
    :gen_tcp.send(socket, encode({:sub, topic, channel}))

    Logger.debug "(#{inspect self}) wait for subscription acknowledgment"
    expected = ok_msg
    {:ok, ^expected} = :gen_tcp.recv(socket, 0, conn_state.config.read_timeout)
  end

  @spec identify_props(conn_state) :: conn_state
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

  @spec reconnect_delay(conn_state) :: integer
  defp reconnect_delay(conn_state) do
    interval = conn_state.config.lookupd_poll_interval
    jitter = round(interval * conn_state.config.lookupd_poll_jitter * :random.uniform)
    interval + jitter
  end

  @spec now :: integer
  defp now do
    :calendar.datetime_to_gregorian_seconds(:calendar.universal_time)
  end

  @spec reset_reconnects(conn_state) :: conn_state
  defp reset_reconnects(state), do: %{state | reconnect_attempts: 0}

  @spec increment_reconnects(conn_state) :: conn_state
  defp increment_reconnects(state) do
    %{state | reconnect_attempts: state.reconnect_attempts + 1, socket: nil}
  end

  @spec received_message(conn_state) :: conn_state
  defp received_message(state) do
    ConnInfo.update state, fn(info) ->
      %{info |
        rdy_count: info.rdy_count - 1,
        messages_in_flight: info.messages_in_flight + 1,
        last_msg_timestamp: now
      }
    end
    state
  end

  @spec decrement_messages_in_flight(conn_state) :: conn_state
  defp decrement_messages_in_flight(state) do
    ConnInfo.update state, fn(info) ->
      %{info | messages_in_flight: info.messages_in_flight - 1}
    end
    state
  end

  @spec update_rdy_count(conn_state, integer) :: conn_state
  defp update_rdy_count(state, rdy_count) do
    ConnInfo.update(state, %{rdy_count: rdy_count, last_rdy: rdy_count})
    state
  end

  @spec should_attempt_connection?(conn_state) :: boolean
  defp should_attempt_connection?(state) do
    state.reconnect_attempts == 0 ||
      state.reconnect_attempts < state.config.max_reconnect_attempts
  end

  @spec log_connect_failed_and_stop({term, String.t}, conn_state) ::
    {:stop, term, conn_state}
  defp log_connect_failed_and_stop({reason, note}, state) do
    Logger.debug("(#{inspect self}) connect failed; #{reason}; #{note}")
    {:stop, reason, state}
  end

  @spec log_connect_failed_and_reconnect(term, conn_state) ::
    {:backoff, integer, conn_state}
  defp log_connect_failed_and_reconnect(reason, state) do
    delay = reconnect_delay(state)
    Logger.debug("(#{inspect self}) connect failed; #{reason}; try again in #{delay / 1000}s")
    {:backoff, delay, increment_reconnects(state)}
  end

  @spec send_data_and_queue_resp(conn_state, tuple, {reference, pid}, atom) ::
    conn_state
  defp send_data_and_queue_resp(state, cmd, from, kind) do
    :gen_tcp.send(state.socket, encode(cmd))
    if kind == :noresponse do
      state
    else
      %{state |
        cmd_resp_queue: :queue.in({cmd, from, kind}, state.cmd_resp_queue)
      }
    end
  end

  @spec flush_cmd_queue(conn_state) :: conn_state
  defp flush_cmd_queue(state) do
    {item, new_queue} = :queue.out(state.cmd_queue)
    case item do
      {:value, {cmd, from, kind}} ->
        state = send_data_and_queue_resp(state, cmd, from, kind)
        flush_cmd_queue(%{state | cmd_queue: new_queue})
      :empty ->
        %{state | cmd_queue: new_queue}
    end
  end

  @spec start_receiving_messages(pid, conn_state) :: {:ok, conn_state}
  defp start_receiving_messages(socket, state) do
    reader_pid = spawn_link(
      __MODULE__,
      :recv_nsq_messages,
      [socket, self, state.config.read_timeout]
    )
    state = %{state | reader_pid: reader_pid}
    GenServer.cast(self, :flush_cmd_queue)
    {:ok, state}
  end

  @spec update_state_from_cmd(tuple, conn_state) :: conn_state
  defp update_state_from_cmd(cmd, state) do
    case cmd do
      {:rdy, count} -> update_rdy_count(state, count)
      _any -> state
    end
  end

  @spec init_conn_info(conn_state) :: any
  defp init_conn_info(state) do
    ConnInfo.update state, %{
      max_rdy: state.max_rdy,
      rdy_count: 0,
      last_rdy: 0,
      messages_in_flight: 0,
      last_msg_timestamp: now,
      retry_rdy_pid: nil
    }
  end

  @spec wait_for_zero_in_flight(pid, binary) :: any
  defp wait_for_zero_in_flight(agent_pid, conn_id) do
    in_flight = ConnInfo.fetch(agent_pid, conn_id, [:messages_in_flight])
    if in_flight <= 0 do
      :ok
    else
      :timer.sleep(1000)
      wait_for_zero_in_flight(agent_pid, conn_id)
    end
  end

  @spec wait_for_zero_in_flight_with_timeout(pid, binary, integer) :: any
  defp wait_for_zero_in_flight_with_timeout(agent_pid, conn_id, timeout) do
    try do
      Task.async(fn -> wait_for_zero_in_flight(agent_pid, conn_id) end)
        |> Task.await(timeout)
    catch
      :timeout, _ -> :timeout
    end
  end
end
