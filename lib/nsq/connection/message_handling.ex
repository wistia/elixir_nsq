defmodule NSQ.Connection.MessageHandling do
  alias NSQ.Connection, as: C
  alias NSQ.ConnInfo
  alias NSQ.Connection.Buffer
  alias NSQ.Connection.Command
  import NSQ.Protocol
  require Logger

  @doc """
  This is the recv loop that we kick off in a separate process immediately
  after the handshake. We send each incoming NSQ message as an erlang message
  back to the connection for handling.
  """
  def recv_nsq_messages(conn_state, conn) do
    case conn_state |> Buffer.recv(4) do
      {:error, :timeout} ->
        # If publishing is quiet, we won't receive any messages in the timeout.
        # This is fine. Let's just try again!
        conn_state |> recv_nsq_messages(conn)

      {:ok, <<msg_size::size(32)>>} ->
        # Got a message! Decode it and let the connection know. We just
        # received data on the socket to get the size of this message, so if we
        # timeout in here, that's probably indicative of a problem.
        raw_msg_data = conn_state |> Buffer.recv!(msg_size)
        decoded = decode(raw_msg_data)
        :ok = GenServer.call(conn, {:nsq_msg, decoded})
        conn_state |> recv_nsq_messages(conn)
    end
  end

  def handle_nsq_message({:response, "_heartbeat_"}, state) do
    respond_to_heartbeat(state)
    {:ok, state}
  end

  def handle_nsq_message({:response, data}, state) do
    state |> Command.send_response_to_caller(data)
  end

  def handle_nsq_message({:error, data}, state) do
    state |> log_error(nil, data)
    {:ok, state}
  end

  def handle_nsq_message({:error, reason, data}, state) do
    state |> log_error(reason, data)
    {:ok, state}
  end

  def handle_nsq_message({:message, data}, state) do
    state |> kick_off_message_processing(data)
  end

  @spec update_conn_stats_on_message_done(C.state(), any) :: any
  def update_conn_stats_on_message_done(state, ret_val) do
    ConnInfo.update(state, fn info ->
      info |> update_stats_from_ret_val(ret_val)
    end)
  end

  @spec update_stats_from_ret_val(map, any) :: map
  defp update_stats_from_ret_val(info, ret_val) do
    info = %{info | messages_in_flight: info.messages_in_flight - 1}

    case ret_val do
      :ok ->
        %{info | finished_count: info.finished_count + 1}

      :fail ->
        %{info | failed_count: info.failed_count + 1}

      :req ->
        %{info | requeued_count: info.requeued_count + 1}

      {:req, _} ->
        %{info | requeued_count: info.requeued_count + 1}

      {:req, _, true} ->
        %{info | requeued_count: info.requeued_count + 1, backoff_count: info.backoff_count + 1}

      {:req, _, _} ->
        %{info | requeued_count: info.requeued_count + 1}
    end
  end

  @spec respond_to_heartbeat(C.state()) :: :ok
  defp respond_to_heartbeat(state) do
    GenEvent.notify(state.event_manager_pid, :heartbeat)
    state |> Buffer.send!(encode(:noop))
  end

  @spec log_error(C.state(), binary, binary) :: any
  defp log_error(state, reason, data) do
    GenEvent.notify(state.event_manager_pid, {:error, reason, data})

    if reason do
      Logger.error("error: #{reason}\n#{inspect(data)}")
    else
      Logger.error("error: #{inspect(data)}")
    end
  end

  @spec kick_off_message_processing(C.state(), binary) :: {:ok, C.state()}
  defp kick_off_message_processing(state, data) do
    message = NSQ.Message.from_data(data)
    state = received_message(state)

    message = %NSQ.Message{
      message
      | connection: self(),
        consumer: state.parent,
        reader: state.reader,
        writer: state.writer,
        config: state.config,
        msg_timeout: state.msg_timeout,
        event_manager_pid: state.event_manager_pid
    }

    GenEvent.notify(state.event_manager_pid, {:message, message})
    GenServer.cast(state.parent, {:maybe_update_rdy, state.nsqd})
    NSQ.Message.Supervisor.start_child(state.msg_sup_pid, message)
    {:ok, state}
  end

  @spec received_message(C.state()) :: C.state()
  defp received_message(state) do
    ConnInfo.update(state, fn info ->
      %{
        info
        | rdy_count: info.rdy_count - 1,
          messages_in_flight: info.messages_in_flight + 1,
          last_msg_timestamp: now()
      }
    end)

    state
  end

  @spec now :: integer
  defp now do
    {megasec, sec, microsec} = :os.timestamp()
    1_000_000 * megasec + sec + microsec / 1_000_000
  end
end
