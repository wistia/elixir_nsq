defmodule NSQ.Connection do
  @moduledoc """
  Sets up a TCP connection to NSQD. Both consumers and producers use this.
  """


  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  require Logger
  alias NSQ.Connection.Command
  alias NSQ.Connection.Initializer
  alias NSQ.Connection.MessageHandling
  alias NSQ.ConnInfo


  # ------------------------------------------------------- #
  # Type Definitions                                        #
  # ------------------------------------------------------- #
  @typedoc """
  A tuple with a host and a port.
  """
  @type host_with_port :: {String.t, integer}

  @typedoc """
  A tuple with a string ID (used to target the connection in
  NSQ.Connection.Supervisor) and a PID of the connection.
  """
  @type connection :: {String.t, pid}

  @typedoc """
  A map, but we can be more specific by asserting some entries that should be
  set for a connection's state map.
  """
  @type state :: %{parent: pid, socket: pid, config: NSQ.Config.t, nsqd: host_with_port}


  # ------------------------------------------------------- #
  # Module Attributes                                       #
  # ------------------------------------------------------- #
  @initial_state %{
    parent: nil,
    socket: nil,
    cmd_resp_queue: :queue.new,
    cmd_queue: :queue.new,
    config: %{},
    reader_pid: nil,
    msg_sup_pid: nil,
    event_manager_pid: nil,
    messages_in_flight: 0,
    nsqd: nil,
    topic: nil,
    channel: nil,
    backoff_counter: 0,
    backoff_duration: 0,
    max_rdy: 2500,
    connect_attempts: 0,
    stop_flag: false,
    conn_info_pid: nil,
    msg_timeout: nil
  }


  # ------------------------------------------------------- #
  # Behaviour Implementation                                #
  # ------------------------------------------------------- #
  @spec start_link(pid, host_with_port, NSQ.Config.t, String.t, String.t, pid, list) ::
    {:ok, pid}
  def start_link(parent, nsqd, config, topic, channel, conn_info_pid, event_manager_pid, opts \\ []) do
    state = %{@initial_state |
      parent: parent,
      nsqd: nsqd,
      config: config,
      topic: topic,
      channel: channel,
      conn_info_pid: conn_info_pid,
      event_manager_pid: event_manager_pid
    }
    {:ok, _pid} = GenServer.start_link(__MODULE__, state, opts)
  end


  @spec init(state) :: {:ok, state}
  def init(conn_state) do
    {:ok, msg_sup_pid} = NSQ.MessageSupervisor.start_link
    conn_state = %{conn_state | msg_sup_pid: msg_sup_pid}
    init_conn_info(conn_state)
    connect_result = Initializer.connect(conn_state)
    case connect_result do
      {:ok, state} -> {:ok, state}
      {{:error, _reason}, state} -> {:ok, state}
    end
  end


  def terminate(_reason, _state) do
    :ok
  end


  @spec handle_call({:cmd, tuple, atom}, {pid, reference}, state) ::
    {:reply, {:ok, reference}, state} |
    {:reply, {:queued, :nosocket}, state}
  def handle_call({:cmd, cmd, kind}, from, state) do
    {reply, state} = state |> Command.exec(cmd, kind, from)
    {:reply, reply, state}
  end


  @spec handle_call(:stop, {pid, reference}, state) :: {:stop, :normal, state}
  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end


  @spec handle_call(:state, {pid, reference}, state) :: {:reply, state, state}
  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end


  @spec handle_cast({:nsq_msg, binary}, state) :: {:noreply, state}
  def handle_cast({:nsq_msg, msg}, state) do
    {:ok, state} = MessageHandling.handle_nsq_message(msg, state)
    {:noreply, state}
  end


  @spec handle_cast(:flush_cmd_queue, state) :: {:noreply, state}
  def handle_cast(:flush_cmd_queue, state) do
    {:noreply, Command.flush_cmd_queue!(state)}
  end


  @spec handle_cast(:reconnect, state) :: {:noreply, state}
  def handle_cast(:reconnect, conn_state) do
    if conn_state.connect_attempts > 0 do
      {_, conn_state} = Initializer.connect(conn_state)
    end
    {:noreply, conn_state}
  end


  # When a task is done, it automatically messages the return value to the
  # calling process. we can use that opportunity to update the messages in
  # flight.
  @spec handle_info({reference, {:message_done, NSQ.Message.t, any}}, state) ::
    {:noreply, T.conn_state}
  def handle_info({:message_done, _msg, ret_val}, state) do
    state |> MessageHandling.update_conn_stats_on_message_done(ret_val)
    {:noreply, state}
  end


  # ------------------------------------------------------- #
  # API Definitions                                         #
  # ------------------------------------------------------- #
  @spec get_state(pid) :: {:ok, state}
  def get_state(pid) when is_pid(pid) do
    GenServer.call(pid, :state)
  end

  @spec get_state(connection) :: {:ok, state}
  def get_state({_conn_id, pid} = _connection) do
    get_state(pid)
  end


  @spec close(pid, state) :: any
  def close(conn, conn_state \\ nil) do
    Logger.debug "Closing connection #{inspect conn}"
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
        Logger.warn "#{inspect conn}: No more messages in flight. Exiting."
      :timeout ->
        Logger.error "#{inspect conn}: Timed out waiting for messages to finish. Exiting anyway."
    end

    Process.exit(self, :normal)
  end

  @doc """
  Calls the command and waits for a response. If a command shouldn't have a
  response, use cmd_noresponse.
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
  Calls the command but doesn't expect any response.
  """
  @spec cmd_noresponse(pid, tuple) :: {:ok, reference} | {:queued, :nosocket}
  def cmd_noresponse(conn, cmd) do
    GenServer.call(conn, {:cmd, cmd, :noresponse})
  end

  # ------------------------------------------------------- #
  # Private Functions                                       #
  # ------------------------------------------------------- #

  @spec now :: integer
  defp now do
    {megasec, sec, microsec} = :os.timestamp
    1_000_000 * megasec + sec + microsec / 1_000_000
  end

  @spec init_conn_info(state) :: any
  defp init_conn_info(state) do
    ConnInfo.update state, %{
      max_rdy: state.max_rdy,
      rdy_count: 0,
      last_rdy: 0,
      messages_in_flight: 0,
      last_msg_timestamp: now,
      retry_rdy_pid: nil,
      finished_count: 0,
      requeued_count: 0,
      failed_count: 0,
      backoff_count: 0,
    }
  end

  @spec wait_for_zero_in_flight(pid, binary) :: any
  defp wait_for_zero_in_flight(agent_pid, conn_id) do
    [in_flight] = ConnInfo.fetch(agent_pid, conn_id, [:messages_in_flight])
    Logger.debug("Conn #{inspect conn_id}: #{in_flight} still in flight")
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
