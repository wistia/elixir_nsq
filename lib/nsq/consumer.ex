defmodule NSQ.Consumer do
  use GenServer
  require Logger
  import NSQ.Protocol
  import NSQ.SharedConnectionInfo

  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  @initial_state %{
    channel: nil,
    config: %NSQ.Config{},
    conn_sup_pid: nil,
    shared_conn_info_agent: nil,
    max_in_flight: 2500,
    topic: nil,
    message_handler: nil,
    rdy_retry_conns: %{},
    need_rdy_redistributed: false,
    stop_flag: false,
    backoff_counter: 0,
    backoff_duration: 0
  }

  # ------------------------------------------------------- #
  # Behaviour Implementation                                #
  # ------------------------------------------------------- #
  @doc """
  This is the standard way to initialize a consumer. It actually initializes a
  supervisor, but we have it in NSQ.Consumer so end-users don't need to think
  about that.
  """
  def new(topic, channel, config) do
    NSQ.ConsumerSupervisor.start_link(topic, channel, config)
  end

  @doc """
  Starts a Consumer process, called via the supervisor.
  """
  @spec start_link(String.t, String.t, Struct.t) :: {:ok, pid}
  def start_link(topic, channel, config, opts \\ []) do
    {:ok, config} = NSQ.Config.validate(config)
    {:ok, config} = NSQ.Config.normalize(config)
    unless is_valid_topic_name?(topic), do: raise "Invalid topic name #{topic}"
    unless is_valid_channel_name?(channel), do: raise "Invalid channel name #{topic}"

    state = %{@initial_state |
      topic: topic,
      channel: channel,
      config: config,
      max_in_flight: config.max_in_flight
    }
    GenServer.start_link(__MODULE__, state, opts)
  end

  @doc """
  On init, we create a connection for each NSQD instance discovered, and set
  up loops for discovery and RDY redistribution.
  """
  def init(cons_state) do
    {:ok, conn_sup_pid} = NSQ.ConnectionSupervisor.start_link
    cons_state = %{cons_state | conn_sup_pid: conn_sup_pid}

    {:ok, shared_conn_info_agent} = Agent.start_link(fn -> %{} end)
    cons_state = %{cons_state | shared_conn_info_agent: shared_conn_info_agent}

    {:ok, _cons_state} = connect_to_nsqds_on_init(self, cons_state)
  end

  @doc """
  The RDY loop periodically calls this to make sure RDY is balanced among our
  connections. Started from the supervisor.
  """
  def handle_call(:redistribute_rdy, _from, cons_state) do
    {:ok, cons_state} = redistribute_rdy(self, cons_state)
    {:reply, :ok, cons_state}
  end

  @doc """
  The discovery loop calls this periodically to add/remove active nsqd
  connections. Started from the supervisor.
  """
  def handle_call(:discover_nsqds, _from, cons_state) do
    if length(cons_state.config.nsqlookupds) > 0 do
      {:ok, cons_state} = discover_nsqds_and_connect(self, cons_state)
      {:reply, :ok, cons_state}
    else
      # No nsqlookupds are configured. They must be specifying nsqds directly.
      {:reply, :ok, cons_state}
    end
  end

  def handle_call({:start_stop_continue_backoff, backoff_flag}, _from, cons_state) do
    {:ok, cons_state} = start_stop_continue_backoff(self, backoff_flag, cons_state)
    {:reply, :ok, cons_state}
  end

  @doc """
  """
  def handle_call({:update_rdy, conn, count}, _from, cons_state) do
    {:ok, cons_state} = update_rdy(self, conn, count, cons_state)
    {:reply, :ok, cons_state}
  end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  def handle_cast(:resume, state) do
    {:ok, cons_state} = resume(self, state)
    {:noreply, cons_state}
  end

  def handle_cast({:maybe_update_rdy, {_host, _port} = nsqd}, cons_state) do
    conn = conn_from_nsqd(self, nsqd, cons_state)
    {:ok, cons_state} = maybe_update_rdy(self, conn, cons_state)
    {:noreply, cons_state}
  end

  # ------------------------------------------------------- #
  # API Definitions                                         #
  # ------------------------------------------------------- #
  @doc """
  This is the standard way to initialize a consumer. It actually initializes a
  supervisor, but we have it in NSQ.Consumer so end-users don't need to think
  about that.
  """
  def new(topic, channel, config) do
    NSQ.ConsumerSupervisor.start_link(topic, channel, config)
  end

  @spec get_connections(map) :: {String.t, pid}
  def get_connections(cons_state) when is_map(cons_state) do
    children = Supervisor.which_children(cons_state.conn_sup_pid)
    Enum.map children, fn({child_id, pid, _, _}) -> {child_id, pid} end
  end

  @spec get_connections(map) :: {String.t, pid}
  def get_connections(cons, cons_state \\ nil) when is_pid(cons) do
    cons_state = cons_state || get_state(cons)
    children = Supervisor.which_children(cons_state.conn_sup_pid)
    Enum.map children, fn({child_id, pid, _, _}) -> {child_id, pid} end
  end

  @spec connect_to_nsqds_on_init(Struct.t, map) :: {:ok, map}
  def connect_to_nsqds_on_init(cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)

    if length(cons_state.config.nsqlookupds) > 0 do
      {:ok, _cons_state} = discover_nsqds_and_connect(cons, cons_state)
    else
      {:ok, _cons_state} = update_connections(
        cons_state.config.nsqds, cons, cons_state
      )
    end
  end

  @spec discover_nsqds_and_connect(Struct.t, map) :: {:ok, map} | {:error, String.t}
  def discover_nsqds_and_connect(cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)

    if length(cons_state.config.nsqlookupds) > 0 do
      nsqds = NSQ.Connection.nsqds_from_lookupds(
        cons_state.config.nsqlookupds, cons_state.topic
      )
      Logger.debug "Discovered nsqds: #{inspect nsqds}"
      {:ok, cons_state} = update_connections(nsqds, cons, cons_state)
    else
      {:error, "No nsqlookupds given"}
    end
  end

  @spec update_connections([{String.t, integer}], Struct.t, map) :: {:ok, map}
  def update_connections(discovered_nsqds, cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)

    dead_conns = dead_connections(discovered_nsqds, cons, cons_state)
    {:ok, cons_state} = stop_connections(dead_conns, cons, cons_state)

    nsqds_to_connect = new_nsqds(discovered_nsqds, cons, cons_state)
    {:ok, cons_state} = connect_to_nsqds(nsqds_to_connect, cons, cons_state)

    {:ok, cons_state}
  end

  @spec connect_to_nsqds([{String.t, integer}], Struct.t, map) :: {:ok, map}
  def connect_to_nsqds(nsqds, cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    cons_state = Enum.reduce nsqds, cons_state, fn(nsqd, last_state) ->
      {:ok, new_state} = connect_to_nsqd(nsqd, cons, last_state)
      new_state
    end
    {:ok, cons_state}
  end

  # Adds a new NSQD connection and sends it RDY 1 to bootstrap.
  def connect_to_nsqd(nsqd, cons, cons_state) do
    {:ok, pid} = NSQ.ConnectionSupervisor.start_child(
      cons, nsqd, cons_state
    )
    {:ok, _cons_state} = send_rdy(cons, {nsqd, pid}, 1, cons_state)
  end

  @doc """
  Given a list of connections, force them to stop. Return the new state without
  those connections.
  """
  @spec stop_connections([{String.t, pid}], Struct.t, map) :: {:ok, map}
  def stop_connections(dead_conns, cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)

    cons_state = Enum.reduce dead_conns, cons_state, fn({nsqd, _pid}, last_state) ->
      {:ok, new_state} = stop_connection(cons, nsqd, last_state)
      new_state
    end

    {:ok, cons_state}
  end

  def stop_connection(cons, nsqd, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    Logger.warn "stop connection #{inspect nsqd}"

    # Delete the connection info from the shared map so we don't use it to
    # perform calculations.
    conn_id = get_conn_id(cons, nsqd)
    delete_conn_info(cons_state, conn_id)

    # Terminate the connection for real.
    # TODO: Change this method to `kill_connection` and make `stop_connection`
    # graceful.
    Supervisor.terminate_child(cons_state.conn_sup_pid, conn_id)

    {:ok, cons_state}
  end

  @doc """
  We may have open connections and nsqlookupd stops reporting them. This
  function tells us which connections we have stored in state but not in
  nsqlookupd.
  """
  @spec dead_connections([{String.t, integer}], Struct.t, map) :: [{String.t, pid}]
  def dead_connections(discovered_nsqds, cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    Enum.reject get_connections(cons_state), fn(conn) ->
      conn_already_discovered?(cons, conn, discovered_nsqds)
    end
  end

  @doc """
  When nsqlookupd reports available producers, there are some that may not
  already be in our connection list. This function reports which ones are new
  so we can connect to them.
  """
  @spec new_nsqds([{String.t, integer}], Struct.t, map) :: [{String.t, integer}]
  def new_nsqds(discovered_nsqds, cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    Enum.reject discovered_nsqds, fn(nsqd) ->
      nsqd_already_has_connection?(nsqd, cons, cons_state)
    end
  end

  @doc """
  Initialized from NSQ.ConsumerSupervisor, sends the consumer a message on a
  fixed interval.
  """
  def rdy_loop(cons, opts \\ []) do
    cons_state = get_state(cons)
    GenServer.call(cons, :redistribute_rdy)
    delay = cons_state.config.rdy_redistribute_interval
    :timer.sleep(delay)
    rdy_loop(cons)
  end

  @doc """
  Initialized from NSQ.ConsumerSupervisor, sends the consumer a message on a
  fixed interval.
  """
  def discovery_loop(cons, opts \\ []) do
    cons_state = get_state(cons)
    %NSQ.Config{
      lookupd_poll_interval: poll_interval,
      lookupd_poll_jitter: poll_jitter
    } = cons_state.config
    delay = poll_interval + round(poll_interval * poll_jitter * :random.uniform)
    :timer.sleep(delay)

    GenServer.call(cons, :discover_nsqds)
    discovery_loop(cons)
  end

  def get_state(cons) do
    GenServer.call(cons, :state)
  end

  def start_stop_continue_backoff(cons, backoff_signal, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)

    backoff_updated = false
    backoff_counter = cons_state.backoff_counter
    cond do
      backoff_signal == :resume ->
        backoff_updated = true
        backoff_counter = backoff_counter - 1
      backoff_signal == :backoff ->
        backoff_updated = true
        backoff_counter = backoff_counter + 1
      true ->
        backoff_updated = false
    end
    cons_state = %{cons_state | backoff_counter: backoff_counter}

    cond do
      backoff_counter == 0 && backoff_updated ->
        count = per_conn_max_in_flight(cons, cons_state)
        Logger.warn "exiting backoff, returning all to RDY #{count}"
        cons_state = Enum.reduce get_connections(cons_state), cons_state, fn(conn, last_state) ->
          {:ok, new_state} = update_rdy(cons, conn, count, last_state)
          new_state
        end
        {:ok, cons_state}
      backoff_counter > 0 ->
        backoff_duration = calculate_backoff(cons_state)
        Logger.warn "backing off for #{backoff_duration / 1000} seconds (backoff level #{backoff_counter}), setting all to RDY 0"
        # send RDY 0 immediately (to *all* connections)
        cons_state = Enum.reduce get_connections(cons_state), cons_state, fn(conn, last_state) ->
          {:ok, new_state} = update_rdy(cons, conn, 0, last_state)
          new_state
        end
        resume_from_backoff_later(cons, backoff_duration, cons_state)
      true ->
        {:ok, cons_state}
    end
  end

  def calculate_backoff(cons_state) do
    case cons_state.config.backoff_strategy do
      :exponential -> exponential_backoff(cons_state)
      :test -> 200
    end
  end

  def exponential_backoff(cons_state) do
    attempts = cons_state.backoff_counter
    mult = cons_state.config.backoff_multiplier
    min(
      mult * :math.pow(2, attempts),
      cons_state.config.max_backoff_duration
    ) |> round
  end

  @doc """
  """
  def resume_from_backoff_later(cons, duration, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    spawn_link fn ->
      :timer.sleep(duration)
      GenServer.cast(cons, :resume)
      resume(cons)
    end
    cons_state = %{cons_state | backoff_duration: duration}
    {:ok, cons_state}
  end

  @doc """
  This function is called asynchronously from `resume_from_backoff_later`. It
  will cause one connection to have RDY 1. We only resume after this if
  messages succeed a number of times == backoff_counter. (That logic is in
  start_stop_continue_backoff.)
  """
  def resume(cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    if cons_state.backoff_duration == 0 || cons_state.backoff_counter == 0 do
      # looks like we successfully left backoff mode already
      {:ok, cons_state}
    else
      if cons_state.stop_flag do
        {:ok, %{cons_state | backoff_duration: 0}}
      else
        # pick a random connection to test the waters
        conn_count = count_connections(cons_state)

        if conn_count == 0 do
          Logger.warn("no connection available to resume")
          Logger.warn("backing off for 1 second")
          {:ok, ons_state} = resume_from_backoff_later(cons, 1000, cons_state)
        else
          if cons_state.config.backoff_strategy == :test do
            # When testing, we're only sending 1 message at a time to a single
            # nsqd. In this mode, instead of a random connection, always use the
            # first one that was defined, which ends up being the last one in our
            # list.
            conn = cons_state |> get_connections |> List.last
          else
            conn = cons_state |> get_connections |> Enum.random
          end
          Logger.warn("(#{inspect conn}) backoff timeout expired, sending RDY 1")

          # while in backoff only ever let 1 message at a time through
          {:ok, cons_state} = update_rdy(cons, conn, 1, cons_state)
        end

        {:ok, %{cons_state | backoff_duration: 0}}
      end
    end
  end

  @doc """
  This will only be triggered in odd cases where we're in backoff or when there
  are more connections than max in flight. It will randomly change RDY on
  some connections to 0 so that they're all guaranteed to eventually process
  messages.
  """
  def redistribute_rdy(cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)

    if should_redistribute_rdy?(cons_state) do
      conns = get_connections(cons_state)
      max_in_flight = cons_state.max_in_flight
      in_backoff = cons_state.backoff_counter > 0
      conn_count = length(conns)

      if conn_count > max_in_flight do
        Logger.debug("redistributing RDY state (#{conn_count} conns > #{max_in_flight} max_in_flight)")
      end

      if in_backoff && conn_count > 1 do
        Logger.debug("redistributing RDY state (in backoff and #{conn_count} conns > 1)")
      end

      possible_conns = Enum.map conns, fn(conn) ->
        conn_id = get_conn_id(conn)
        [last_msg_t, rdy_count] = fetch_conn_info(cons_state, conn_id, [:last_msg_timestamp, :rdy_count])
        time_since_last_msg = now - last_msg_t

        Logger.debug("(#{inspect conn}) rdy: #{rdy_count} (last message received #{inspect datetime_from_timestamp(time_since_last_msg)})")
        if rdy_count > 0 && time_since_last_msg > cons_state.config.low_rdy_idle_timeout do
          Logger.debug("(#{inspect conn}) idle connection, giving up RDY")
          {:ok, _cons_state} = update_rdy(cons, conn, 0, cons_state)
        end

        conn
      end

      total_rdy = total_rdy_count(cons_state)
      available_max_in_flight = max_in_flight - total_rdy
      if in_backoff do
        available_max_in_flight = 1 - total_rdy
      end

      redistribute_rdy_r(cons, possible_conns, available_max_in_flight, cons_state)
    else
      # nothing to do
      {:ok, cons_state}
    end
  end

  def maybe_update_rdy(cons, conn, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    in_backoff = cons_state.backoff_counter > 0
    in_backoff_timeout = cons_state.backoff_duration > 0

    if in_backoff || in_backoff_timeout do
      Logger.debug("(#{inspect conn}) skip sending RDY in_backoff:#{in_backoff} || "
        <> "in_backoff_timeout:#{in_backoff_timeout}")
      {:ok, cons_state}
    else
      [remain, last_rdy] = fetch_conn_info(cons_state, get_conn_id(conn), [:rdy_count, :last_rdy])
      count = per_conn_max_in_flight(cons, cons_state)

      if remain <= 1 || remain < (last_rdy / 4) || (count > 0 && count < remain) do
        Logger.debug("(#{inspect conn}) sending RDY #{count} (#{remain} remain from last RDY #{last_rdy})")
        {:ok, _cons_state} = update_rdy(cons, conn, count, cons_state)
      else
        Logger.debug("(#{inspect conn}) skip sending RDY #{count} (#{remain} remain out of last RDY #{last_rdy})")
        {:ok, cons_state}
      end
    end
  end

  def update_rdy(cons, conn, count, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)

    conn_info = fetch_conn_info(cons_state, get_conn_id(conn))
    if count > conn_info.max_rdy, do: count = conn_info.max_rdy

    # If this is for a connection that's retrying, kill the timer and clean up.
    if retry_pid = cons_state.rdy_retry_conns[conn] do
      Logger.debug("#{inspect conn} rdy retry pid #{inspect retry_pid} detected, killing")
      Process.exit(retry_pid, :kill)
      cons_state = %{cons_state |
        rdy_retry_conns: Map.delete(cons_state.rdy_retry_conns, conn)
      }
    end

    rdy_count = conn_info.rdy_count
    max_in_flight = cons_state.max_in_flight
    total_rdy = total_rdy_count(cons_state)
    max_possible_rdy = max_in_flight + total_rdy + rdy_count

    if max_possible_rdy > 0 && max_possible_rdy < count do
      count = max_possible_rdy
    end

    if max_possible_rdy <= 0 && count > 0 do
      if rdy_count == 0 do
        # Schedule update_rdy(consumer, conn, count) for this connection again
        # in 5 seconds. This is to prevent eternal starvation.
        Logger.debug("(#{inspect conn}) retry RDY in 5 seconds")
        retry_rdy(cons, conn, count, cons_state)
      end
      {:error, :over_max_in_flight}
    else
      {:ok, _cons_state} = send_rdy(cons, conn, count, cons_state)
    end
  end

  def retry_rdy(cons, conn, count, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)

    retry_pid = spawn fn ->
      :timer.sleep(5000)
      GenServer.call(cons, {:update_rdy, conn, count})
    end
    rdy_retry_conns = Map.put(cons_state.rdy_retry_conns, conn, retry_pid)

    {:ok, retry_pid, %{cons_state | rdy_retry_conns: rdy_retry_conns}}
  end

  def send_rdy(cons, {_id, pid} = conn, count, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)

    [last_rdy] = fetch_conn_info(cons_state, get_conn_id(conn), [:last_rdy])

    if count == 0 && last_rdy == 0 do
      {:ok, cons_state}
    else
      # We intentionally don't match this GenServer.call. If the socket isn't
      # set up or is erroring out, we don't want to propagate that connection
      # error to the consumer.
      result = NSQ.Connection.cmd_noresponse(pid, {:rdy, count})
      {:ok, cons_state}
    end
  end

  def per_conn_max_in_flight(cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    max_in_flight = cons_state.max_in_flight
    conn_count = count_connections(cons_state)
    min(max(1, max_in_flight / conn_count), max_in_flight) |> round
  end

  def total_rdy_count(agent_pid) when is_pid(agent_pid) do
    reduce_conn_info agent_pid, 0, fn({_, conn_info}, acc) ->
      acc + conn_info.rdy_count
    end
  end

  def total_rdy_count(%{shared_conn_info_agent: agent_pid} = _cons_state) do
    total_rdy_count(agent_pid)
  end

  # ------------------------------------------------------- #
  # Private Functions                                       #
  # ------------------------------------------------------- #
  defp now do
    :calendar.datetime_to_gregorian_seconds(:calendar.universal_time)
  end

  epoch = {{1970, 1, 1}, {0, 0, 0}}
  @epoch :calendar.datetime_to_gregorian_seconds(epoch)
  defp datetime_from_timestamp(timestamp) do
    timestamp
    |> +(@epoch)
    |> :calendar.gregorian_seconds_to_datetime
  end

  defp count_connections(cons_state) do
    %{active: active} = Supervisor.count_children(cons_state.conn_sup_pid)
    active
  end

  defp conn_already_discovered?(cons, {conn_id, _}, discovered_nsqds) do
    Enum.any? discovered_nsqds, fn(nsqd) ->
      get_conn_id(cons, nsqd) == conn_id
    end
  end

  defp nsqd_already_has_connection?(nsqd, cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    needle = get_conn_id(cons, nsqd)
    Enum.any? get_connections(cons_state), fn({conn_id, _}) ->
      conn_id == needle
    end
  end

  # Helper for redistribute_rdy; we set RDY to 1 for _some_ connections that
  # were halted, randomly, until there's no more RDY left to assign.
  defp redistribute_rdy_r(cons, possible_conns, available_max_in_flight, cons_state) do
    cons_state = cons_state || get_state(cons)
    if length(possible_conns) == 0 || available_max_in_flight <= 0 do
      {:ok, cons_state}
    else
      [conn|rest] = Enum.shuffle(possible_conns)
      Logger.debug("(#{inspect conn}) redistributing RDY")
      {:ok, cons_state} = update_rdy(cons, conn, 1, cons_state)
      redistribute_rdy_r(cons, rest, available_max_in_flight - 1, cons_state)
    end
  end

  defp should_redistribute_rdy?(cons_state) do
    conn_count = count_connections(cons_state)
    in_backoff = cons_state.backoff_counter > 0
    in_backoff_timeout = cons_state.backoff_duration > 0

    !in_backoff_timeout
      && conn_count > 0
      && (
        conn_count > cons_state.max_in_flight
        || (in_backoff && conn_count > 1)
        || cons_state.need_rdy_redistributed
      )
  end

  # TODO: We can do this with a fold or reduce instead of recursion
  defp connections_maybe_update_rdy(connections, cons, cons_state) do
    if connections == [] do
      {:ok, cons_state}
    else
      [conn|rest] = connections
      {:ok, cons_state} = maybe_update_rdy(cons, conn, cons_state)
      connections_maybe_update_rdy(rest, cons, cons_state)
    end
  end

  # The end-user will be targeting the supervisor, but it's the consumer that
  # can actually handle the command.
  def get(sup_pid) do
    children = Supervisor.which_children(sup_pid)
    child = Enum.find(children, fn({kind, pid, _, _}) -> kind == NSQ.Consumer end)
    {_, pid, _, _} = child
    pid
  end

  defp conn_from_nsqd(cons, nsqd, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    needle = get_conn_id(cons, nsqd)
    Enum.find get_connections(cons_state), fn({conn_id, pid}) ->
      needle == conn_id
    end
  end

  defp pid_from_nsqd(cons, nsqd, cons_state \\ nil) do
    case conn_from_nsqd(cons, nsqd, cons_state) do
      nil -> nil
      {conn_id, pid} -> pid
    end
  end
end
