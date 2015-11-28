defmodule NSQ.Consumer do
  use GenServer
  require Logger
  import NSQ.Protocol

  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  @initial_state %{
    channel: nil,
    config: %NSQ.Config{},
    conn_sup_pid: nil,
    max_in_flight: 2500,
    topic: nil,
    message_handler: nil,
    total_rdy_count: 0,
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

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  @spec connections(map) :: {String.t, pid}
  def connections(cons_state) when is_map(cons_state) do
    children = Supervisor.which_children(cons_state.conn_sup_pid)
    Enum.map children, fn({child_id, pid, _, _}) -> {child_id, pid} end
  end

  @spec connections(map) :: {String.t, pid}
  def connections(cons, cons_state \\ nil) when is_pid(cons) do
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

  @spec connect_to_nsqds_on_init(Struct.t, map) :: {:ok, map} | {:error, String.t}
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

    {:ok, cons_state} = connections_maybe_update_rdy(
      connections(cons_state), cons, cons_state
    )

    {:ok, cons_state}
  end

  @spec connect_to_nsqds([{String.t, integer}], Struct.t, map) :: {:ok, map}
  def connect_to_nsqds(nsqds, cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    new_conns = Enum.map nsqds, fn(nsqd) ->
      {:ok, conn} = NSQ.ConnectionSupervisor.start_child(
        cons, nsqd, cons_state
      )
      {nsqd, conn}
    end
    {:ok, cons_state}
  end

  @doc """
  Given a list of connections, force them to stop. Return the new state without
  those connections.
  """
  @spec connect_to_nsqds([{String.t, pid}], Struct.t, map) :: {:ok, map}
  def stop_connections(dead_conns, cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)

    Enum.map dead_conns, fn({_nsqd, pid}) ->
      stop_connection(cons, pid, cons_state)
    end

    {:ok, cons_state}
  end

  def stop_connection(cons, nsqd, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    Supervisor.terminate_child(
      cons_state.conn_sup_pid, NSQ.Connection.connection_id(cons, nsqd)
    )
  end

  @doc """
  We may have open connections and nsqlookupd stops reporting them. This
  function tells us which connections we have stored in state but not in
  nsqlookupd.
  """
  @spec connect_to_nsqds([{String.t, integer}], Struct.t, map) :: [{String.t, pid}]
  def dead_connections(discovered_nsqds, cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    Enum.reject connections(cons_state), fn(conn) ->
      conn_already_discovered?(cons, conn, discovered_nsqds)
    end
  end

  @doc """
  When nsqlookupd reports available producers, there are some that may not
  already be in our connection list. This function reports which ones are new
  so we can connect to them.
  """
  @spec connect_to_nsqds([{String.t, integer}], Struct.t, map) :: [{String.t, integer}]
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

  # TODO: Test and doc
  def backoff(cons, duration, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    spawn_link fn ->
      :timer.sleep(duration)
      resume(cons)
    end
    {:ok, %{cons_state | backoff_duration: duration}}
  end

  # TODO: Test and doc
  def resume(cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    if cons_state.stop_flag do
      {:ok, %{cons_state | backoff_duration: 0}}
    else
      # pick a random connection to test the waters
      conn_count = count_connections(cons_state)

      if conn_count == 0 do
        Logger.warn("no connection available to resume")
        Logger.warn("backing off for 1 second")
        {:ok, _cons_state} = backoff(cons, 1000, cons_state)
      else
        conn = Enum.random(connections(cons_state))
        Logger.warning("(#{inspect conn}) backoff timeout expired, sending RDY 1")

        # while in backoff only ever let 1 message at a time through
        {:ok, _cons_state} = update_rdy(cons, conn, 1, cons_state)
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
      conns = connections(cons_state)
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
        conn_state = NSQ.Connection.get_state(conn)
        time_since_last_msg = now - conn_state.last_msg_timestamp
        rdy_count = conn_state.rdy_count

        Logger.debug("(#{inspect conn}) rdy: #{rdy_count} (last message received #{inspect datetime_from_timestamp(time_since_last_msg)})")
        if rdy_count > 0 && time_since_last_msg > cons_state.config.low_rdy_idle_timeout do
          Logger.debug("(#{inspect conn}) idle connection, giving up RDY")
          {:ok, _cons_state} = update_rdy(cons, conn, 0, cons_state, conn_state)
        end

        conn
      end

      available_max_in_flight = max_in_flight - cons_state.total_rdy_count
      if in_backoff do
        available_max_in_flight = 1 - cons_state.total_rdy_count
      end

      redistribute_rdy_r(cons, possible_conns, available_max_in_flight, cons_state)
    else
      # nothing to do
      {:ok, cons_state}
    end
  end

  def maybe_update_rdy(cons, conn, cons_state \\ nil, conn_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    conn_state = conn_state || NSQ.Connection.get_state(conn)

    in_backoff = cons_state.backoff_counter > 0
    in_backoff_timeout = cons_state.backoff_duration > 0

    if in_backoff || in_backoff_timeout do
      Logger.debug("(#{inspect conn}) skip sending RDY in_backoff:#{in_backoff} || "
        <> "in_backoff_timeout:#{in_backoff_timeout}")
      {:ok, cons_state}
    else
      remain = conn_state.rdy_count
      last_rdy_count = conn_state.last_rdy
      count = per_conn_max_in_flight(cons, cons_state)

      if remain <= 1 || remain < (last_rdy_count / 4) || (count > 0 && count < remain) do
        Logger.debug("(#{inspect conn}) sending RDY #{count} (#{remain} remain from last RDY #{last_rdy_count})")
        {:ok, _cons_state} = update_rdy(cons, conn, count, cons_state, conn_state)
      else
        Logger.debug("(#{inspect conn}) skip sending RDY #{count} (#{remain} remain out of last RDY #{last_rdy_count})")
        {:ok, cons_state}
      end
    end
  end

  def update_rdy(cons, conn, count, cons_state \\ nil, conn_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    conn_state = conn_state || NSQ.Connection.get_state(conn)

    if count > conn_state.max_rdy, do: count = conn_state.max_rdy

    # If this is for a connection that's retrying, kill the timer and clean up.
    if retry_pid = cons_state.rdy_retry_conns[conn] do
      Logger.debug("#{inspect conn} rdy retry pid #{inspect retry_pid} detected, killing")
      Process.exit(retry_pid, :kill)
      cons_state = %{cons_state |
        rdy_retry_conns: Map.delete(cons_state.rdy_retry_conns, conn)
      }
    end

    rdy_count = conn_state.rdy_count
    max_in_flight = cons_state.max_in_flight
    total_rdy_count = cons_state.total_rdy_count
    max_possible_rdy = max_in_flight + total_rdy_count + rdy_count

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
      {:ok, cons_state} = send_rdy(cons, conn, count, cons_state)
      total_rdy_count = cons_state.total_rdy_count - conn_state.rdy_count + count
      {:ok, %{cons_state | total_rdy_count: total_rdy_count}}
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

  def send_rdy(cons, {_nsqd, pid} = conn, count, cons_state \\ nil, conn_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    conn_state = conn_state || NSQ.Connection.get_state(conn)

    if count == 0 && conn_state.last_rdy == 0 do
      {:ok, cons_state}
    else
      # We intentionally don't match this call. If the socket isn't set up or
      # is erroring out, we don't want to propagate that connection error to
      # the consumer.
      GenServer.call(pid, {:rdy, count})
      {:ok, cons_state}
    end
  end

  def per_conn_max_in_flight(cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    max_in_flight = cons_state.max_in_flight
    conn_count = count_connections(cons_state)
    min(max(1, max_in_flight / conn_count), max_in_flight) |> round
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
      NSQ.Connection.connection_id(cons, nsqd) == conn_id
    end
  end

  defp nsqd_already_has_connection?(nsqd, cons, cons_state \\ nil) do
    cons_state = cons_state || get_state(cons)
    nsqd_id = NSQ.Connection.connection_id(cons, nsqd)
    Enum.any? connections(cons_state), fn({child_id, _}) ->
      child_id == nsqd_id
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

  # TODO: We can do this with a fold or map_reduce instead of recursion
  defp connections_maybe_update_rdy(connections, cons, cons_state) do
    if connections == [] do
      {:ok, cons_state}
    else
      [conn|rest] = connections
      {:ok, cons_state} = maybe_update_rdy(cons, conn, cons_state)
      connections_maybe_update_rdy(rest, cons, cons_state)
    end
  end
end
