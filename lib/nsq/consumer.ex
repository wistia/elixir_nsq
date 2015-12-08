defmodule NSQ.Consumer do
  @moduledoc """
  A consumer is a process that creates connections to NSQD to receive messages
  for a specific topic and channel. It has three primary functions:

  1. Provide a simple interface for a user to setup and configure message
     handlers.
  2. Balance RDY across all available connections.
  3. Add/remove connections as they are discovered.

  ## Simple Interface

  In standard practice, the only function a user should need to know about is
  `NSQ.Consumer.new/3`. It takes a topic, a channel, and an NSQ.Config struct,
  which has possible values defined and explained in nsq/config.ex.

      {:ok, consumer} = NSQ.Consumer.new("my-topic", "my-channel", %NSQ.Config{
        nsqlookupds: ["127.0.0.1:6751", "127.0.0.1:6761"],
        message_handler: fn(body, msg) ->
          # handle them message
          :ok
        end
      })

  ### Message handler return values

  The return value of the message handler determines how we will respond to
  NSQ.

  #### :ok

  The message was handled and should not be requeued. This sends a FIN command
  to NSQD.

  #### :req

  This message should be requeued. With no delay specified, it will calculate
  delay exponentially based on the number of attempts. Refer to
  Message.calculate_delay for the exact formula.

  #### {:req, delay}

  This message should be requeued. Use the delay specified. A positive integer
  is expected.

  #### {:req, delay, backoff}

  This message should be requeued. Use the delay specified. If `backoff` is
  truthy, the consumer will temporarily set RDY to 0 in order to stop receiving
  messages. It will use a standard strategy to resume from backoff mode.

  This type of return value is only meant for exceptional cases, such as
  internal network partitions, where stopping message handling briefly could be
  beneficial. Only use this return value if you know what you're doing.

  A message handler that throws an unhandled exception will automatically
  requeue and enter backoff mode.

  ### NSQ.Message.touch(msg)

  NSQ.Config has a property called msg_timeout, which configures the NSQD
  server to wait that long before assuming the message failed and requeueing
  it. If you expect your message handler to take longer than that, you can call
  `NSQ.Message.touch(msg)` from the message handler to reset the server-side
  timer.

  ### NSQ.Consumer.change_max_in_flight(consumer, max_in_flight)

  If you'd like to manually change the max in flight of a consumer, use this
  function. It will cause the consumer's connections to rebalance to the new
  value. If the new `max_in_flight` is smaller than the current messages in
  flight, it must wait for the existing handlers to finish or requeue before
  it can fully rebalance.
  """

  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  use GenServer
  require Logger
  import NSQ.Protocol
  alias NSQ.ConnInfo, as: ConnInfo

  # ------------------------------------------------------- #
  # Module Attributes                                       #
  # ------------------------------------------------------- #
  @initial_state %{
    channel: nil,
    config: %NSQ.Config{},
    conn_sup_pid: nil,
    conn_info_pid: nil,
    max_in_flight: 2500,
    topic: nil,
    message_handler: nil,
    need_rdy_redistributed: false,
    stop_flag: false,
    backoff_counter: 0,
    backoff_duration: 0
  }

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
  @type cons_state :: %{conn_sup_pid: pid, config: NSQ.Config.t, conn_info_pid: pid}

  # ------------------------------------------------------- #
  # Behaviour Implementation                                #
  # ------------------------------------------------------- #
  @doc """
  Starts a Consumer process, called via the supervisor.
  """
  @spec start_link(String.t, String.t, NSQ.Config.t, list) :: {:ok, pid}
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
  @spec init(map) :: {:ok, cons_state}
  def init(cons_state) do
    {:ok, conn_sup_pid} = NSQ.ConnectionSupervisor.start_link
    cons_state = %{cons_state | conn_sup_pid: conn_sup_pid}

    {:ok, conn_info_pid} = Agent.start_link(fn -> %{} end)
    cons_state = %{cons_state | conn_info_pid: conn_info_pid}

    cons_state = %{cons_state | max_in_flight: cons_state.config.max_in_flight}

    {:ok, _cons_state} = discover_nsqds_and_connect(self, cons_state)
  end

  @doc """
  The RDY loop periodically calls this to make sure RDY is balanced among our
  connections. Called from ConsumerSupervisor.
  """
  @spec handle_call(:redistribute_rdy, {reference, pid}, cons_state) ::
    {:reply, :ok, cons_state}
  def handle_call(:redistribute_rdy, _from, cons_state) do
    {:ok, cons_state} = redistribute_rdy(self, cons_state)
    {:reply, :ok, cons_state}
  end

  @doc """
  The discovery loop calls this periodically to add/remove active nsqd
  connections. Called from ConsumerSupervisor.
  """
  @spec handle_call(:discover_nsqds, {reference, pid}, cons_state) ::
    {:reply, :ok, cons_state}
  def handle_call(:discover_nsqds, _from, cons_state) do
    {:ok, cons_state} = delete_dead_connections(cons_state)
    {:ok, cons_state} = discover_nsqds_and_connect(self, cons_state)
    {:reply, :ok, cons_state}
  end

  @doc """
  Called from `NSQ.Message.fin/1`. Not for external use.
  """
  @spec handle_call({:start_stop_continue_backoff, atom}, {reference, pid}, cons_state) ::
    {:reply, :ok, cons_state}
  def handle_call({:start_stop_continue_backoff, backoff_flag}, _from, cons_state) do
    {:ok, cons_state} = start_stop_continue_backoff(self, backoff_flag, cons_state)
    {:reply, :ok, cons_state}
  end

  @doc """
  Called from `retry_rdy/4`. Not for external use.
  """
  @spec handle_call({:update_rdy, connection, integer}, {reference, pid}, cons_state) ::
    {:reply, :ok, cons_state}
  def handle_call({:update_rdy, conn, count}, _from, cons_state) do
    {:ok, cons_state} = update_rdy(self, conn, count, cons_state)
    {:reply, :ok, cons_state}
  end

  @doc """
  Called from tests to assert correct consumer state. Not for external use.
  """
  @spec handle_call(:state, {reference, pid}, cons_state) ::
    {:reply, cons_state, cons_state}
  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  @doc """
  Called from `NSQ.Consumer.change_max_in_flight(consumer, max_in_flight)`. Not
  for external use.
  """
  @spec handle_call({:max_in_flight, integer}, {reference, pid}, cons_state) ::
    {:reply, :ok, cons_state}
  def handle_call({:max_in_flight, new_max_in_flight}, _from, state) do
    state = %{state | max_in_flight: new_max_in_flight}
    {:reply, :ok, state}
  end

  @doc """
  Called from `resume_from_backoff_later/3`. Not for external use.
  """
  @spec handle_cast(:resume, cons_state) :: {:noreply, cons_state}
  def handle_cast(:resume, state) do
    {:ok, cons_state} = resume(self, state)
    {:noreply, cons_state}
  end

  @doc """
  Called from `NSQ.Connection.handle_cast({:nsq_msg, _}, _)` after each message
  is received. Not for external use.
  """
  @spec handle_cast({:maybe_update_rdy, host_with_port}, cons_state) ::
    {:noreply, cons_state}
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
  @spec new(String.t, String.t, NSQ.Config.t) :: {:ok, pid}
  def new(topic, channel, config) do
    NSQ.ConsumerSupervisor.start_link(topic, channel, config)
  end

  @doc """
  Returns all live connections for a consumer. This function, which takes
  a consumer's entire state as an argument, is for convenience. Not for
  external use.
  """
  @spec get_connections(cons_state) :: [connection]
  def get_connections(%{conn_sup_pid: conn_sup_pid}) do
    children = Supervisor.which_children(conn_sup_pid)
    Enum.map children, fn({child_id, pid, _, _}) -> {child_id, pid} end
  end

  @doc """
  Returns all live connections for a consumer. Used in tests. Not for external
  use.
  """
  @spec get_connections(pid, cons_state) :: [connection]
  def get_connections(cons, cons_state \\ nil) when is_pid(cons) do
    cons_state = cons_state || get_state(cons)
    children = Supervisor.which_children(cons_state.conn_sup_pid)
    Enum.map children, fn({child_id, pid, _, _}) -> {child_id, pid} end
  end

  @doc """
  Finds and updates list of live NSQDs using either NSQ.Config.nsqlookupds or
  NSQ.Config.nsqds, depending on what's configured. Preference is given to
  nsqlookupd. Not for external use.
  """
  @spec discover_nsqds_and_connect(pid, cons_state) :: {:ok, cons_state}
  def discover_nsqds_and_connect(cons, cons_state) do
    nsqds = cond do
      length(cons_state.config.nsqlookupds) > 0 ->
        Logger.debug "(#{inspect self}) Discovering nsqds via nsqlookupds #{inspect cons_state.config.nsqlookupds}"
        NSQ.Connection.nsqds_from_lookupds(
          cons_state.config.nsqlookupds, cons_state.topic
        )

      length(cons_state.config.nsqds) > 0 ->
        Logger.debug "(#{inspect self}) Using configured nsqds #{inspect cons_state.config.nsqds}"
        cons_state.config.nsqds

      true ->
        raise "No nsqds or nsqlookupds are configured"
    end

    {:ok, _cons_state} = update_connections(nsqds, cons, cons_state)
  end

  @doc """
  Any inactive connections will be killed and any newly discovered connections
  will be added. Existing connections with no change are left alone. Not for
  external use.
  """
  @spec update_connections([host_with_port], pid, cons_state) ::
    {:ok, cons_state}
  def update_connections(discovered_nsqds, cons, cons_state) do
    dead_conns = dead_connections(discovered_nsqds, cons, cons_state)
    {:ok, cons_state} = stop_connections(dead_conns, cons, cons_state)

    nsqds_to_connect = new_nsqds(discovered_nsqds, cons, cons_state)
    {:ok, cons_state} = connect_to_nsqds(nsqds_to_connect, cons, cons_state)

    {:ok, cons_state}
  end

  @doc """
  Given a list of NSQD hosts, open a connection for each.
  """
  @spec connect_to_nsqds([host_with_port], pid, cons_state) ::
    {:ok, cons_state}
  def connect_to_nsqds(nsqds, cons, cons_state \\ nil) do
    cons_state = Enum.reduce nsqds, cons_state, fn(nsqd, last_state) ->
      {:ok, new_state} = connect_to_nsqd(nsqd, cons, last_state)
      new_state
    end
    {:ok, cons_state}
  end

  @doc """
  Create a connection to NSQD and add it to the consumer's supervised list.
  Not for external use.
  """
  @spec connect_to_nsqd(host_with_port, pid, cons_state) :: {:ok, cons_state}
  def connect_to_nsqd(nsqd, cons, cons_state) do
    {:ok, _pid} = NSQ.ConnectionSupervisor.start_child(
      cons, nsqd, cons_state
    )

    # We normally set RDY to 1, but if we're spawning more connections than
    # max_in_flight, we don't want to break our contract. In that case, the
    # `redistribute_rdy` loop will take care of getting this connection some
    # messages later.
    remaining_rdy = cons_state.max_in_flight - total_rdy_count(cons_state)
    if remaining_rdy > 0 do
      conn = conn_from_nsqd(cons, nsqd, cons_state)
      {:ok, cons_state} = send_rdy(conn, 1, cons_state)
    end

    {:ok, cons_state}
  end

  @doc """
  Given a list of connections, force them to stop. Return the new state without
  those connections.
  """
  @spec stop_connections([connection], pid, cons_state) :: {:ok, cons_state}
  def stop_connections(dead_conns, cons, cons_state) do
    cons_state = Enum.reduce dead_conns, cons_state, fn({nsqd, _pid}, last_state) ->
      {:ok, new_state} = stop_connection(cons, nsqd, last_state)
      new_state
    end

    {:ok, cons_state}
  end

  @doc """
  Given a single connection, immediately terminate its process (and all
  descendant processes, such as message handlers) and remove its info from the
  ConnInfo agent. Not for external use.
  """
  @spec stop_connection(pid, host_with_port, cons_state) :: {:ok, cons_state}
  def stop_connection(cons, nsqd, cons_state) do
    # Terminate the connection for real.
    # TODO: Change this method to `kill_connection` and make `stop_connection`
    # graceful.
    conn_id = ConnInfo.conn_id(cons, nsqd)
    Supervisor.terminate_child(cons_state.conn_sup_pid, conn_id)
    {:ok, cons_state} = cleanup_connection(cons, nsqd, cons_state)

    {:ok, cons_state}
  end

  @doc """
  When a connection is terminated or dies, we must do some extra cleanup.
  First, a terminated process isn't necessarily removed from the supervisor's
  list; therefore we call `Supervisor.delete_child/2`. And info about this
  connection like RDY must be removed so it doesn't contribute to `total_rdy`.
  Not for external use.
  """
  @spec cleanup_connection(pid, host_with_port, cons_state)
    :: {:ok, cons_state}
  def cleanup_connection(cons, nsqd, cons_state) do
    conn_id = ConnInfo.conn_id(cons, nsqd)

    # If a connection is terminated normally or non-normally, it will still be
    # listed in the supervision tree. Let's remove it when we clean up.
    Supervisor.delete_child(cons_state.conn_sup_pid, conn_id)

    # Delete the connection info from the shared map so we don't use it to
    # perform calculations.
    ConnInfo.delete(cons_state, conn_id)

    {:ok, cons_state}
  end

  @doc """
  We may have open connections which nsqlookupd stops reporting. This function
  tells us which connections we have stored in state but not in nsqlookupd.
  Not for external use.
  """
  @spec dead_connections([host_with_port], pid, cons_state) :: [connection]
  def dead_connections(discovered_nsqds, cons, cons_state) do
    Enum.reject get_connections(cons_state), fn(conn) ->
      conn_already_discovered?(cons, conn, discovered_nsqds)
    end
  end

  @doc """
  When nsqlookupd reports available producers, there are some that may not
  already be in our connection list. This function reports which ones are new
  so we can connect to them.
  """
  @spec new_nsqds([host_with_port], pid, cons_state) :: [host_with_port]
  def new_nsqds(discovered_nsqds, cons, cons_state) do
    Enum.reject discovered_nsqds, fn(nsqd) ->
      nsqd_already_has_connection?(nsqd, cons, cons_state)
    end
  end

  @doc """
  Initialized from NSQ.ConsumerSupervisor, sends the consumer a message on a
  fixed interval.
  """
  @spec rdy_loop(pid) :: any
  def rdy_loop(cons) do
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
  @spec rdy_loop(pid) :: any
  def discovery_loop(cons) do
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

  @doc """
  Called from tests to assert correct consumer state. Not for external use.
  """
  @spec get_state(pid) :: {:ok, cons_state}
  def get_state(cons) do
    GenServer.call(cons, :state)
  end

  @doc """
  Called from `handle_call/3` when we need to decide what to do about the
  current backoff state. Not for external use.
  """
  @spec start_stop_continue_backoff(pid, atom, cons_state) :: {:ok, cons_state}
  def start_stop_continue_backoff(cons, backoff_signal, cons_state) do
    {backoff_updated, backoff_counter} = cond do
      backoff_signal == :resume ->
        {true, cons_state.backoff_counter - 1}
      backoff_signal == :backoff ->
        {true, cons_state.backoff_counter + 1}
      true ->
        {false, cons_state.backoff_counter}
    end
    cons_state = %{cons_state | backoff_counter: backoff_counter}

    cond do
      backoff_counter == 0 && backoff_updated ->
        count = per_conn_max_in_flight(cons_state)
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

  @doc """
  Returns the backoff duration in milliseconds. Different strategies can
  technically be used, but currently there is only `:exponential` in production
  mode and `:test` for tests. Not for external use.
  """
  @spec calculate_backoff(cons_state) :: integer
  def calculate_backoff(cons_state) do
    case cons_state.config.backoff_strategy do
      :exponential -> exponential_backoff(cons_state)
      :test -> 200
    end
  end

  @doc """
  Used to calculate backoff in milliseconds in production. We include jitter so
  that, if we have many consumers in a cluster, we avoid the thundering herd
  problem when they attempt to resume. Not for external use.
  """
  @spec exponential_backoff(cons_state) :: integer
  def exponential_backoff(cons_state) do
    attempts = cons_state.backoff_counter
    mult = cons_state.config.backoff_multiplier
    min(
      mult * :math.pow(2, attempts),
      cons_state.config.max_backoff_duration
    ) |> round
  end

  @doc """
  Try resuming from backoff in a few seconds. Not for external use.
  """
  @spec resume_from_backoff_later(pid, integer, cons_state) ::
    {:ok, cons_state}
  def resume_from_backoff_later(cons, duration, cons_state) do
    Task.start_link fn ->
      :timer.sleep(duration)
      GenServer.cast(cons, :resume)
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
  @spec resume(pid, cons_state) :: {:ok, cons_state}
  def resume(cons, cons_state) do
    if cons_state.backoff_duration == 0 || cons_state.backoff_counter == 0 do
      # looks like we successfully left backoff mode already
      {:ok, cons_state}
    else
      if cons_state.stop_flag do
        {:ok, %{cons_state | backoff_duration: 0}}
      else
        conn_count = count_connections(cons_state)

        if conn_count == 0 do
          # This could happen if nsqlookupd suddenly stops discovering
          # connections. Maybe a network partition?
          Logger.warn("no connection available to resume")
          Logger.warn("backing off for 1 second")
          {:ok, cons_state} = resume_from_backoff_later(cons, 1000, cons_state)
        else
          # pick a random connection to test the waters
          conn = random_connection_for_backoff(cons_state)
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
  some connections to 0 and 1 so that they're all guaranteed to eventually
  process messages. Not for external use.
  """
  @spec redistribute_rdy(pid, cons_state) :: {:ok, cons_state}
  def redistribute_rdy(cons, cons_state) do
    if should_redistribute_rdy?(cons_state) do
      conns = get_connections(cons_state)
      conn_count = length(conns)

      if conn_count > cons_state.max_in_flight do
        Logger.debug """
          redistributing RDY state
          (#{conn_count} conns > #{cons_state.max_in_flight} max_in_flight)
        """
      end

      if cons_state.backoff_counter > 0 && conn_count > 1 do
        Logger.debug """
          redistributing RDY state (in backoff and #{conn_count} conns > 1)
        """
      end

      # Free up any connections that are RDY but not processing messages.
      give_up_rdy_for_idle_connections(cons, cons_state)

      # Determine how much RDY we can distribute. This needs to happen before
      # we give up RDY, or max_in_flight will end up equalling RDY.
      available_max_in_flight = get_available_max_in_flight(cons_state)

      # Distribute it!
      distribute_rdy_randomly(
        cons, conns, available_max_in_flight, cons_state
      )
    else
      # Nothing to do. This is the usual path!
      {:ok, cons_state}
    end
  end

  @doc """
  If we're not in backoff mode and we've hit a "trigger point" to update RDY,
  then go ahead and update RDY. Not for external use.
  """
  @spec maybe_update_rdy(pid, connection, cons_state) :: {:ok, cons_state}
  def maybe_update_rdy(cons, conn, cons_state) do
    if cons_state.backoff_counter > 0 || cons_state.backoff_duration > 0 do
      # In backoff mode, we only let `start_stop_continue_backoff/3` handle
      # this case.
      Logger.debug """
        (#{inspect conn}) skip sending RDY \
        in_backoff:#{cons_state.backoff_counter} || \
        in_backoff_timeout:#{cons_state.backoff_duration}
      """
      {:ok, cons_state}
    else
      [remain, last_rdy] = ConnInfo.fetch(
        cons_state, ConnInfo.conn_id(conn), [:rdy_count, :last_rdy]
      )
      desired_rdy = per_conn_max_in_flight(cons_state)

      if remain <= 1 || remain < (last_rdy / 4) || (desired_rdy > 0 && desired_rdy < remain) do
        Logger.debug """
          (#{inspect conn}) sending RDY #{desired_rdy} \
          (#{remain} remain from last RDY #{last_rdy})
        """
        {:ok, _cons_state} = update_rdy(cons, conn, desired_rdy, cons_state)
      else
        Logger.debug """
          (#{inspect conn}) skip sending RDY #{desired_rdy} \
          (#{remain} remain out of last RDY #{last_rdy})
        """
        {:ok, cons_state}
      end
    end
  end

  @doc """
  Try to update RDY for a given connection, taking configuration and the
  current state into account. Not for external use.
  """
  @spec update_rdy(pid, connection, integer, cons_state) :: {:ok, cons_state}
  def update_rdy(cons, conn, new_rdy, cons_state) do
    conn_info = ConnInfo.fetch(cons_state, ConnInfo.conn_id(conn))

    cancel_outstanding_rdy_retry(cons_state, conn)

    # Cap the given RDY based on the connection config.
    new_rdy = [new_rdy, conn_info.max_rdy] |> Enum.min |> round

    # Cap the given RDY based on how much we can actually assign. Unless it's
    # 0, in which case we'll be retrying.
    max_possible_rdy = calc_max_possible_rdy(cons_state, conn_info)
    if max_possible_rdy > 0 do
      new_rdy = [new_rdy, max_possible_rdy] |> Enum.min |> round
    end

    if max_possible_rdy <= 0 && new_rdy > 0 do
      if conn_info.rdy_count == 0 do
        # Schedule update_rdy(consumer, conn, new_rdy) for this connection again
        # in 5 seconds. This is to prevent eternal starvation.
        {:ok, cons_state} = retry_rdy(cons, conn, new_rdy, cons_state)
      end
      {:ok, cons_state}
    else
      {:ok, _cons_state} = send_rdy(conn, new_rdy, cons_state)
    end
  end

  @doc """
  Delay for a configured interval, then call update_rdy. Not for external use.
  """
  @spec retry_rdy(pid, connection, integer, cons_state) :: {:ok, cons_state}
  def retry_rdy(cons, conn, count, cons_state) do
    delay = cons_state.config.rdy_retry_delay
    Logger.debug("(#{inspect conn}) retry RDY in #{delay / 1000} seconds")

    {:ok, retry_pid} = Task.start_link fn ->
      :timer.sleep(delay)
      GenServer.call(cons, {:update_rdy, conn, count})
    end
    ConnInfo.update(cons_state, ConnInfo.conn_id(conn), %{retry_rdy_pid: retry_pid})

    {:ok, cons_state}
  end

  @doc """
  Send a RDY command for the given connection.
  """
  @spec send_rdy(connection, integer, cons_state) :: {:ok, cons_state}
  def send_rdy({_id, pid} = conn, count, cons_state) do
    [last_rdy] = ConnInfo.fetch(cons_state, ConnInfo.conn_id(conn), [:last_rdy])

    if count == 0 && last_rdy == 0 do
      {:ok, cons_state}
    else
      # We intentionally don't match this GenServer.call. If the socket isn't
      # set up or is erroring out, we don't want to propagate that connection
      # error to the consumer.
      NSQ.Connection.cmd_noresponse(pid, {:rdy, count})
      {:ok, cons_state}
    end
  end

  @doc """
  Returns how much `max_in_flight` should be distributed to each connection.
  If `max_in_flight` is less than the number of connections, then this always
  returns 1 and they are randomly distributed via `redistribute_rdy`. Not for
  external use.
  """
  @spec per_conn_max_in_flight(cons_state) :: integer
  def per_conn_max_in_flight(cons_state) do
    max_in_flight = cons_state.max_in_flight
    conn_count = count_connections(cons_state)
    min(max(1, max_in_flight / conn_count), max_in_flight) |> round
  end

  @doc """
  Each connection is responsible for maintaining its own rdy_count in ConnInfo.
  This function sums all the values of rdy_count for each connection, which
  lets us get an accurate picture of a consumer's total RDY count. Not for
  external use.
  """
  @spec total_rdy_count(pid) :: integer
  def total_rdy_count(agent_pid) when is_pid(agent_pid) do
    ConnInfo.reduce agent_pid, 0, fn({_, conn_info}, acc) ->
      acc + conn_info.rdy_count
    end
  end

  @doc """
  Convenience function; uses the consumer state to get the conn info pid. Not
  for external use.
  """
  @spec total_rdy_count(cons_state) :: integer
  def total_rdy_count(%{conn_info_pid: agent_pid} = _cons_state) do
    total_rdy_count(agent_pid)
  end

  @doc """
  Public function to change `max_in_flight` for a consumer. The new value will
  be balanced across connections.
  """
  @spec change_max_in_flight(pid, integer) :: {:ok, :ok}
  def change_max_in_flight(sup_pid, new_max_in_flight) do
    cons = get(sup_pid)
    GenServer.call(cons, {:max_in_flight, new_max_in_flight})
  end

  @doc """
  Frequently, when testing, we publish a message then immediately want a
  consumer to process it, but this doesn't work if the consumer doesn't
  discover the nsqd first. Only meant for testing.
  """
  @spec discover_nsqds(pid) :: :ok
  def discover_nsqds(sup_pid) do
    cons = get(sup_pid)
    GenServer.call(cons, :discover_nsqds)
    :ok
  end

  @doc """
  Iterate over all listed connections and delete the ones that are dead. This
  exists because it is difficult to reliably clean up a connection immediately
  after it is terminated (it might still be running). This function runs in the
  discovery loop to provide consistency.
  """
  @spec delete_dead_connections(cons_state) :: {:ok, cons_state}
  def delete_dead_connections(state) do
    Enum.map get_connections(state), fn({conn_id, pid}) ->
      unless Process.alive?(pid) do
        Supervisor.delete_child(state.conn_sup_pid, conn_id)
      end
    end
    {:ok, state}
  end

  @doc """
  NSQ.Consumer.new actually returns the supervisor pid so that we can
  effectively recover from consumer crashes. This function takes the supervisor
  pid and returns the consumer pid. We use this for public facing functions so
  that the end user can simply target the supervisor, e.g.
  `NSQ.Consumer.change_max_in_flight(supervisor_pid, 100)`. Not for external
  use.
  """
  @spec get(pid) :: pid
  def get(sup_pid) do
    children = Supervisor.which_children(sup_pid)
    child = Enum.find(children, fn({kind, _, _, _}) -> kind == NSQ.Consumer end)
    {_, pid, _, _} = child
    pid
  end

  # ------------------------------------------------------- #
  # Private Functions                                       #
  # ------------------------------------------------------- #
  @spec now() :: integer
  defp now do
    :calendar.datetime_to_gregorian_seconds(:calendar.universal_time)
  end

  epoch = {{1970, 1, 1}, {0, 0, 0}}
  @epoch :calendar.datetime_to_gregorian_seconds(epoch)
  @spec datetime_from_timestamp(tuple) :: integer
  defp datetime_from_timestamp(timestamp) do
    timestamp
    |> +(@epoch)
    |> :calendar.gregorian_seconds_to_datetime
  end

  @spec count_connections(cons_state) :: integer
  defp count_connections(cons_state) do
    %{active: active} = Supervisor.count_children(cons_state.conn_sup_pid)
    active
  end

  @spec conn_already_discovered?(pid, connection, [host_with_port]) :: boolean
  defp conn_already_discovered?(cons, {conn_id, _}, discovered_nsqds) do
    Enum.any? discovered_nsqds, fn(nsqd) ->
      ConnInfo.conn_id(cons, nsqd) == conn_id
    end
  end

  @spec nsqd_already_has_connection?(host_with_port, pid, cons_state) :: boolean
  defp nsqd_already_has_connection?(nsqd, cons, cons_state) do
    needle = ConnInfo.conn_id(cons, nsqd)
    Enum.any? get_connections(cons_state), fn({conn_id, _}) ->
      conn_id == needle
    end
  end

  # Helper for redistribute_rdy; we set RDY to 1 for _some_ connections that
  # were halted, randomly, until there's no more RDY left to assign.
  @spec distribute_rdy_randomly(pid, [connection], integer, cons_state) ::
    {:ok, cons_state}
  defp distribute_rdy_randomly(cons, possible_conns, available_max_in_flight, cons_state) do
    if length(possible_conns) == 0 || available_max_in_flight <= 0 do
      {:ok, cons_state}
    else
      [conn|rest] = Enum.shuffle(possible_conns)
      Logger.debug("(#{inspect conn}) redistributing RDY")
      {:ok, cons_state} = update_rdy(cons, conn, 1, cons_state)
      distribute_rdy_randomly(
        cons, rest, available_max_in_flight - 1, cons_state
      )
    end
  end

  @spec should_redistribute_rdy?(cons_state) :: boolean
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

  @spec conn_from_nsqd(pid, host_with_port, cons_state) :: connection
  defp conn_from_nsqd(cons, nsqd, cons_state) do
    needle = ConnInfo.conn_id(cons, nsqd)
    Enum.find get_connections(cons_state), fn({conn_id, _}) ->
      needle == conn_id
    end
  end

  @spec random_connection_for_backoff(cons_state) :: connection
  defp random_connection_for_backoff(cons_state) do
    if cons_state.config.backoff_strategy == :test do
      # When testing, we're only sending 1 message at a time to a single
      # nsqd. In this mode, instead of a random connection, always use the
      # first one that was defined, which ends up being the last one in our
      # list.
      cons_state |> get_connections |> List.last
    else
      cons_state |> get_connections |> Enum.random
    end
  end

  @spec give_up_rdy_for_idle_connections(pid, cons_state) :: [connection]
  defp give_up_rdy_for_idle_connections(cons, cons_state) do
    conns = get_connections(cons_state)
    Enum.map conns, fn(conn) ->
      conn_id = ConnInfo.conn_id(conn)
      [last_msg_t, rdy_count] = ConnInfo.fetch(
        cons_state, conn_id, [:last_msg_timestamp, :rdy_count]
      )
      sec_since_last_msg = now - last_msg_t
      ms_since_last_msg = sec_since_last_msg * 1000

      Logger.debug(
        "(#{inspect conn}) rdy: #{rdy_count} (last message received \
        #{sec_since_last_msg} seconds ago, \
        #{inspect datetime_from_timestamp(last_msg_t)})"
      )

      is_idle = ms_since_last_msg > cons_state.config.low_rdy_idle_timeout
      if rdy_count > 0 && is_idle do
        Logger.debug("(#{inspect conn}) idle connection, giving up RDY")
        {:ok, _cons_state} = update_rdy(cons, conn, 0, cons_state)
      end

      conn
    end
  end

  # Cap available max in flight based on current RDY/backoff status.
  defp get_available_max_in_flight(cons_state) do
    total_rdy = total_rdy_count(cons_state)
    if cons_state.backoff_counter > 0 do
      # In backoff mode, we only ever want RDY=1 for the whole consumer. This
      # makes sure that available is only 1 if total_rdy is 0.
      1 - total_rdy
    else
      cons_state.max_in_flight - total_rdy
    end
  end

  @spec cancel_outstanding_rdy_retry(cons_state, connection) :: any
  defp cancel_outstanding_rdy_retry(cons_state, conn) do
    conn_info = ConnInfo.fetch(cons_state, ConnInfo.conn_id(conn))

    # If this is for a connection that's retrying, kill the timer and clean up.
    if retry_pid = conn_info.retry_rdy_pid do
      if Process.alive?(retry_pid) do
        Logger.debug("(#{inspect conn}) rdy retry pid #{inspect retry_pid} detected, killing")
        Process.exit(retry_pid, :normal)
      end

      ConnInfo.update(cons_state, ConnInfo.conn_id(conn), %{retry_rdy_pid: nil})
    end
  end

  @spec calc_max_possible_rdy(cons_state, map) :: integer
  defp calc_max_possible_rdy(cons_state, conn_info) do
    rdy_count = conn_info.rdy_count
    max_in_flight = cons_state.max_in_flight
    total_rdy = total_rdy_count(cons_state)
    max_in_flight - total_rdy + rdy_count
  end

end
