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
  `NSQ.ConsumerSupervisor.start_link/3`. It takes a topic, a channel, and an
  NSQ.Config struct, which has possible values defined and explained in
  nsq/config.ex.

      {:ok, consumer} = NSQ.ConsumerSupervisor.start_link("my-topic", "my-channel", %NSQ.Config{
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
  import NSQ.Consumer.Helpers
  alias NSQ.Consumer.Backoff
  alias NSQ.Consumer.RDY
  alias NSQ.ConnInfo, as: ConnInfo

  # ------------------------------------------------------- #
  # Module Attributes                                       #
  # ------------------------------------------------------- #
  @initial_state %{
    channel: nil,
    config: %NSQ.Config{},
    conn_sup_pid: nil,
    conn_info_pid: nil,
    event_manager_pid: nil,
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
  NSQ.Connection.Supervisor) and a PID of the connection.
  """
  @type connection :: {String.t, pid}

  @typedoc """
  A map, but we can be more specific by asserting some entries that should be
  set for a connection's state map.
  """
  @type cons_state :: %{conn_sup_pid: pid, config: NSQ.Config.t, conn_info_pid: pid}
  @type state :: %{conn_sup_pid: pid, config: NSQ.Config.t, conn_info_pid: pid}

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
    {:ok, conn_sup_pid} = NSQ.Connection.Supervisor.start_link
    cons_state = %{cons_state | conn_sup_pid: conn_sup_pid}

    {:ok, conn_info_pid} = Agent.start_link(fn -> %{} end)
    cons_state = %{cons_state | conn_info_pid: conn_info_pid}

    manager =
      if cons_state.config.event_manager do
        cons_state.config.event_manager
      else
        {:ok, manager} = GenEvent.start_link
        manager
      end
    cons_state = %{cons_state | event_manager_pid: manager}

    cons_state = %{cons_state | max_in_flight: cons_state.config.max_in_flight}

    {:ok, _cons_state} = discover_nsqds_and_connect(self, cons_state)
  end

  @doc """
  The RDY loop periodically calls this to make sure RDY is balanced among our
  connections.
  """
  @spec handle_call(:redistribute_rdy, {reference, pid}, cons_state) ::
    {:reply, :ok, cons_state}
  def handle_call(:redistribute_rdy, _from, cons_state) do
    {:ok, cons_state} = RDY.redistribute(self, cons_state)
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
    {:ok, cons_state} = reconnect_failed_connections(cons_state)
    {:ok, cons_state} = discover_nsqds_and_connect(self, cons_state)
    {:reply, :ok, cons_state}
  end

  @doc """
  Only used for specs.
  """
  @spec handle_call(:delete_dead_connections, {reference, pid}, cons_state) ::
    {:reply, :ok, cons_state}
  def handle_call(:delete_dead_connections, _from, cons_state) do
    {:ok, cons_state} = delete_dead_connections(cons_state)
    {:reply, :ok, cons_state}
  end

  @doc """
  Called from `NSQ.Message.fin/1`. Not for external use.
  """
  @spec handle_call({:start_stop_continue_backoff, atom}, {reference, pid}, cons_state) ::
    {:reply, :ok, cons_state}
  def handle_call({:start_stop_continue_backoff, backoff_flag}, _from, cons_state) do
    {:ok, cons_state} = Backoff.start_stop_continue(self, backoff_flag, cons_state)
    {:reply, :ok, cons_state}
  end

  @doc """
  Called from `retry_rdy/4`. Not for external use.
  """
  @spec handle_call({:update_rdy, connection, integer}, {reference, pid}, cons_state) ::
    {:reply, :ok, cons_state}
  def handle_call({:update_rdy, conn, count}, _from, cons_state) do
    {:ok, cons_state} = RDY.update(self, conn, count, cons_state)
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

  def handle_call(:close, _, cons_state) do
    Logger.info "Closing consumer #{inspect self}"
    connections = get_connections(cons_state)
    Task.start_link fn ->
      Enum.map connections, fn({_, conn_pid}) ->
        Task.start_link(NSQ.Connection, :close, [conn_pid])
      end
    end
    {:reply, :ok, %{cons_state | stop_flag: true}}
  end

  @doc """
  Called from NSQ.Consume.event_manager.
  """
  @spec handle_call(:event_manager, any, cons_state) ::
    {:reply, pid, cons_state}
  def handle_call(:event_manager, _from, state) do
    {:reply, state.event_manager_pid, state}
  end

  @doc """
  Called to observe all connection stats. For debugging or reporting purposes.
  """
  @spec handle_call(:conn_info, any, cons_state) :: {:reply, map, cons_state}
  def handle_call(:conn_info, _from, state) do
    info = ConnInfo.all(state.conn_info_pid)
    {:reply, info, state}
  end

  @doc """
  Called from `Backoff.resume_later/3`. Not for external use.
  """
  @spec handle_cast(:resume, cons_state) :: {:noreply, cons_state}
  def handle_cast(:resume, state) do
    {:ok, cons_state} = Backoff.resume(self, state)
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
    {:ok, cons_state} = RDY.maybe_update(self, conn, cons_state)
    {:noreply, cons_state}
  end

  # ------------------------------------------------------- #
  # API Definitions                                         #
  # ------------------------------------------------------- #
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
        cons_state.config.nsqlookupds
        |> NSQ.Lookupd.nsqds_with_topic(cons_state.topic)

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
    if length(nsqds) > 0 do
      Logger.info "Connecting to nsqds #{inspect nsqds}"
    end
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
    Process.flag(:trap_exit, true)
    try do
      {:ok, _pid} = NSQ.Connection.Supervisor.start_child(
        cons, nsqd, cons_state
      )

      # We normally set RDY to 1, but if we're spawning more connections than
      # max_in_flight, we don't want to break our contract. In that case, the
      # `RDY.redistribute` loop will take care of getting this connection some
      # messages later.
      remaining_rdy = cons_state.max_in_flight - total_rdy_count(cons_state)
      if remaining_rdy > 0 do
        conn = conn_from_nsqd(cons, nsqd, cons_state)
        {:ok, cons_state} = RDY.transmit(conn, 1, cons_state)
      end

      {:ok, cons_state}
    catch
      :error, _ ->
        Logger.error "#{inspect cons}: Error connecting to #{inspect nsqd}"
        conn_id = ConnInfo.conn_id(cons, nsqd)
        ConnInfo.delete(cons_state, conn_id)
        {:ok, cons_state}
    after
      Process.flag(:trap_exit, false)
    end
  end

  @doc """
  Given a list of connections, force them to stop. Return the new state without
  those connections.
  """
  @spec stop_connections([connection], pid, cons_state) :: {:ok, cons_state}
  def stop_connections(dead_conns, cons, cons_state) do
    if length(dead_conns) > 0 do
      Logger.info "Stopping connections #{inspect dead_conns}"
    end

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

  def close(sup_pid) do
    cons = get(sup_pid)
    GenServer.call(cons, :close)
  end

  @doc """
  Called from tests to assert correct consumer state. Not for external use.
  """
  @spec get_state(pid) :: {:ok, cons_state}
  def get_state(cons) do
    GenServer.call(cons, :state)
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
  If the event manager is not defined in NSQ.Config, it will be generated. So
  if you want to attach event handlers on the fly, you can use a syntax like
  `NSQ.Consumer.event_manager(consumer) |> GenEvent.add_handler(MyHandler, [])`
  """
  def event_manager(sup_pid) do
    cons = get(sup_pid)
    GenServer.call(cons, :event_manager)
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

  def reconnect_failed_connections(state) do
    Enum.map get_connections(state), fn({_, pid}) ->
      if Process.alive?(pid), do: GenServer.cast(pid, :reconnect)
    end
    {:ok, state}
  end

  def conn_info(sup_pid) do
    cons = get(sup_pid)
    GenServer.call(cons, :conn_info)
  end

  @doc """
  NSQ.ConsumerSupervisor.start_link returns the supervisor pid so that we can
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

  @spec count_connections(cons_state) :: integer
  def count_connections(cons_state) do
    %{active: active} = Supervisor.count_children(cons_state.conn_sup_pid)
    if is_integer(active) do
      active
    else
      Logger.warn "(#{inspect self}) non-integer #{inspect active} returned counting connections, returning 0 instead"
      0
    end
  end

  # ------------------------------------------------------- #
  # Private Functions                                       #
  # ------------------------------------------------------- #
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

  @spec conn_from_nsqd(pid, host_with_port, cons_state) :: connection
  defp conn_from_nsqd(cons, nsqd, cons_state) do
    needle = ConnInfo.conn_id(cons, nsqd)
    Enum.find get_connections(cons_state), fn({conn_id, _}) ->
      needle == conn_id
    end
  end
end
