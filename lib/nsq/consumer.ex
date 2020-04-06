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
  `NSQ.Consumer.Supervisor.start_link/3`. It takes a topic, a channel, and an
  NSQ.Config struct, which has possible values defined and explained in
  nsq/config.ex.

      {:ok, consumer} = NSQ.Consumer.Supervisor.start_link("my-topic", "my-channel", %NSQ.Config{
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
  alias NSQ.Consumer.Connections
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
    backoff_duration: 0,
    distribution_counter: 0
  }

  # ------------------------------------------------------- #
  # Type Definitions                                        #
  # ------------------------------------------------------- #
  @typedoc """
  A tuple with a host and a port.
  """
  @type host_with_port :: {String.t(), integer}

  @typedoc """
  A tuple with a string ID (used to target the connection in
  NSQ.Connection.Supervisor) and a PID of the connection.
  """
  @type connection :: {String.t(), pid}

  @typedoc """
  A map, but we can be more specific by asserting some entries that should be
  set for a connection's state map.
  """
  @type cons_state :: %{conn_sup_pid: pid, config: NSQ.Config.t(), conn_info_pid: pid}
  @type state :: %{conn_sup_pid: pid, config: NSQ.Config.t(), conn_info_pid: pid}

  # ------------------------------------------------------- #
  # Behaviour Implementation                                #
  # ------------------------------------------------------- #
  @doc """
  Starts a Consumer process, called via the supervisor.
  """
  @spec start_link(String.t(), String.t(), NSQ.Config.t(), list) :: {:ok, pid}
  def start_link(topic, channel, config, opts \\ []) do
    {:ok, config} = NSQ.Config.validate(config)
    {:ok, config} = NSQ.Config.normalize(config)
    unless is_valid_topic_name?(topic), do: raise("Invalid topic name #{topic}")
    unless is_valid_channel_name?(channel), do: raise("Invalid channel name #{channel}")

    state = %{
      @initial_state
      | topic: topic,
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
    {:ok, conn_sup_pid} = NSQ.Connection.Supervisor.start_link()
    cons_state = %{cons_state | conn_sup_pid: conn_sup_pid}

    {:ok, conn_info_pid} = Agent.start_link(fn -> %{} end)
    cons_state = %{cons_state | conn_info_pid: conn_info_pid}

    manager =
      if cons_state.config.event_manager do
        cons_state.config.event_manager
      else
        {:ok, manager} = GenEvent.start_link()
        manager
      end

    cons_state = %{cons_state | event_manager_pid: manager}

    cons_state = %{cons_state | max_in_flight: cons_state.config.max_in_flight}

    {:ok, _cons_state} = Connections.discover_nsqds_and_connect(self(), cons_state)
  end

  @doc """
  The RDY loop periodically calls this to make sure RDY is balanced among our
  connections.
  """
  @spec handle_call(:redistribute_rdy, {reference, pid}, cons_state) ::
          {:reply, :ok, cons_state}
  def handle_call(:redistribute_rdy, _from, cons_state) do
    {:reply, :ok, RDY.redistribute!(self(), cons_state)}
  end

  @doc """
  The discovery loop calls this periodically to add/remove active nsqd
  connections. Called from Consumer.Supervisor.
  """
  @spec handle_call(:discover_nsqds, {reference, pid}, cons_state) ::
          {:reply, :ok, cons_state}
  def handle_call(:discover_nsqds, _from, cons_state) do
    {:reply, :ok, Connections.refresh!(cons_state)}
  end

  @doc """
  Only used for specs.
  """
  @spec handle_call(:delete_dead_connections, {reference, pid}, cons_state) ::
          {:reply, :ok, cons_state}
  def handle_call(:delete_dead_connections, _from, cons_state) do
    {:reply, :ok, Connections.delete_dead!(cons_state)}
  end

  @doc """
  Called from `NSQ.Message.fin/1`. Not for external use.
  """
  @spec handle_call({:start_stop_continue_backoff, atom}, {reference, pid}, cons_state) ::
          {:reply, :ok, cons_state}
  def handle_call({:start_stop_continue_backoff, backoff_flag}, _from, cons_state) do
    {:reply, :ok, Backoff.start_stop_continue!(self(), backoff_flag, cons_state)}
  end

  @spec handle_call({:update_rdy, connection, integer}, {reference, pid}, cons_state) ::
          {:reply, :ok, cons_state}
  def handle_call({:update_rdy, conn, count}, _from, cons_state) do
    {:reply, :ok, RDY.update!(self(), conn, count, cons_state)}
  end

  @doc """
  Called from tests to assert correct consumer state. Not for external use.
  """
  @spec handle_call(:state, {reference, pid}, cons_state) ::
          {:reply, cons_state, cons_state}
  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  def handle_call(:starved, _from, cons_state) do
    is_starved =
      ConnInfo.all(cons_state.conn_info_pid)
      |> Enum.any?(fn {_conn_id, info} ->
        info.messages_in_flight > 0 &&
          info.messages_in_flight >= info.last_rdy * 0.85
      end)

    {:reply, is_starved, cons_state}
  end

  @doc """
  Called from `NSQ.Consumer.change_max_in_flight(consumer, max_in_flight)`. Not
  for external use.
  """
  @spec handle_call({:max_in_flight, integer}, {reference, pid}, cons_state) ::
          {:reply, :ok, cons_state}
  def handle_call({:max_in_flight, new_max_in_flight}, _from, state) do
    {:reply, :ok, %{state | max_in_flight: new_max_in_flight}}
  end

  def handle_call(:close, _, cons_state) do
    {:reply, :ok, Connections.close!(cons_state)}
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
    {:reply, ConnInfo.all(state.conn_info_pid), state}
  end

  @doc """
  Called from `Backoff.resume_later/3`. Not for external use.
  """
  @spec handle_cast(:resume, cons_state) :: {:noreply, cons_state}
  def handle_cast(:resume, state) do
    {:noreply, Backoff.resume!(self(), state)}
  end

  @doc """
  Called from `NSQ.Connection.handle_cast({:nsq_msg, _}, _)` after each message
  is received. Not for external use.
  """
  @spec handle_cast({:maybe_update_rdy, host_with_port}, cons_state) ::
          {:noreply, cons_state}
  def handle_cast({:maybe_update_rdy, {_host, _port} = nsqd}, cons_state) do
    conn = conn_from_nsqd(self(), nsqd, cons_state)
    {:noreply, RDY.maybe_update!(self(), conn, cons_state)}
  end

  # ------------------------------------------------------- #
  # API Definitions                                         #
  # ------------------------------------------------------- #
  def starved?(sup_pid) do
    cons = get(sup_pid)
    GenServer.call(cons, :starved)
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

  def conn_info(sup_pid) do
    cons = get(sup_pid)
    GenServer.call(cons, :conn_info)
  end

  @doc """
  NSQ.Consumer.Supervisor.start_link returns the supervisor pid so that we can
  effectively recover from consumer crashes. This function takes the supervisor
  pid and returns the consumer pid. We use this for public facing functions so
  that the end user can simply target the supervisor, e.g.
  `NSQ.Consumer.change_max_in_flight(supervisor_pid, 100)`. Not for external
  use.
  """
  @spec get(pid) :: pid
  def get(sup_pid) do
    children = Supervisor.which_children(sup_pid)
    child = Enum.find(children, fn {kind, _, _, _} -> kind == NSQ.Consumer end)
    {_, pid, _, _} = child
    pid
  end
end
