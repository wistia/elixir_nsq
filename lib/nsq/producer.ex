defmodule NSQ.Producer do
  @moduledoc """
  A producer is a process that connects to one or many NSQDs and publishes
  messages.

  ## Interface

  To initialize a producer, use `new/2`.

      {:ok, producer} = NSQ.Producer.new("the-default-topic", %NSQ.Config{
        nsqds: ["127.0.0.1:6750"]
      })

  The default `topic` argument is _required_, even if you plan on explicitly
  publishing to a different topic. If you don't plan on using, you can set it
  to something like `_default_topic_`.

  If you provide more than one nsqd, each `pub`/`mpub` will choose one
  randomly.

  Note that, unlike consumers, producers _cannot_ be configured to use
  discovery with nsqlookupd. This is because discovery requires a topic and
  channel, and an nsqd will only appear in nsqlookupd if it has already
  published messages on that topic. So there's a chicken-and-egg problem. The
  recommended solution is to run NSQD on the same box where you're publishing,
  so your address is always 127.0.0.1 with a static port.

  ### pub

  Publish a single message to NSQD.

      NSQ.Producer.pub(producer, "a message")
      NSQ.Producer.pub(producer, "different-topic", "a message")

  ### mpub

  Publish a bunch of messages to NSQD atomically.

      NSQ.Producer.mpub(producer, ["one", "two"])
      NSQ.Producer.mpub(producer, "different-topic", ["one", "two"])
  """

  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  require Logger
  import NSQ.Protocol
  use GenServer

  # ------------------------------------------------------- #
  # Module Attributes                                       #
  # ------------------------------------------------------- #
  @initial_state %{
    topic: nil,
    channel: nil,
    conn_sup_pid: nil,
    config: nil,
    conn_info_pid: nil
  }

  # ------------------------------------------------------- #
  # Behaviour Implementation                                #
  # ------------------------------------------------------- #
  def init(pro_state) do
    {:ok, conn_sup_pid} = NSQ.ConnectionSupervisor.start_link
    pro_state = %{pro_state | conn_sup_pid: conn_sup_pid}

    {:ok, conn_info_pid} = Agent.start_link(fn -> %{} end)
    pro_state = %{pro_state | conn_info_pid: conn_info_pid}

    {:ok, _pro_state} = connect_to_nsqds(pro_state.config.nsqds, self, pro_state)
  end

  def handle_call({:pub, data}, _from, pro_state) do
    do_pub(pro_state.topic, data, pro_state)
  end

  def handle_call({:pub, topic, data}, _from, pro_state) do
    do_pub(topic, data, pro_state)
  end

  def handle_call({:mpub, data}, _from, pro_state) do
    do_mpub(pro_state.topic, data, pro_state)
  end

  def handle_call({:mpub, topic, data}, _from, pro_state) do
    do_mpub(topic, data, pro_state)
  end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  # ------------------------------------------------------- #
  # API Definitions                                         #
  # ------------------------------------------------------- #
  def new(topic, config) do
    NSQ.ProducerSupervisor.start_link(topic, config)
  end

  def start_link(topic, config) do
    {:ok, config} = NSQ.Config.validate(config || %NSQ.Config{})
    {:ok, config} = NSQ.Config.normalize(config)
    unless is_valid_topic_name?(topic), do: raise "Invalid topic name #{topic}"
    state = %{@initial_state | topic: topic, config: config}
    GenServer.start_link(__MODULE__, state)
  end

  def get_connections(pro_state) when is_map(pro_state) do
    children = Supervisor.which_children(pro_state.conn_sup_pid)
    Enum.map children, fn({child_id, pid, _, _}) -> {child_id, pid} end
  end

  def get_connections(pro, pro_state \\ nil) when is_pid(pro) do
    pro_state = pro_state || get_state(pro)
    Supervisor.which_children(pro_state.conn_sup_pid)
  end

  def random_connection_pid(pro_state) do
    {_child_id, pid} = Enum.random(get_connections(pro_state))
    pid
  end

  @doc """
  Create supervised connections to NSQD.
  """
  def connect_to_nsqds(nsqds, pro, pro_state) do
    Enum.map nsqds, fn(nsqd) ->
      {:ok, _conn} = NSQ.ConnectionSupervisor.start_child(
        pro, nsqd, pro_state, [restart: :permanent]
      )
    end
    {:ok, pro_state}
  end

  @doc """
  Get the current state of a producer. Used in tests. Not for external use.
  """
  def get_state(producer) do
    GenServer.call(producer, :state)
  end

  @doc """
  Publish data to whatever topic is the default.
  """
  def pub(sup_pid, data) do
    {:ok, _resp} = GenServer.call(get(sup_pid), {:pub, data})
  end

  @doc """
  Publish data to a specific topic.
  """
  def pub(sup_pid, topic, data) do
    {:ok, _resp} = GenServer.call(get(sup_pid), {:pub, topic, data})
  end

  @doc """
  Publish data to whatever topic is the default.
  """
  def mpub(sup_pid, data) do
    GenServer.call(get(sup_pid), {:mpub, data})
  end

  @doc """
  Publish data to a specific topic.
  """
  def mpub(sup_pid, topic, data) do
    {:ok, _resp} = GenServer.call(get(sup_pid), {:mpub, topic, data})
  end

  # The end-user will be targeting the supervisor, but it's the producer that
  # can actually handle the command.
  def get(sup_pid) do
    children = Supervisor.which_children(sup_pid)
    child = Enum.find(children, fn({kind, _, _, _}) -> kind == NSQ.Producer end)
    {_, pid, _, _} = child
    pid
  end

  # Used to DRY up handle_call({:pub, ...).
  defp do_pub(topic, data, pro_state) do
    conn_pid = random_connection_pid(pro_state)
    {:ok, resp} = NSQ.Connection.cmd(conn_pid, {:pub, topic, data})
    {:reply, {:ok, resp}, pro_state}
  end

  # Used to DRY up handle_call({:mpub, ...).
  defp do_mpub(topic, data, pro_state) do
    conn_pid = random_connection_pid(pro_state)
    {:ok, resp} = NSQ.Connection.cmd(conn_pid, {:mpub, topic, data})
    {:reply, {:ok, resp}, pro_state}
  end
end
