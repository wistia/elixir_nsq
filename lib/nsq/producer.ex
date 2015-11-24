defmodule NSQ.Producer do
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
    config: nil
  }

  # ------------------------------------------------------- #
  # Behaviour Implementation                                #
  # ------------------------------------------------------- #
  def init(pro_state) do
    {:ok, conn_sup_pid} = NSQ.ConnectionSupervisor.start_link
    pro_state = %{pro_state | conn_sup_pid: conn_sup_pid}
    {:ok, _pro_state} = connect_to_nsqds(pro_state.config.nsqds, self, pro_state)
  end

  def handle_call({:pub, data}, _from, pro_state) do
    conn_pid = random_connection_pid(self, pro_state)
    GenServer.call(conn_pid, {:pub, pro_state.topic, data})
    {:reply, :ok, pro_state}
  end

  def handle_call({:pub, topic, data}, _from, pro_state) do
    conn_pid = random_connection_pid(self, pro_state)
    GenServer.call(conn_pid, {:pub, topic, data})
    {:reply, :ok, pro_state}
  end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  # ------------------------------------------------------- #
  # API Definitions                                         #
  # ------------------------------------------------------- #
  def new(config, topic) do
    NSQ.ProducerSupervisor.start_link(config, topic)
  end


  def start_link(config, topic) do
    {:ok, config} = NSQ.Config.validate(config || %NSQ.Config{})
    unless is_valid_topic_name?(topic), do: raise "Invalid topic name #{topic}"
    state = %{@initial_state | topic: topic, config: config}
    GenServer.start_link(__MODULE__, state)
  end


  def connections(pro_state) when is_map(pro_state) do
    children = Supervisor.which_children(pro_state.conn_sup_pid)
    Enum.map children, fn({child_id, pid, _, _}) -> {child_id, pid} end
  end


  def connections(pro, pro_state \\ nil) when is_pid(pro) do
    pro_state = pro_state || get_state(pro)
    Supervisor.which_children(pro_state.conn_sup_pid)
  end


  def random_connection_pid(pro, pro_state \\ nil) do
    pro_state = pro_state || get_state(pro)
    {_child_id, pid} = Enum.shuffle(connections(pro_state)) |> List.first
    pid
  end


  def connect_to_nsqds(nsqds, pro, pro_state \\ nil) do
    pro_state = pro_state || get_state(pro)
    new_conns = Enum.map nsqds, fn(nsqd) ->
      {:ok, conn} = NSQ.ConnectionSupervisor.start_child(
        pro, nsqd, pro_state
      )
      {nsqd, conn}
    end
    {:ok, pro_state}
  end


  def get_state(producer) do
    GenServer.call(producer, :state)
  end


  def pub(producer, data) do
    :ok = GenServer.call(producer, {:pub, data})
  end


  def pub(producer, topic, data) do
    :ok = GenServer.call(producer, {:pub, topic, data})
  end
end
