defmodule NSQ.Producer do
  use GenServer
  require Logger
  import NSQ.Protocol


  @initial_state %{
    connections: [],
    topic: nil,
    config: %NSQ.Config{}
  }


  def start_link(config, topic) do
    {:ok, config} = NSQ.Config.validate(config || %NSQ.Config{})
    unless is_valid_topic_name?(topic), do: raise "Invalid topic name #{topic}"
    state = %{@initial_state | topic: topic, config: config}
    GenServer.start_link(__MODULE__, state)
  end


  def init(pro_state) do
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


  def random_connection_pid(pro, pro_state \\ nil) do
    pro_state = pro_state || NSQ.Producer.get_state(pro)
    {_nsqd, {pid, _ref}} = Enum.shuffle(pro_state.connections) |> List.first
    pid
  end


  def connect_to_nsqds(nsqds, pro, pro_state \\ nil) do
    pro_state = pro_state || NSQ.Producer.get_state(pro)

    new_conns = Enum.map nsqds, fn(nsqd) ->
      {:ok, conn} = NSQ.Connection.start_monitor(
        pro, nsqd, pro_state.config, pro_state.topic
      )
      {nsqd, conn}
    end
    {:ok, %{pro_state | connections: pro_state.connections ++ new_conns}}
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
