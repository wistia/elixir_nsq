defmodule NSQ.Producer do
  use GenServer
  require Logger
  import NSQ.Protocol


  @initial_state %{
    connection: nil,
    topic: nil,
    config: %NSQ.Config{}
  }


  def start_link(nsqds, topic, config \\ nil) do
    {:ok, config} = NSQ.Config.validate(config || %NSQ.Config{})
    unless is_valid_topic_name?(topic), do: raise "Invalid topic name #{topic}"
    state = %{@initial_state | topic: topic, config: config}
    GenServer.start_link(__MODULE__, state)
  end


  def init(pro_state) do
    nsqd = Enum.shuffle(pro_state.config.nsqds) |> List.first
    {:ok, from} = NSQ.Connection.start_monitor(
      self, nsqd, pro_state.config, pro_state.topic
    )
    Logger.debug "Producer connection.start_monitor"
    {:ok, %{pro_state | connection: {nsqd, from}}}
  end


  def pub(producer, data) do
    :ok = GenServer.call(producer, {:pub, data})
  end
end
