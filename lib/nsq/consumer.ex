defmodule NSQ.Consumer do
  defstruct ~w(
    topic
    channel
    config
    log_level
    max_in_flight
    incoming_messages
    rdy_retry_timers
    pending_connections
    connections
    connection
  )a

  def new(topic, channel, config \\ %{}) do
    consumer = %NSQ.Consumer{topic: topic, channel: channel, config: config}
    {:ok, conn} = NSQ.Consumer.Connection.start_link(consumer)
    %{consumer | connection: conn}
  end
end
