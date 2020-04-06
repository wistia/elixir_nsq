defmodule NSQ.Consumer.Supervisor do
  use Supervisor

  def start_link(topic, channel, config, opts \\ []) do
    Supervisor.start_link(__MODULE__, {topic, channel, config}, opts)
  end

  @impl true
  def init({topic, channel, config}) do
    consumer_name = String.to_atom("nsq_consumer_#{UUID.uuid4(:hex)}")

    children = [
      {NSQ.Consumer, {topic, channel, consumer_name, config}},
      {NSQ.Consumer.ConnectionsTask, consumer_name},
      {NSQ.Consumer.RDYTask, consumer_name}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
