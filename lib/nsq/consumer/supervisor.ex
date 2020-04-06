defmodule NSQ.Consumer.Supervisor do
  use Supervisor

  def start_link(topic, channel, config, opts \\ []) do
    Supervisor.start_link(__MODULE__, {topic, channel, config}, opts)
  end

  @impl true
  def init({topic, channel, config}) do
    consumer_name = String.to_atom("nsq_consumer_#{UUID.uuid4(:hex)}")
    discovery_loop_id = String.to_atom("#{consumer_name}_discovery_loop")
    rdy_loop_id = String.to_atom("#{consumer_name}_rdy_loop")

    children = [
      {NSQ.Consumer, {topic, channel, consumer_name, config}},
      %{
        :id => discovery_loop_id,
        :start =>
          {Task, :start_link, [NSQ.Consumer.Connections, :discovery_loop, [consumer_name]]}
      },
      %{
        :id => rdy_loop_id,
        :start => {Task, :start_link, [NSQ.Consumer.RDY, :redistribute_loop, [consumer_name]]}
      }
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
