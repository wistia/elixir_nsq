defmodule NSQ.Consumer.Supervisor do
  use Supervisor

  def start_link(topic, channel, config, opts \\ []) do
    Supervisor.start_link(__MODULE__, {topic, channel, config}, opts)
  end

  def init({topic, channel, config}) do
    consumer_name = String.to_atom("nsq_consumer_#{UUID.uuid4(:hex)}")
    discovery_loop_id = String.to_atom("#{consumer_name}_discovery_loop")
    rdy_loop_id = String.to_atom("#{consumer_name}_rdy_loop")

    children = [
      worker(NSQ.Consumer, [topic, channel, config, [name: consumer_name]]),
      worker(Task, [NSQ.Consumer.Connections, :discovery_loop, [consumer_name]],
        id: discovery_loop_id
      ),
      worker(Task, [NSQ.Consumer.RDY, :redistribute_loop, [consumer_name]], id: rdy_loop_id)
    ]

    supervise(children, strategy: :rest_for_one)
  end
end
