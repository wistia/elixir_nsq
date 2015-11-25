defmodule NSQ.ConsumerSupervisor do
  use Supervisor

  def start_link(topic, channel, config, opts \\ []) do
    Supervisor.start_link(__MODULE__, {topic, channel, config}, opts)
  end

  def init({topic, channel, config}) do
    guid = generate_guid
    consumer_name = String.to_atom("nsq_consumer_#{topic}_#{channel}_#{guid}")
    discovery_loop_id = String.to_atom("#{consumer_name}_discovery_loop")
    rdy_loop_id = String.to_atom("#{consumer_name}_rdy_loop")
    conn_sup_name = String.to_atom("#{consumer_name}_conn_sup")

    children = [
      supervisor(NSQ.ConnectionSupervisor, [[name: conn_sup_name]]),
      worker(NSQ.Consumer, [conn_sup_name, topic, channel, config, [name: consumer_name]]),
      worker(Task, [NSQ.Consumer, :discovery_loop, [consumer_name]], id: discovery_loop_id),
      worker(Task, [NSQ.Consumer, :rdy_loop, [consumer_name]], id: rdy_loop_id),
    ]

    supervise(children, strategy: :rest_for_one)
  end

  def generate_guid do
    :crypto.strong_rand_bytes(4) |> :base64.encode_to_string |> to_string
  end
end
