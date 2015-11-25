defmodule NSQ.ProducerSupervisor do
  use Supervisor

  def start_link(config, topic, opts \\ []) do
    Supervisor.start_link(__MODULE__, {config, topic}, opts)
  end

  def init({config, topic}) do
    guid = generate_guid
    producer_name = String.to_atom("producer_#{guid}")
    conn_sup_name = String.to_atom("#{producer_name}_conn_sup")
    children = [
      supervisor(NSQ.ConnectionSupervisor, [[name: conn_sup_name]]),
      worker(NSQ.Producer, [conn_sup_name, config, topic])
    ]
    supervise(children, strategy: :rest_for_one)
  end

  def generate_guid do
    :crypto.strong_rand_bytes(4) |> :base64.encode_to_string |> to_string
  end
end
