defmodule NSQ.ProducerSupervisor do
  use Supervisor

  def start_link(config, topic, opts \\ []) do
    Supervisor.start_link(__MODULE__, {config, topic}, opts)
  end

  def init({config, topic}) do
    children = [
      worker(NSQ.Producer, [config, topic])
    ]
    supervise(children, strategy: :one_for_one)
  end
end
