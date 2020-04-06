defmodule NSQ.ProducerSupervisor do
  use Supervisor

  def start_link(topic, config, opts \\ []) do
    Supervisor.start_link(__MODULE__, {topic, config}, opts)
  end

  def init({topic, config}) do
    children = [
      worker(NSQ.Producer, [topic, config])
    ]

    supervise(children, strategy: :one_for_one)
  end
end
