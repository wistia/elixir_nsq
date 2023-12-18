defmodule NSQ.Producer.Supervisor do
  use Supervisor

  def start_link(topic, config, opts \\ []) do
    Supervisor.start_link(__MODULE__, {topic, config}, opts)
  end

  @impl true
  def init({topic, config}) do
    children = [{NSQ.Producer, {topic, config}}]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
