defmodule NSQ.Message.Supervisor do
  @moduledoc """
  """

  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  use Supervisor

  # ------------------------------------------------------- #
  # Behaviour Implementation                                #
  # ------------------------------------------------------- #
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, opts)
  end

  def start_child(msg_sup_pid, message, opts \\ []) do
    # If a message fails, NSQ will handle requeueing.
    id = message.id <> "-" <> UUID.uuid4(:hex)
    opts = [id: id, restart: :temporary] ++ opts
    child = Supervisor.child_spec({NSQ.Message, message}, opts)
    Supervisor.start_child(msg_sup_pid, child)
  end

  @impl true
  def init(:ok) do
    Supervisor.init([], strategy: :one_for_one)
  end
end
