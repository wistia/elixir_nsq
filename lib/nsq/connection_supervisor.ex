defmodule NSQ.ConnectionSupervisor do
  @moduledoc """
  A consumer or producer will initialize this supervisor empty to start.
  Candidate nsqd connections will be discovered elsewhere and added with
  start_child.
  """

  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, opts)
  end

  # ------------------------------------------------------- #
  # Behaviour Implementation                                #
  # ------------------------------------------------------- #
  def start_child(parent, nsqd, parent_state \\ nil, opts \\ []) do
    parent_state = parent_state || GenServer.call(parent, :state)
    conn_sup_pid = parent_state.conn_sup_pid
    args = [
      parent,
      nsqd,
      parent_state.config,
      parent_state.topic,
      parent_state.channel
    ]
    conn_id = NSQ.Connection.connection_id(parent, nsqd)
    opts = [restart: :temporary, id: conn_id] ++ opts
    child = worker(NSQ.Connection, args, opts)
    Supervisor.start_child(conn_sup_pid, child)
  end

  def init(:ok) do
    supervise([], strategy: :one_for_one)
  end
end
