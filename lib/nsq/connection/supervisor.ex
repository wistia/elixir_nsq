defmodule NSQ.Connection.Supervisor do
  @moduledoc """
  A consumer or producer will initialize this supervisor empty to start.
  Candidate nsqd connections will be discovered elsewhere and added with
  start_child.
  """

  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  use Supervisor
  alias NSQ.ConnInfo, as: ConnInfo

  # ------------------------------------------------------- #
  # Behaviour Implementation                                #
  # ------------------------------------------------------- #
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, opts)
  end

  def start_child(parent, nsqd, parent_state \\ nil, opts \\ []) do
    parent_state = parent_state || GenServer.call(parent, :state)
    conn_sup_pid = parent_state.conn_sup_pid

    args = [
      parent,
      nsqd,
      parent_state.config,
      parent_state.topic,
      parent_state.channel,
      parent_state.conn_info_pid,
      parent_state.event_manager_pid
    ]

    conn_id = ConnInfo.conn_id(parent, nsqd)

    # When using nsqlookupd, we expect connections will be naturally
    # rediscovered if they fail.
    opts = [restart: :temporary, id: conn_id] ++ opts

    child = worker(NSQ.Connection, args, opts)
    Supervisor.start_child(conn_sup_pid, child)
  end

  def init(:ok) do
    supervise([], strategy: :one_for_one)
  end
end
