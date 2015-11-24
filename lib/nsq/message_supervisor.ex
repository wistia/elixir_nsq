defmodule NSQ.MessageSupervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, opts)
  end

  def start_child(msg_sup_pid, conn, message, conn_state \\ nil, opts \\ nil) do
    conn_state = conn_state || NSQ.Connection.get_state(conn_state)
    args = [
      conn,
      conn_state.config,
      conn_state.socket,
      message,
    ]
    # TODO: Get opts working?
    # opts = [restart: :temporary, id: message.id] ++ opts
    child = worker(Task, [NSQ.Message, :process, args])
    Supervisor.start_child(msg_sup_pid, child)
  end

  def init(:ok) do
    children = []
    supervise(children, strategy: :one_for_one)
  end
end
