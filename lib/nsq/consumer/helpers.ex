defmodule NSQ.Consumer.Helpers do
  alias NSQ.Consumer, as: C
  alias NSQ.Consumer.Connections
  alias NSQ.ConnInfo

  @doc """
  Each connection is responsible for maintaining its own rdy_count in ConnInfo.
  This function sums all the values of rdy_count for each connection, which
  lets us get an accurate picture of a consumer's total RDY count. Not for
  external use.
  """
  @spec total_rdy_count(pid) :: integer
  def total_rdy_count(agent_pid) when is_pid(agent_pid) do
    ConnInfo.reduce(agent_pid, 0, fn {_, conn_info}, acc ->
      acc + conn_info.rdy_count
    end)
  end

  @doc """
  Convenience function; uses the consumer state to get the conn info pid. Not
  for external use.
  """
  @spec total_rdy_count(C.state()) :: integer
  def total_rdy_count(%{conn_info_pid: agent_pid} = _cons_state) do
    total_rdy_count(agent_pid)
  end

  @doc """
  Returns how much `max_in_flight` should be distributed to each connection.
  If `max_in_flight` is less than the number of connections, then this always
  returns 1 and they are randomly distributed via `redistribute_rdy`. Not for
  external use.
  """
  @spec per_conn_max_in_flight(C.state()) :: integer
  def per_conn_max_in_flight(cons_state) do
    max_in_flight = cons_state.max_in_flight
    conn_count = Connections.count(cons_state)

    if conn_count == 0 do
      0
    else
      min(max(1, max_in_flight / conn_count), max_in_flight) |> round
    end
  end

  @spec now() :: integer
  def now do
    :calendar.datetime_to_gregorian_seconds(:calendar.universal_time())
  end

  @spec conn_from_nsqd(pid, C.host_with_port(), C.state()) :: C.connection()
  def conn_from_nsqd(cons, nsqd, cons_state) do
    needle = ConnInfo.conn_id(cons, nsqd)

    Enum.find(Connections.get(cons_state), fn {conn_id, _} ->
      needle == conn_id
    end)
  end
end
