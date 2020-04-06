defmodule NSQ.ConnInfo do
  require Logger

  @doc """
  Given a consumer state object and an nsqd host/port tuple, return the
  connection ID.
  """
  def conn_id(parent, {host, port} = _nsqd) do
    "parent:#{inspect(parent)}:conn:#{host}:#{port}"
  end

  @doc """
  Given a `conn` object return by `Consumer.get_connections`, return the
  connection ID.
  """
  def conn_id({conn_id, conn_pid} = _conn) when is_pid(conn_pid) do
    conn_id
  end

  @doc """
  Given a connection state object, return the connection ID.
  """
  def conn_id(%{parent: parent, nsqd: {host, port}} = _conn_state) do
    conn_id(parent, {host, port})
  end

  @doc """
  Get info for all connections in a map like `%{conn_id: %{... data ...}}`.
  """
  def all(agent_pid) when is_pid(agent_pid) do
    Agent.get(agent_pid, fn data -> data end)
  end

  @doc """
  `func` is passed `conn_info` for each connection.
  """
  def reduce(agent_pid, start_acc, func) do
    Agent.get(agent_pid, fn all_conn_info ->
      Enum.reduce(all_conn_info, start_acc, func)
    end)
  end

  @doc """
  Get info for the connection matching `conn_id`.
  """
  def fetch(agent_pid, conn_id) when is_pid(agent_pid) do
    Agent.get(agent_pid, fn data -> data[conn_id] || %{} end)
  end

  @doc false
  def fetch(%{conn_info_pid: agent_pid}, conn_id) do
    fetch(agent_pid, conn_id)
  end

  @doc """
  Get specific data for the connection, e.g.:

      [rdy_count, last_rdy] = fetch(pid, "conn_id", [:rdy_count, :last_rdy])
      rdy_count = fetch(pid, "conn_id", :rdy_count)
  """
  def fetch(agent_pid, conn_id, keys) when is_pid(agent_pid) do
    Agent.get(agent_pid, fn data ->
      conn_map = data[conn_id] || %{}

      if is_list(keys) do
        Enum.map(keys, &Map.get(conn_map, &1))
      else
        Map.get(conn_map, keys)
      end
    end)
  end

  @doc false
  def fetch(%{conn_info_pid: agent_pid} = _state, {conn_id, _conn_pid} = _conn, keys) do
    fetch(agent_pid, conn_id, keys)
  end

  @doc false
  def fetch(%{conn_info_pid: agent_pid} = _state, conn_id, keys) do
    fetch(agent_pid, conn_id, keys)
  end

  @doc """
  Update the info for a specific connection matching `conn_id`. `conn_info` is
  passed to `func`, and the result of `func` is saved as the new `conn_info`.
  """
  def update(agent_pid, conn_id, func) when is_pid(agent_pid) and is_function(func) do
    Agent.update(agent_pid, fn data ->
      Map.put(data, conn_id, func.(data[conn_id] || %{}))
    end)
  end

  @doc """
  Update the info for a specific connection matching `conn_id`. The map is
  merged into the existing conn_info.
  """
  def update(agent_pid, conn_id, map) when is_pid(agent_pid) and is_map(map) do
    Agent.update(agent_pid, fn data ->
      new_conn_data = Map.merge(data[conn_id] || %{}, map)
      Map.put(data, conn_id, new_conn_data)
    end)
  end

  @doc false
  def update(%{conn_info_pid: agent_pid}, conn_id, func) do
    update(agent_pid, conn_id, func)
  end

  @doc false
  def update(%{conn_info_pid: agent_pid, parent: parent, nsqd: nsqd}, func) do
    update(agent_pid, conn_id(parent, nsqd), func)
  end

  @doc """
  Delete connection info matching `conn_id`. This should be called when a
  connection is terminated.
  """
  def delete(agent_pid, conn_id) when is_pid(agent_pid) do
    Agent.update(agent_pid, fn data -> Map.delete(data, conn_id) end)
  end

  @doc false
  def delete(%{conn_info_pid: agent_pid}, conn_id) do
    delete(agent_pid, conn_id)
  end

  @spec init(map) :: any
  def init(state) do
    update(state, %{
      max_rdy: state.max_rdy,
      rdy_count: 0,
      last_rdy: 0,
      messages_in_flight: 0,
      last_msg_timestamp: now(),
      retry_rdy_pid: nil,
      finished_count: 0,
      requeued_count: 0,
      failed_count: 0,
      backoff_count: 0
    })
  end

  @spec now :: integer
  defp now do
    {megasec, sec, microsec} = :os.timestamp()
    1_000_000 * megasec + sec + microsec / 1_000_000
  end
end
