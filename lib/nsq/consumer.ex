defmodule NSQ.Consumer do
  use GenServer
  require Logger

  @initial_state %{
    channel: nil,
    config: %{},
    connections: [],
    max_in_flight: 0,
    topic: nil,
    rdy_loop_pid: nil,
    discovery_loop_pid: nil,
    total_rdy_count: 0,
    rdy_retry_conns: %{}
  }

  def start_link(topic, channel, config \\ %{}, opts \\ %{}) do
    state = %{@initial_state | topic: topic, channel: channel, config: config}
    GenServer.start_link(__MODULE__, state)
  end

  @doc """
  On init, we create a connection for each NSQD instance discovered, and set
  up loops for discovery and RDY redistribution.
  """
  def init(state) do
    consumer = self
    connections = Enum.map state.config.nsqds, fn(nsqd) ->
      {:ok, conn} = NSQ.Connection.start_link(
        consumer, nsqd, state.config, state.topic, state.channel
      )
      conn
    end
    rdy_loop_pid = spawn_link(fn -> rdy_loop(consumer) end)
    discovery_loop_pid = spawn_link(fn -> discovery_loop(consumer) end)
    state = %{state |
      rdy_loop_pid: rdy_loop_pid,
      discovery_loop_pid: discovery_loop_pid,
      connections: connections
    }
    {:ok, %{state | connections: connections}}
  end

  def rdy_loop(consumer, delay \\ 30_000) do
    IO.puts "redistribute rdy"
    :timer.sleep(delay)
    rdy_loop(consumer, delay)
  end

  def discovery_loop(consumer, delay \\ 30_000) do
    IO.puts "discover"
    :timer.sleep(delay)
    discovery_loop(consumer, delay)
  end

  def handle_call({:retry_rdy, consumer, conn, count}, _from, state) do
    retry_pid = spawn_link fn ->
      :timer.sleep(5000)
      update_rdy(consumer, conn, count)
    end
    rdy_retry_conns = Map.put(state.rdy_retry_conns, conn, retry_pid)
    {:reply, retry_pid, %{state | rdy_retry_conns: rdy_retry_conns}}
  end

  def handle_call({:set_total_rdy, total_rdy_count}, _from, state) do
    {:reply, :ok, %{state | total_rdy_count: total_rdy_count}}
  end

  def handle_call({:state, prop}, _from, state) do
    {:reply, state[prop], state}
  end

  def get_state(consumer, prop) do
    GenServer.call(consumer, {:state, prop})
  end


  def maybe_update_rdy(consumer, conn) do
    in_backoff = in_backoff?(consumer)
    in_backoff_timeout = in_backoff_timeout?(consumer)
    if in_backoff > 0 || in_backoff_timeout > 0 do
      Logger.debug("(#{conn}) skip sending RDY in_backoff:#{in_backoff} || "
        <> "in_backoff_timeout:#{in_backoff_timeout}")
    else
      remain = NSQ.Connection.get_state(:rdy, conn)
      last_rdy_count = NSQ.Connection.get_state(:last_rdy, conn)
      count = per_conn_max_in_flight(consumer)
      if remain <= 1 || remain < (last_rdy_count / 4) || (count > 0 && count < remain) do
        Logger.debug("(#{conn}) sending RDY #{count} (#{remain} remain from last RDY #{last_rdy_count})")
        update_rdy(consumer, conn, count)
      else
        Logger.debug("(#{conn}) skip sending RDY #{count} (#{remain} remain out of last RDY #{last_rdy_count})")
      end
    end
  end


  def update_rdy(consumer, conn, count) do
    conn_max_rdy = NSQ.Connection.get_state(conn, :max_rdy)
    if count > conn_max_rdy, do: count = conn_max_rdy

    # TODO: Understand RDY retry timers and do something with them here?

    rdy_count = NSQ.Connection.get_state(conn, :rdy)
    max_in_flight = get_state(consumer, :max_in_flight)
    total_rdy_count = get_state(consumer, :total_rdy_count)
    max_possible_rdy = max_in_flight + total_rdy_count + rdy_count

    if max_possible_rdy > 0 && max_possible_rdy < count do
      count = max_possible_rdy
    end

    if max_possible_rdy <= 0 && count > 0 do
      if rdy_count == 0 do
        # Schedule update_rdy(consumer, conn, count) for this connection again
        # in 5 seconds. This is to prevent eternal starvation.
        GenServer.call(consumer, {:retry_rdy, consumer, conn, count})
      end
      {:error, :over_max_in_flight}
    end
  end


  def send_rdy(consumer, conn, count) do
    last_rdy = NSQ.Connection.get_state(conn, :last_rdy)
    if count == 0 && last_rdy == 0 do
      # do nothing
    else
      conn_rdy = NSQ.Connection.get_state(conn, :rdy)
      total_rdy_count = GenServer.get_state(consumer, :total_rdy_count)
      total_rdy_count = total_rdy_count - conn_rdy + count
      GenServer.call(consumer, {:set_total_rdy, total_rdy_count})
      GenServer.call(conn, {:set_rdy, count})
      :ok = GenServer.call(conn, {:command, {:rdy, count}})
    end
  end


  def in_backoff?(conn) do
    NSQ.Connection.get_state(conn, :backoff_counter) > 0
  end


  def in_backoff_timeout?(conn) do
    NSQ.Connection.get_state(conn, :backoff_duration) > 0
  end


  def per_conn_max_in_flight(consumer) do
    in_flight = get_state(consumer, :max_in_flight)
    conn_count = length get_state(consumer, :connections)
    min(max(1, in_flight / conn_count), in_flight)
  end
end
