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
    rdy_retry_conns: %{},
    need_rdy_redistributed: false,
    stop_flag: false
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


  def handle_call(:redistribute_rdy, _from, state) do
    :ok = redistribute_rdy(self, state)
  end


  def rdy_loop(cons) do
    cons_state = NSQ.Consumer.get_state(cons)
    GenServer.call(cons, :redistribute_rdy)
    delay = cons_state.config.rdy_redistribute_interval || 30_000
    :timer.sleep(delay)
    rdy_loop(cons)
  end


  def discovery_loop(cons, delay \\ 30_000) do
    IO.puts "discover"
    :timer.sleep(delay)
    discovery_loop(cons, delay)
  end


  def handle_call({:state, prop}, _from, state) do
    {:reply, state[prop], state}
  end


  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end


  def get_state(cons, prop) do
    GenServer.call(cons, {:state, prop})
  end


  def get_state(cons) do
    GenServer.call(cons, :state)
  end


  def backoff(cons, duration, cons_state \\ nil) do
    cons_state = cons_state || NSQ.Consumer.get_state(cons)
    spawn_link fn ->
      :timer.sleep(duration)
      resume(cons)
    end
    {:ok, %{cons_state | backoff_duration: duration}}
  end


  def resume(cons, cons_state \\ nil) do
    cons_state = cons_state || NSQ.Consumer.get_state(cons)
    if cons_state.stop_flag do
      {:ok, %{cons_state | backoff_duration: 0}}
    else
      # pick a random connection to test the waters
      conn_count = length(cons_state.connections)

      if conn_count == 0 do
        Logger.warn("no connection available to resume")
        Logger.warn("backing off for 1 second")
        {:ok, cons_state} = backoff(cons, 1000, cons_state)
      else
        choice = Enum.random(cons_state.connections)
        Logger.warning("(#{choice}) backoff timeout expired, sending RDY 1")

        # while in backoff only ever let 1 message at a time through
        {:ok, cons_state} = update_rdy(cons, choice, 1, cons_state)
      end
    end
  end


  def redistribute_rdy(cons, cons_state \\ nil) do
    cons_state = NSQ.Consumer.get_state(cons)

    if should_redistribute_rdy?(cons_state) do
      conns = cons_state.connections
      max_in_flight = cons_state.max_in_flight
      in_backoff = cons_state.backoff_counter > 0
      conn_count = length(conns)

      if conn_count > max_in_flight do
        Logger.debug("redistributing RDY state (#{conn_count} conns > #{max_in_flight} max_in_flight)")
      end

      if in_backoff && conn_count > 1 do
        Logger.debug("redistributing RDY state (in backoff and #{conn_count} conns > 1)")
      end

      possible_conns = Enum.map conns, fn(conn) ->
        conn_state = NSQ.Connection.get_state(conn)
        time_since_last_msg = now - conn_state.last_msg_timestamp
        rdy_count = conn_state.rdy_count

        Logger.debug("(#{conn}) rdy: #{rdy_count} (last message received #{inspect datetime_from_timestamp(time_since_last_msg)})")
        if rdy_count > 0 && time_since_last_msg > cons_state.config.low_rdy_idle_timeout do
          Logger.debug("(#{conn}) idle connection, giving up RDY")
          update_rdy(cons, conn, 0, cons_state, conn_state)
        end

        conn
      end

      available_max_in_flight = max_in_flight - cons_state.total_rdy_count
      if in_backoff do
        available_max_in_flight = 1 - cons_state.total_rdy_count
      end

      redistribute_rdy_r(cons, possible_conns, available_max_in_flight)
    else
      # nothing to do
      :ok
    end
  end


  defp redistribute_rdy_r(cons, possible_conns, available_max_in_flight, cons_state \\ nil) do
    if length(possible_conns) == 0 || available_max_in_flight <= 0 do
      :ok
    else
      cons_state = cons_state || NSQ.Consumer.get_state(cons)
      [conn|rest] = Enum.shuffle(possible_conns)
      Logger.debug("(#{conn}) redistributing RDY")
      update_rdy(cons, conn, 1, cons_state)
      redistribute_rdy_r(cons, rest, available_max_in_flight - 1, cons_state)
    end
  end


  defp should_redistribute_rdy?(cons_state) do
    conn_count = length(cons_state.connections)
    in_backoff = cons_state.backoff_counter > 0
    in_backoff_timeout = cons_state.backoff_duration > 0

    !in_backoff_timeout
      && conn_count > 0
      && (
        conn_count > cons_state.max_in_flight
        || (in_backoff && conn_count > 1)
        || cons_state.need_rdy_redistributed
      )
  end


  def maybe_update_rdy(cons, conn, cons_state \\ nil, conn_state \\ nil) do
    cons_state = cons_state || NSQ.Consumer.get_state(cons)
    conn_state = conn_state || NSQ.Connection.get_state(conn)

    in_backoff = cons_state.backoff_counter > 0
    in_backoff_timeout = cons_state.backoff_duration > 0

    if in_backoff > 0 || in_backoff_timeout > 0 do
      Logger.debug("(#{conn}) skip sending RDY in_backoff:#{in_backoff} || "
        <> "in_backoff_timeout:#{in_backoff_timeout}")
    else
      remain = conn_state.rdy_count
      last_rdy_count = conn_state.last_rdy
      count = per_conn_max_in_flight(cons)

      if remain <= 1 || remain < (last_rdy_count / 4) || (count > 0 && count < remain) do
        Logger.debug("(#{conn}) sending RDY #{count} (#{remain} remain from last RDY #{last_rdy_count})")
        update_rdy(cons, conn, cons_state, conn_state)
      else
        Logger.debug("(#{conn}) skip sending RDY #{count} (#{remain} remain out of last RDY #{last_rdy_count})")
        {:ok, cons_state}
      end
    end
  end


  def update_rdy(cons, conn, count, cons_state \\ nil, conn_state \\ nil) do
    cons_state = cons_state || NSQ.Consumer.get_state(cons)
    conn_state = conn_state || NSQ.Connection.get_state(conn)

    if count > conn_state.max_rdy, do: count = conn_state.max_rdy

    # TODO: Understand RDY retry timers and do something with them here?

    rdy_count = conn_state.rdy_count
    max_in_flight = cons_state.max_in_flight
    total_rdy_count = cons_state.total_rdy_count
    max_possible_rdy = max_in_flight + total_rdy_count + rdy_count

    if max_possible_rdy > 0 && max_possible_rdy < count do
      count = max_possible_rdy
    end

    if max_possible_rdy <= 0 && count > 0 do
      if rdy_count == 0 do
        # Schedule update_rdy(consumer, conn, count) for this connection again
        # in 5 seconds. This is to prevent eternal starvation.
        retry_rdy(cons, conn, count, cons_state)
      end
      {:error, :over_max_in_flight}
    else
      {:ok, cons_state} = send_rdy(cons, conn, count, cons_state)
      total_rdy_count = cons_state.total_rdy_count - conn_state.rdy_count + count
      {:ok, %{cons_state | total_rdy_count: total_rdy_count}}
    end
  end


  def retry_rdy(cons, conn, count, cons_state \\ nil) do
    cons_state = cons_state || NSQ.Consumer.get_state(cons)

    retry_pid = spawn_link fn ->
      :timer.sleep(5000)
      update_rdy(cons, conn, count)
    end
    rdy_retry_conns = Map.put(cons_state.rdy_retry_conns, conn, retry_pid)

    {:ok, retry_pid, %{cons_state | rdy_retry_conns: rdy_retry_conns}}
  end


  def send_rdy(cons, conn, count, cons_state \\ nil, conn_state \\ nil) do
    cons_state = cons_state || NSQ.Consumer.get_state(cons)
    conn_state = conn_state || NSQ.Connection.get_state(conn)

    if count == 0 && conn_state.last_rdy == 0 do
      {:ok, cons_state}
    else
      :ok = GenServer.call(conn, {:rdy, count})
      {:ok, cons_state}
    end
  end


  def per_conn_max_in_flight(cons, cons_state \\ nil) do
    cons_state = cons_state || NSQ.Consumer.get_state(cons)
    max_in_flight = cons_state.max_in_flight
    conn_count = length(cons_state.connections)
    min(max(1, max_in_flight / conn_count), max_in_flight)
  end


  defp now do
    :calendar.datetime_to_gregorian_seconds(:calendar.universal_time)
  end


  epoch = {{1970, 1, 1}, {0, 0, 0}}
  @epoch :calendar.datetime_to_gregorian_seconds(epoch)
  defp datetime_from_timestamp(timestamp) do
    timestamp
    |> +(@epoch)
    |> :calendar.gregorian_seconds_to_datetime
  end
end
