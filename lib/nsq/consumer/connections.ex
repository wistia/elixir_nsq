defmodule NSQ.Consumer.Connections do
  @moduledoc """
  Functions for connecting, disconnecting, managing connections, etc.
  """

  import NSQ.Consumer.Helpers
  require Logger
  alias NSQ.ConnInfo
  alias NSQ.Consumer, as: C
  alias NSQ.Consumer.RDY

  @doc """
  Initialized from NSQ.Consumer.Supervisor, sends the consumer a message on a
  fixed interval.
  """
  @spec discovery_loop(pid) :: any
  def discovery_loop(cons) do
    cons_state = C.get_state(cons)

    %NSQ.Config{
      lookupd_poll_interval: poll_interval,
      lookupd_poll_jitter: poll_jitter
    } = cons_state.config

    delay = poll_interval + round(poll_interval * poll_jitter * :rand.uniform())
    :timer.sleep(delay)

    GenServer.call(cons, :discover_nsqds)
    discovery_loop(cons)
  end

  def close(cons_state) do
    Logger.info("Closing connections for consumer #{inspect(self())}")
    connections = get(cons_state)

    Enum.each(connections, fn {_, conn_pid} ->
      Task.start_link(NSQ.Connection, :close, [conn_pid])
    end)

    {:ok, %{cons_state | stop_flag: true}}
  end

  def close!(cons_state) do
    {:ok, cons_state} = close(cons_state)
    cons_state
  end

  def refresh(cons_state) do
    {:ok, cons_state} = delete_dead(cons_state)
    {:ok, cons_state} = reconnect_failed(cons_state)
    {:ok, cons_state} = discover_nsqds_and_connect(self(), cons_state)
    {:ok, cons_state}
  end

  def refresh!(cons_state) do
    {:ok, cons_state} = refresh(cons_state)
    cons_state
  end

  @doc """
  Finds and updates list of live NSQDs using either NSQ.Config.nsqlookupds or
  NSQ.Config.nsqds, depending on what's configured. Preference is given to
  nsqlookupd. Not for external use.
  """
  @spec discover_nsqds_and_connect(pid, C.state()) :: {:ok, C.state()}
  def discover_nsqds_and_connect(cons, cons_state) do
    nsqds =
      cond do
        length(cons_state.config.nsqlookupds) > 0 ->
          Logger.debug(
            "(#{inspect(self())}) Discovering nsqds via nsqlookupds #{
              inspect(cons_state.config.nsqlookupds)
            }"
          )

          cons_state.config.nsqlookupds
          |> NSQ.Lookupd.nsqds_with_topic(cons_state.topic)

        length(cons_state.config.nsqds) > 0 ->
          Logger.debug(
            "(#{inspect(self())}) Using configured nsqds #{inspect(cons_state.config.nsqds)}"
          )

          cons_state.config.nsqds

        true ->
          raise "No nsqds or nsqlookupds are configured"
      end

    {:ok, _cons_state} = update(nsqds, cons, cons_state)
  end

  @doc """
  Any inactive connections will be killed and any newly discovered connections
  will be added. Existing connections with no change are left alone. Not for
  external use.
  """
  @spec update([C.host_with_port()], pid, C.state()) :: {:ok, C.state()}
  def update(discovered_nsqds, cons, cons_state) do
    dead_conns = dead_connections(discovered_nsqds, cons, cons_state)
    {:ok, cons_state} = stop_connections(dead_conns, cons, cons_state)

    nsqds_to_connect = new_nsqds(discovered_nsqds, cons, cons_state)
    {:ok, cons_state} = connect_to_nsqds(nsqds_to_connect, cons, cons_state)

    {:ok, cons_state}
  end

  @doc """
  Given a list of NSQD hosts, open a connection for each.
  """
  @spec connect_to_nsqds([C.host_with_port()], pid, C.state()) :: {:ok, C.state()}
  def connect_to_nsqds(nsqds, cons, cons_state \\ nil) do
    if length(nsqds) > 0 do
      Logger.info("Connecting to nsqds #{inspect(nsqds)}")
    end

    cons_state =
      Enum.reduce(nsqds, cons_state, fn nsqd, last_state ->
        {:ok, new_state} = connect_to_nsqd(nsqd, cons, last_state)
        new_state
      end)

    {:ok, cons_state}
  end

  @doc """
  Create a connection to NSQD and add it to the consumer's supervised list.
  Not for external use.
  """
  @spec connect_to_nsqd(C.host_with_port(), pid, C.state()) :: {:ok, C.state()}
  def connect_to_nsqd(nsqd, cons, cons_state) do
    Process.flag(:trap_exit, true)

    try do
      {:ok, _pid} =
        NSQ.Connection.Supervisor.start_child(
          cons,
          nsqd,
          cons_state
        )

      # We normally set RDY to 1, but if we're spawning more connections than
      # max_in_flight, we don't want to break our contract. In that case, the
      # `RDY.redistribute` loop will take care of getting this connection some
      # messages later.
      remaining_rdy = cons_state.max_in_flight - total_rdy_count(cons_state)

      cons_state =
        if remaining_rdy > 0 do
          conn = conn_from_nsqd(cons, nsqd, cons_state)
          {:ok, cons_state} = RDY.transmit(conn, 1, cons_state)
          cons_state
        else
          cons_state
        end

      {:ok, cons_state}
    catch
      :error, _ ->
        Logger.error("#{inspect(cons)}: Error connecting to #{inspect(nsqd)}")
        conn_id = ConnInfo.conn_id(cons, nsqd)
        ConnInfo.delete(cons_state, conn_id)
        {:ok, cons_state}
    after
      Process.flag(:trap_exit, false)
    end
  end

  @doc """
  Given a list of connections, force them to stop. Return the new state without
  those connections.
  """
  @spec stop_connections([C.connection()], pid, C.state()) :: {:ok, C.state()}
  def stop_connections(dead_conns, cons, cons_state) do
    if length(dead_conns) > 0 do
      Logger.info("Stopping connections #{inspect(dead_conns)}")
    end

    cons_state =
      Enum.reduce(dead_conns, cons_state, fn {nsqd, _pid}, last_state ->
        {:ok, new_state} = stop_connection(cons, nsqd, last_state)
        new_state
      end)

    {:ok, cons_state}
  end

  @doc """
  Given a single connection, immediately terminate its process (and all
  descendant processes, such as message handlers) and remove its info from the
  ConnInfo agent. Not for external use.
  """
  @spec stop_connection(pid, C.host_with_port(), C.state()) :: {:ok, C.state()}
  def stop_connection(cons, conn_id, cons_state) do
    # Terminate the connection for real.
    # TODO: Change this method to `kill_connection` and make `stop_connection`
    # graceful.
    Supervisor.terminate_child(cons_state.conn_sup_pid, conn_id)
    {:ok, cons_state} = cleanup_connection(cons, conn_id, cons_state)

    {:ok, cons_state}
  end

  @doc """
  When a connection is terminated or dies, we must do some extra cleanup.
  First, a terminated process isn't necessarily removed from the supervisor's
  list; therefore we call `Supervisor.delete_child/2`. And info about this
  connection like RDY must be removed so it doesn't contribute to `total_rdy`.
  Not for external use.
  """
  @spec cleanup_connection(pid, C.host_with_port(), C.state()) :: {:ok, C.state()}
  def cleanup_connection(_cons, conn_id, cons_state) do
    # If a connection is terminated normally or non-normally, it will still be
    # listed in the supervision tree. Let's remove it when we clean up.
    Supervisor.delete_child(cons_state.conn_sup_pid, conn_id)

    # Delete the connection info from the shared map so we don't use it to
    # perform calculations.
    ConnInfo.delete(cons_state, conn_id)

    {:ok, cons_state}
  end

  @doc """
  We may have open connections which nsqlookupd stops reporting. This function
  tells us which connections we have stored in state but not in nsqlookupd.
  Not for external use.
  """
  @spec dead_connections([C.host_with_port()], pid, C.state()) :: [C.connection()]
  def dead_connections(discovered_nsqds, cons, cons_state) do
    Enum.reject(get(cons_state), fn conn ->
      conn_already_discovered?(cons, conn, discovered_nsqds)
    end)
  end

  @doc """
  When nsqlookupd reports available producers, there are some that may not
  already be in our connection list. This function reports which ones are new
  so we can connect to them.
  """
  @spec new_nsqds([C.host_with_port()], pid, C.state()) :: [C.host_with_port()]
  def new_nsqds(discovered_nsqds, cons, cons_state) do
    Enum.reject(discovered_nsqds, fn nsqd ->
      nsqd_already_has_connection?(nsqd, cons, cons_state)
    end)
  end

  @doc """
  Frequently, when testing, we publish a message then immediately want a
  consumer to process it, but this doesn't work if the consumer doesn't
  discover the nsqd first. Only meant for testing.
  """
  @spec discover_nsqds(pid) :: :ok
  def discover_nsqds(sup_pid) do
    cons = C.get(sup_pid)
    GenServer.call(cons, :discover_nsqds)
    :ok
  end

  @doc """
  Iterate over all listed connections and delete the ones that are dead. This
  exists because it is difficult to reliably clean up a connection immediately
  after it is terminated (it might still be running). This function runs in the
  discovery loop to provide consistency.
  """
  @spec delete_dead(C.state()) :: {:ok, C.state()}
  def delete_dead(state) do
    Enum.each(get(state), fn {conn_id, pid} ->
      unless Process.alive?(pid) do
        Supervisor.delete_child(state.conn_sup_pid, conn_id)
      end
    end)

    {:ok, state}
  end

  def delete_dead!(state) do
    {:ok, state} = delete_dead(state)
    state
  end

  def reconnect_failed(state) do
    Enum.each(get(state), fn {_, pid} ->
      if Process.alive?(pid), do: GenServer.cast(pid, :reconnect)
    end)

    {:ok, state}
  end

  @spec count(C.state()) :: integer
  def count(cons_state) do
    %{active: active} = Supervisor.count_children(cons_state.conn_sup_pid)

    if is_integer(active) do
      active
    else
      Logger.warn(
        "(#{inspect(self())}) non-integer #{inspect(active)} returned counting connections, returning 0 instead"
      )

      0
    end
  end

  @doc """
  Returns all live connections for a consumer. This function, which takes
  a consumer's entire state as an argument, is for convenience. Not for
  external use.
  """
  @spec get(C.state()) :: [C.connection()]
  def get(%{conn_sup_pid: conn_sup_pid}) do
    children = Supervisor.which_children(conn_sup_pid)
    Enum.map(children, fn {child_id, pid, _, _} -> {child_id, pid} end)
  end

  @doc """
  Returns all live connections for a consumer. Used in tests. Not for external
  use.
  """
  @spec get(pid, C.state()) :: [C.connection()]
  def get(cons, cons_state \\ nil) when is_pid(cons) do
    cons_state = cons_state || C.get_state(cons)
    children = Supervisor.which_children(cons_state.conn_sup_pid)
    Enum.map(children, fn {child_id, pid, _, _} -> {child_id, pid} end)
  end

  @spec idle_with_rdy(C.state()) :: [C.connection()]
  def idle_with_rdy(cons_state) do
    conns = get(cons_state)

    Enum.filter(conns, fn conn ->
      conn_id = ConnInfo.conn_id(conn)

      [last_msg_t, rdy_count] =
        ConnInfo.fetch(
          cons_state,
          conn_id,
          [:last_msg_timestamp, :rdy_count]
        )

      sec_since_last_msg = now() - last_msg_t
      ms_since_last_msg = sec_since_last_msg * 1000

      Logger.debug(
        "(#{inspect(conn)}) rdy: #{rdy_count} (last message received #{sec_since_last_msg} seconds ago)"
      )

      ms_since_last_msg > cons_state.config.low_rdy_idle_timeout && rdy_count > 0
    end)
  end

  @spec conn_already_discovered?(pid, C.connection(), [C.host_with_port()]) :: boolean
  defp conn_already_discovered?(cons, {conn_id, _}, discovered_nsqds) do
    Enum.any?(discovered_nsqds, fn nsqd ->
      ConnInfo.conn_id(cons, nsqd) == conn_id
    end)
  end

  @spec nsqd_already_has_connection?(C.host_with_port(), pid, C.state()) :: boolean
  defp nsqd_already_has_connection?(nsqd, cons, cons_state) do
    needle = ConnInfo.conn_id(cons, nsqd)

    Enum.any?(get(cons_state), fn {conn_id, _} ->
      conn_id == needle
    end)
  end
end
