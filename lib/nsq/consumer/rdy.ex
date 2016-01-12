defmodule NSQ.Consumer.RDY do
  @moduledoc """
  Consumers have a lot of logic around calculating and distributing RDY. This
  is where that goes!
  """


  require Logger
  alias NSQ.ConnInfo
  alias NSQ.Consumer, as: C
  alias NSQ.Consumer.Connections
  import NSQ.Consumer.Helpers


  @doc """
  If we're not in backoff mode and we've hit a "trigger point" to update RDY,
  then go ahead and update RDY. Not for external use.
  """
  @spec maybe_update(pid, C.connection, C.state) :: {:ok, C.state}
  def maybe_update(cons, conn, cons_state) do
    if cons_state.backoff_counter > 0 || cons_state.backoff_duration > 0 do
      # In backoff mode, we only let `start_stop_continue_backoff/3` handle
      # this case.
      Logger.debug """
        (#{inspect conn}) skip sending RDY in_backoff:#{cons_state.backoff_counter} || in_backoff_timeout:#{cons_state.backoff_duration}
      """
      {:ok, cons_state}
    else
      [remain, last_rdy] = ConnInfo.fetch(
        cons_state, ConnInfo.conn_id(conn), [:rdy_count, :last_rdy]
      )
      desired_rdy = per_conn_max_in_flight(cons_state)

      if remain <= 1 || remain < (last_rdy / 4) || (desired_rdy > 0 && desired_rdy < remain) do
        Logger.debug """
          (#{inspect conn}) sending RDY #{desired_rdy} \
          (#{remain} remain from last RDY #{last_rdy})
        """
        {:ok, _cons_state} = update(cons, conn, desired_rdy, cons_state)
      else
        Logger.debug """
          (#{inspect conn}) skip sending RDY #{desired_rdy} \
          (#{remain} remain out of last RDY #{last_rdy})
        """
        {:ok, cons_state}
      end
    end
  end


  @doc """
  Try to update RDY for a given connection, taking configuration and the
  current state into account. Not for external use.
  """
  @spec update(pid, C.connection, integer, C.state) :: {:ok, C.state}
  def update(cons, conn, new_rdy, cons_state) do
    conn_info = ConnInfo.fetch(cons_state, ConnInfo.conn_id(conn))

    cancel_outstanding_retry(cons_state, conn)

    # Cap the given RDY based on the connection config.
    new_rdy = [new_rdy, conn_info.max_rdy] |> Enum.min |> round

    # Cap the given RDY based on how much we can actually assign. Unless it's
    # 0, in which case we'll be retrying.
    max_possible_rdy = calc_max_possible(cons_state, conn_info)
    if max_possible_rdy > 0 do
      new_rdy = [new_rdy, max_possible_rdy] |> Enum.min |> round
    end

    if max_possible_rdy <= 0 && new_rdy > 0 do
      if conn_info.rdy_count == 0 do
        # Schedule RDY.update(consumer, conn, new_rdy) for this connection
        # again in 5 seconds. This is to prevent eternal starvation.
        {:ok, cons_state} = retry(cons, conn, new_rdy, cons_state)
      end
      {:ok, cons_state}
    else
      {:ok, _cons_state} = transmit(conn, new_rdy, cons_state)
    end
  end


  @doc """
  Delay for a configured interval, then call RDY.update. Not for external use.
  """
  @spec retry(pid, C.connection, integer, C.state) :: {:ok, C.state}
  def retry(cons, conn, count, cons_state) do
    delay = cons_state.config.rdy_retry_delay
    Logger.debug("(#{inspect conn}) retry RDY in #{delay / 1000} seconds")

    {:ok, retry_pid} = Task.start_link fn ->
      :timer.sleep(delay)
      GenServer.call(cons, {:update_rdy, conn, count})
    end
    ConnInfo.update(cons_state, ConnInfo.conn_id(conn), %{retry_rdy_pid: retry_pid})

    {:ok, cons_state}
  end


  @doc """
  Send a RDY command for the given connection.
  """
  @spec transmit(C.connection, integer, C.state) :: {:ok, C.state}
  def transmit({_id, pid} = conn, count, cons_state) do
    [last_rdy] = ConnInfo.fetch(cons_state, ConnInfo.conn_id(conn), [:last_rdy])

    if count == 0 && last_rdy == 0 do
      {:ok, cons_state}
    else
      # We intentionally don't match this GenServer.call. If the socket isn't
      # set up or is erroring out, we don't want to propagate that connection
      # error to the consumer.
      NSQ.Connection.cmd_noresponse(pid, {:rdy, count})
      {:ok, cons_state}
    end
  end


  @doc """
  This will only be triggered in odd cases where we're in backoff or when there
  are more connections than max in flight. It will randomly change RDY on
  some connections to 0 and 1 so that they're all guaranteed to eventually
  process messages. Not for external use.
  """
  @spec redistribute(pid, C.state) :: {:ok, C.state}
  def redistribute(cons, cons_state) do
    if should_redistribute?(cons_state) do
      conns = Connections.get(cons_state)
      conn_count = length(conns)

      if conn_count > cons_state.max_in_flight do
        Logger.debug """
          redistributing RDY state
          (#{conn_count} conns > #{cons_state.max_in_flight} max_in_flight)
        """
      end

      if cons_state.backoff_counter > 0 && conn_count > 1 do
        Logger.debug """
          redistributing RDY state (in backoff and #{conn_count} conns > 1)
        """
      end

      # Free up any connections that are RDY but not processing messages.
      give_up_for_idle_connections(cons, cons_state)

      # Determine how much RDY we can distribute. This needs to happen before
      # we give up RDY, or max_in_flight will end up equalling RDY.
      available_max_in_flight = get_available_max_in_flight(cons_state)

      # Distribute it!
      distribute_randomly(
        cons, conns, available_max_in_flight, cons_state
      )
    else
      # Nothing to do. This is the usual path!
      {:ok, cons_state}
    end
  end

  # Helper for redistribute; we set RDY to 1 for _some_ connections that
  # were halted, randomly, until there's no more RDY left to assign.
  @spec distribute_randomly(pid, [C.connection], integer, C.state) ::
    {:ok, C.state}
  defp distribute_randomly(cons, possible_conns, available_max_in_flight, cons_state) do
    if length(possible_conns) == 0 || available_max_in_flight <= 0 do
      {:ok, cons_state}
    else
      [conn|rest] = Enum.shuffle(possible_conns)
      Logger.debug("(#{inspect conn}) redistributing RDY")
      {:ok, cons_state} = update(cons, conn, 1, cons_state)
      distribute_randomly(
        cons, rest, available_max_in_flight - 1, cons_state
      )
    end
  end

  @spec cancel_outstanding_retry(C.state, C.connection) :: any
  defp cancel_outstanding_retry(cons_state, conn) do
    conn_info = ConnInfo.fetch(cons_state, ConnInfo.conn_id(conn))

    # If this is for a connection that's retrying, kill the timer and clean up.
    if retry_pid = conn_info.retry_rdy_pid do
      if Process.alive?(retry_pid) do
        Logger.debug("(#{inspect conn}) rdy retry pid #{inspect retry_pid} detected, killing")
        Process.exit(retry_pid, :normal)
      end

      ConnInfo.update(cons_state, ConnInfo.conn_id(conn), %{retry_rdy_pid: nil})
    end
  end

  @spec calc_max_possible(C.state, map) :: integer
  defp calc_max_possible(cons_state, conn_info) do
    rdy_count = conn_info.rdy_count
    max_in_flight = cons_state.max_in_flight
    total_rdy = total_rdy_count(cons_state)
    max_in_flight - total_rdy + rdy_count
  end


  @spec should_redistribute?(C.state) :: boolean
  defp should_redistribute?(cons_state) do
    conn_count = Connections.count(cons_state)
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


  @spec give_up_for_idle_connections(pid, C.state) :: [C.connection]
  defp give_up_for_idle_connections(cons, cons_state) do
    conns = Connections.get(cons_state)
    Enum.map conns, fn(conn) ->
      conn_id = ConnInfo.conn_id(conn)
      [last_msg_t, rdy_count] = ConnInfo.fetch(
        cons_state, conn_id, [:last_msg_timestamp, :rdy_count]
      )
      sec_since_last_msg = now - last_msg_t
      ms_since_last_msg = sec_since_last_msg * 1000

      Logger.debug(
        "(#{inspect conn}) rdy: #{rdy_count} (last message received #{sec_since_last_msg} seconds ago)"
      )

      is_idle = ms_since_last_msg > cons_state.config.low_rdy_idle_timeout
      if rdy_count > 0 && is_idle do
        Logger.debug("(#{inspect conn}) idle connection, giving up RDY")
        {:ok, _cons_state} = update(cons, conn, 0, cons_state)
      end

      conn
    end
  end


  # Cap available max in flight based on current RDY/backoff status.
  defp get_available_max_in_flight(cons_state) do
    total_rdy = total_rdy_count(cons_state)
    if cons_state.backoff_counter > 0 do
      # In backoff mode, we only ever want RDY=1 for the whole consumer. This
      # makes sure that available is only 1 if total_rdy is 0.
      1 - total_rdy
    else
      cons_state.max_in_flight - total_rdy
    end
  end
end
