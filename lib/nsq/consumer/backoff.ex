defmodule NSQ.Consumer.Backoff do
  @moduledoc """
  When messages fail unexpectedly hard, we go into "backoff mode".
  """

  require Logger
  alias NSQ.Consumer, as: C
  alias NSQ.Consumer.Connections
  alias NSQ.Consumer.RDY
  import NSQ.Consumer.Helpers

  @doc """
  Decision point about whether to continue/end/ignore backoff.
  """
  @spec start_stop_continue(pid, atom, C.state()) :: {:ok, C.state()}
  def start_stop_continue(cons, backoff_signal, cons_state) do
    {backoff_updated, cons_state} = cons_state |> update_backoff_counter(backoff_signal)

    cond do
      cons_state.config.max_backoff_duration <= 0 ->
        # Never backoff if max_backoff_duration is <= 0
        {:ok, cons_state}

      cons_state.backoff_counter == 0 && backoff_updated ->
        {:ok, _state} = exit_backoff(cons, cons_state)

      cons_state.backoff_counter > 0 ->
        {:ok, _state} = backoff(cons, cons_state, backoff_signal)

      true ->
        {:ok, cons_state}
    end
  end

  def start_stop_continue!(cons, backoff_signal, cons_state) do
    {:ok, cons_state} = start_stop_continue(cons, backoff_signal, cons_state)
    cons_state
  end

  @doc """
  This function is called asynchronously from `resume_later`. It
  will cause one connection to have RDY 1. We only resume after this if
  messages succeed a number of times == backoff_counter. (That logic is in
  start_stop_continue.)
  """
  @spec resume(pid, C.state()) :: {:ok, C.state()}
  def resume(_cons, %{backoff_duration: 0, backoff_counter: 0} = cons_state),
    # looks like we successfully left backoff mode already
    do: {:ok, cons_state}

  def resume(_cons, %{stop_flag: true} = cons_state),
    do: {:ok, %{cons_state | backoff_duration: 0}}

  def resume(cons, cons_state) do
    {:ok, cons_state} =
      if Connections.count(cons_state) == 0 do
        # This could happen if nsqlookupd suddenly stops discovering
        # connections. Maybe a network partition?
        Logger.warn("no connection available to resume")
        Logger.warn("backing off for 1 second")
        resume_later(cons, 1000, cons_state)
      else
        # pick a random connection to test the waters
        conn = random_connection_for_backoff(cons_state)
        Logger.warn("(#{inspect(conn)}) backoff timeout expired, sending RDY 1")

        # while in backoff only ever let 1 message at a time through
        RDY.update(cons, conn, 1, cons_state)
      end

    {:ok, %{cons_state | backoff_duration: 0}}
  end

  def resume!(cons, cons_state) do
    {:ok, cons_state} = resume(cons, cons_state)
    cons_state
  end

  @spec backoff(pid, C.state(), boolean) :: {:ok, C.state()}
  defp backoff(cons, cons_state, backoff_signal) do
    backoff_duration = calculate_backoff(cons_state)

    Logger.warn(
      "backing off for #{backoff_duration / 1000} seconds (backoff level #{
        cons_state.backoff_counter
      }), setting all to RDY 0"
    )

    # send RDY 0 immediately (to *all* connections)
    cons_state =
      Enum.reduce(Connections.get(cons_state), cons_state, fn conn, last_state ->
        {:ok, new_state} = RDY.update(cons, conn, 0, last_state)
        new_state
      end)

    GenEvent.notify(cons_state.event_manager_pid, backoff_signal)
    {:ok, _cons_state} = resume_later(cons, backoff_duration, cons_state)
  end

  @spec update_backoff_counter(C.state(), atom) :: {boolean, C.state()}
  defp update_backoff_counter(cons_state, backoff_signal) do
    {backoff_updated, backoff_counter} =
      cond do
        backoff_signal == :resume ->
          if cons_state.backoff_counter <= 0 do
            {false, cons_state.backoff_counter}
          else
            {true, cons_state.backoff_counter - 1}
          end

        backoff_signal == :backoff ->
          {true, cons_state.backoff_counter + 1}

        true ->
          {false, cons_state.backoff_counter}
      end

    cons_state = %{cons_state | backoff_counter: backoff_counter}

    {backoff_updated, cons_state}
  end

  @spec exit_backoff(pid, C.state()) :: {:ok, C.state()}
  defp exit_backoff(cons, cons_state) do
    count = per_conn_max_in_flight(cons_state)
    Logger.warn("exiting backoff, returning all to RDY #{count}")

    cons_state =
      Enum.reduce(Connections.get(cons_state), cons_state, fn conn, last_state ->
        {:ok, new_state} = RDY.update(cons, conn, count, last_state)
        new_state
      end)

    GenEvent.notify(cons_state.event_manager_pid, :resume)
    {:ok, cons_state}
  end

  # Try resuming from backoff in a few seconds.
  @spec resume_later(pid, integer, C.state()) ::
          {:ok, C.state()}
  defp resume_later(cons, duration, cons_state) do
    Task.start_link(fn ->
      :timer.sleep(duration)
      GenServer.cast(cons, :resume)
    end)

    cons_state = %{cons_state | backoff_duration: duration}
    {:ok, cons_state}
  end

  @spec random_connection_for_backoff(C.state()) :: C.connection()
  defp random_connection_for_backoff(cons_state) do
    if cons_state.config.backoff_strategy == :test do
      # When testing, we're only sending 1 message at a time to a single
      # nsqd. In this mode, instead of a random connection, always use the
      # first one that was defined, which ends up being the last one in our
      # list.
      cons_state |> Connections.get() |> List.last()
    else
      cons_state |> Connections.get() |> Enum.random()
    end
  end

  # Returns the backoff duration in milliseconds. Different strategies can
  # technically be used, but currently there is only `:exponential` in
  # production mode and `:test` for tests. Not for external use.
  @spec calculate_backoff(C.state()) :: integer
  defp calculate_backoff(cons_state) do
    case cons_state.config.backoff_strategy do
      :exponential -> exponential_backoff(cons_state)
      :test -> 200
    end
  end

  # Used to calculate backoff in milliseconds in production. We include jitter
  # so that, if we have many consumers in a cluster, we avoid the thundering
  # herd problem when they attempt to resume. Not for external use.
  @spec exponential_backoff(C.state()) :: integer
  defp exponential_backoff(cons_state) do
    attempts = cons_state.backoff_counter
    mult = cons_state.config.backoff_multiplier

    min(
      mult * :math.pow(2, attempts),
      cons_state.config.max_backoff_duration
    )
    |> round
  end
end
