defmodule NSQ.Connection.Command do
  @moduledoc """
  Both consumers and producers use Connection, which, at a lower level, kicks
  off message processing and issues commands to NSQD. This module handles some
  of the trickier command queueing, flushing, etc.
  """

  alias NSQ.Connection, as: C
  alias NSQ.Connection.Buffer
  alias NSQ.ConnInfo
  import NSQ.Protocol

  def exec(state, cmd, kind, {_, ref} = from) do
    if state.connected do
      state = send_data_and_queue_resp(state, cmd, from, kind)
      state = update_state_from_cmd(cmd, state)
      {{:ok, ref}, state}
    else
      # Not connected currently; add this call onto a queue to be run as soon
      # as we reconnect.
      state = %{state | cmd_queue: :queue.in({cmd, from, kind}, state.cmd_queue)}
      {{:queued, :no_socket}, state}
    end
  end

  @spec send_data_and_queue_resp(C.state(), tuple, {reference, pid}, atom) :: C.state()
  def send_data_and_queue_resp(state, cmd, from, kind) do
    state |> Buffer.send!(encode(cmd))

    if kind == :noresponse do
      state
    else
      %{state | cmd_resp_queue: :queue.in({cmd, from, kind}, state.cmd_resp_queue)}
    end
  end

  @spec send_response_to_caller(C.state(), binary) :: {:ok, C.state()}
  def send_response_to_caller(state, data) do
    GenEvent.notify(state.event_manager_pid, {:response, data})
    {item, cmd_resp_queue} = :queue.out(state.cmd_resp_queue)

    case item do
      {:value, {_cmd, {pid, ref}, :reply}} ->
        send(pid, {ref, data})

      :empty ->
        :ok
    end

    {:ok, %{state | cmd_resp_queue: cmd_resp_queue}}
  end

  @spec flush_cmd_queue(C.state()) :: C.state()
  def flush_cmd_queue(state) do
    {item, new_queue} = :queue.out(state.cmd_queue)

    case item do
      {:value, {cmd, from, kind}} ->
        state = send_data_and_queue_resp(state, cmd, from, kind)
        flush_cmd_queue(%{state | cmd_queue: new_queue})

      :empty ->
        {:ok, %{state | cmd_queue: new_queue}}
    end
  end

  def flush_cmd_queue!(state) do
    {:ok, state} = flush_cmd_queue(state)
    state
  end

  @spec update_state_from_cmd(tuple, C.state()) :: C.state()
  def update_state_from_cmd(cmd, state) do
    case cmd do
      {:rdy, count} ->
        ConnInfo.update(state, %{rdy_count: count, last_rdy: count})
        state

      _any ->
        state
    end
  end
end
