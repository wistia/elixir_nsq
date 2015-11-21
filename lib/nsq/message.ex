defmodule NSQ.Message do
  import NSQ.Protocol
  use GenServer
  require Logger


  defstruct [:id, :timestamp, :attempts, :data, :socket, max_attempts: 0]


  @initial_state %{message: nil, handler: nil, handler_pid: nil}


  def from_data(data) do
    {:ok, message} = decode_as_message(data)
    Map.merge(%NSQ.Message{}, message)
  end


  def process(message, handler, max_attempts, socket) do
    message = %{message | max_attempts: max_attempts, socket: socket}
    {:ok, pid} = start(message, handler)
    ref = Process.monitor(pid)
    GenServer.call(pid, :process)
    {pid, ref}
  end


  def start(message, handler) do
    state = %{@initial_state | message: message, handler: handler}
    GenServer.start(__MODULE__, state)
  end


  def init(state) do
    {:ok, state}
  end


  @doc """
  We expect the consumer connection to call NSQ.Message.process(pid, :process),
  which will both create the process and kick off the async job.
  """
  def handle_call(:process, from, state) do
    handler_pid = spawn_link(fn -> do_handle_process(state, from) end)
    state = %{state | handler_pid: handler_pid}
    {:reply, handler_pid, state}
  end


  defp do_handle_process(%{message: message, handler: handler} = state, from) do
    if should_fail_message?(message) do
      log_failed_message(message)
      fin(message)
      end_processing(message, from)
    else
      result = try do
        run_handler(handler, message)
      catch
        any -> {:req, -1}
      end

      case result do
        {:ok} -> fin(message)
        {:req, delay} -> req(message)
      end

      end_processing(message, from)
    end
  end


  defp should_fail_message?(message) do
    message.max_attempts > 0 && message.attempts > message.max_attempts
  end


  defp log_failed_message(message) do
    Logger.warning("msg #{message.id} attempted #{message.attempts} times, giving up")
    # TODO: Custom error logging/handling?
  end


  @doc """
  Handler can be either an anonymous function or a module that implements the
  `handle_message\2` function.
  """
  def run_handler(handler, message) do
    if is_function(handler) do
      handler.(message.data, message)
    else
      handler.handle_message(message.data, message)
    end
  end


  def fin(message) do
    :gen_tcp.send(message.socket, encode({:fin, message.id}))
  end


  def req(message, delay \\ -1, backoff \\ false) do
    :gen_tcp.send(message.socket, encode({:req, message.id, delay}))
  end


  def touch(message) do
    Logger.debug("(#{message.connection}) touch msg ID #{message.id}")
    :gen_tcp.send(message.socket, encode({:touch, message.id}))
  end


  defp end_processing(message, {pid, _ref}) do
    GenServer.call(pid, {:message_done, message})
    Process.exit(self, :normal)
  end
end
