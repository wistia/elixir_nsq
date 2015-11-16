defmodule NSQ.Message do
  import NSQ.Protocol
  use GenServer


  defstruct [:id, :timestamp, :attempts, :data]


  @initial_state %{message: nil, socket: nil, handler: nil}


  def from_data(data) do
    {:ok, message} = decode_as_message(data)
    Map.merge(%NSQ.Message{}, message)
  end


  def process(message, socket, handler) do
    {:ok, pid} = start_link(message, socket, handler)
    GenServer.call(pid, :process)
    pid
  end


  def start_link(message, socket, handler) do
    state = %{@initial_state | socket: socket, message: message, handler: handler}
    GenServer.start_link(__MODULE__, state)
  end


  def init(state) do
    {:ok, state}
  end


  @doc """
  We expect the consumer connection to call NSQ.Message.process(pid, :process),
  which will both create the process and kick off the async job.
  """
  def handle_call(cmd, from, state) do
    case cmd do
      :process ->
        spawn_link fn -> do_handle_process(state, from) end
      _else ->
        IO.inspect "Unhandled command #{inspect cmd} from #{from}"
    end
    {:noreply, state}
  end


  defp do_handle_process(%{message: message, handler: handler} = state, from) do
    case run_handler(handler, message) do
      {:ok} ->
        finish(state.socket, message)
        end_processing(message, from)
      {:req, delay} ->
        requeue(state.socket, message)
        end_processing(message, from)
      anything ->
        IO.puts "Unmatched handler.message return value #{inspect anything}"
    end
  end


  @doc """
  Handler can be either an anonymous function or a module that implements the
  `message` function.
  """
  defp run_handler(handler, message) do
    if is_function(handler) do
      handler.(message.data, message)
    else
      handler.message(message.data, message)
    end
  end


  def finish(socket, message) do
    :gen_tcp.send(socket, encode({:fin, message.id}))
  end


  def requeue(socket, message, delay \\ 1000) do
    :gen_tcp.send(socket, encode({:req, message.id, delay}))
  end


  defp end_processing(message, {pid, ref} = from) do
    GenServer.call(pid, {:done, message})
    Process.exit(self, :normal)
  end
end
