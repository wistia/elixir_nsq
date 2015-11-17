defmodule NSQ.Message do
  import NSQ.Protocol
  use GenServer
  require Logger


  defstruct [:id, :timestamp, :attempts, :data]


  @initial_state %{message: nil, socket: nil, handler: nil, handler_pid: nil}


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
  def handle_call(:process, from, state) do
    handler_pid = spawn_link(fn -> do_handle_process(state, from) end)
    state = %{state | handler_pid: handler_pid}
    {:noreply, state}
  end


  defp do_handle_process(%{message: message, handler: handler} = state, from) do
    case run_handler(handler, message) do
      {:ok} ->
        fin(state.socket, message)
        end_processing(message, from)
      {:req, delay} ->
        req(state.socket, message)
        end_processing(message, from)
    end
  end


  @doc """
  Handler can be either an anonymous function or a module that implements the
  `handle_message\2` function.
  """
  defp run_handler(handler, message) do
    if is_function(handler) do
      handler.(message.data, message)
    else
      handler.handle_message(message.data, message)
    end
  end


  def fin(socket, message) do
    :gen_tcp.send(socket, encode({:fin, message.id}))
  end


  def req(socket, message, delay \\ 1000) do
    :gen_tcp.send(socket, encode({:req, message.id, delay}))
  end


  defp end_processing(message, from) do
    GenServer.reply(from, {:done, message})
    Process.exit(self, :normal)
  end
end
