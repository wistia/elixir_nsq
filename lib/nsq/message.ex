defmodule NSQ.Message do
  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  require Logger
  use GenServer
  import NSQ.Protocol

  # ------------------------------------------------------- #
  # Module Attributes                                       #
  # ------------------------------------------------------- #
  @initial_state %{message: nil, handler: nil, handler_fun: nil}

  # ------------------------------------------------------- #
  # Struct Definition                                       #
  # ------------------------------------------------------- #
  defstruct [:id, :timestamp, :attempts, :data, :socket, max_attempts: 0]

  # ------------------------------------------------------- #
  # Behaviour Implementation                                #
  # ------------------------------------------------------- #
  def init(state) do
    Process.flag(:trap_exit, true)
    {:ok, state}
  end

  @doc """
  We expect the consumer connection to call NSQ.Message.process(pid, :process),
  which will both create the process and kick off the async job.
  """
  def handle_call(:process, from, state) do
    handler_fun = spawn_monitor(fn -> do_handle_process(state, from) end)
    state = %{state | handler_fun: handler_fun}
    {:reply, handler_fun, state}
  end

  @doc """
  When message processing finishes, it will call this, terminating the message
  server.
  """
  def handle_call(:message_done, _from, state) do
    {:stop, :normal, state}
  end

  # ------------------------------------------------------- #
  # API Definitions                                         #
  # ------------------------------------------------------- #
  @doc """
  Given a raw NSQ message with frame_type = message, return a struct with its
  parsed data.
  """
  def from_data(data) do
    {:ok, message} = decode_as_message(data)
    Map.merge(%NSQ.Message{}, message)
  end

  @doc """
  This is the main entry point when processing a message. It starts the message
  GenServer and immediately kicks of a processing call.
  """
  def process(message, handler, max_attempts, socket) do
    message = %{message | max_attempts: max_attempts, socket: socket}
    {pid, ref} = start_monitor(message, handler)
    GenServer.call(pid, :process)
    {pid, ref}
  end

  def start_monitor(message, handler) do
    {:ok, pid} = start(message, handler)
    ref = Process.monitor(pid)
    {pid, ref}
  end

  def start(message, handler) do
    state = %{@initial_state | message: message, handler: handler}
    GenServer.start(__MODULE__, state)
  end

  @doc """
  Tells NSQD that we're done processing this message. This is called
  automatically when the handler returns successfully, or when all retries have
  been exhausted.
  """
  def fin(message) do
    :gen_tcp.send(message.socket, encode({:fin, message.id}))
  end

  @doc """
  Tells NSQD to requeue the message, with delay and backoff. According to the
  go-nsq client (but doc'ed nowhere), a delay of -1 is a special value that
  means nsqd will calculate the duration itself based on the default requeue
  timeout. If backoff=true is set, then the connection will go into "backoff"
  mode, where it stops receiving messages for a fixed duration.
  """
  def req(message, delay \\ -1, backoff \\ false) do
    Logger.debug("(#{message.connection}) requeue msg ID #{message.id}, delay #{delay}, backoff #{backoff}")
    :gen_tcp.send(message.socket, encode({:req, message.id, delay}))
  end

  @doc """
  This function is intended to be used by the handler for long-running
  functions. They can set up a separate process that periodically touches the
  message until the process finishes.
  """
  def touch(message) do
    Logger.debug("(#{message.connection}) touch msg ID #{message.id}")
    :gen_tcp.send(message.socket, encode({:touch, message.id}))
  end

  # ------------------------------------------------------- #
  # Private Functions                                       #
  # ------------------------------------------------------- #
  # This runs in a separate process kicked off via the :process call. When it
  # finishes, it will also kill the message GenServer.
  defp do_handle_process(%{message: message, handler: handler}, from) do
    if should_fail_message?(message) do
      log_failed_message(message)
      fin(message)
      end_processing(from, message)
    else
      result = try do
        run_handler(handler, message)
      catch
        true -> {:req, -1}
      end

      case result do
        :ok -> fin(message)
        :req -> req(message)
        {:req, delay} -> req(message, delay)
      end

      end_processing(from, message)
    end
  end

  defp should_fail_message?(message) do
    message.max_attempts > 0 && message.attempts > message.max_attempts
  end

  # TODO: Custom error logging/handling?
  defp log_failed_message(message) do
    Logger.warning("msg #{message.id} attempted #{message.attempts} times, giving up")
  end

  # Handler can be either an anonymous function or a module that implements the
  # `handle_message\2` function.
  defp run_handler(handler, message) do
    if is_function(handler) do
      handler.(message.data, message)
    else
      handler.handle_message(message.data, message)
    end
  end

  defp end_processing({caller_pid, _ref}, message) do
    GenServer.call(caller_pid, {:message_done, message})
    Process.exit(self, :message_done)
  end
end
