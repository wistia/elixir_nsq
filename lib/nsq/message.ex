defmodule NSQ.Message do
  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  require Logger
  import NSQ.Protocol

  # ------------------------------------------------------- #
  # Struct Definition                                       #
  # ------------------------------------------------------- #
  defstruct [:id, :timestamp, :attempts, :body, :connection, :consumer, :socket, :config]

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
  def process(message) do
    # Kick off processing in a separate process, so we can kill it if it takes
    # too long.
    parent = self
    pid = spawn_link fn ->
      NSQ.Message.process_without_timeout(parent, message)
    end

    # We expect our function will send us a message when it's done. Block until
    # that happens. If it takes too long, requeue the message and cancel
    # processing.
    receive do
      {:message_done, _msg} ->
        unlink_and_exit(pid)
    after
      message.config.msg_timeout ->
        unlink_and_exit(pid)
        NSQ.Message.req(message)
    end

    # Even if we've killed the message processing, we're still "done"
    # processing it for now. That is, we should free up a spot on
    # messages_in_flight.
    {:message_done, message}
  end

  def process_without_timeout(parent, message) do
    if should_fail_message?(message) do
      log_failed_message(message)
      fin(message)
    else
      result = try do
        run_handler(message.config.message_handler, message)
      catch
        any ->
          Logger.error "Error running message handler: #{inspect any}"
          {:req, -1}
      end

      case result do
        :ok -> fin(message)
        :req -> req(message)
        {:req, delay} -> req(message, delay)
      end
    end

    send parent, {:message_done, message}
  end

  @doc """
  Tells NSQD that we're done processing this message. This is called
  automatically when the handler returns successfully, or when all retries have
  been exhausted.
  """
  def fin(message) do
    Logger.debug("(#{inspect message.connection}) fin msg ID #{message.id}")
    :gen_tcp.send(message.socket, encode({:fin, message.id}))
    GenServer.call(message.consumer, {:start_stop_continue_backoff, :resume})
  end

  @doc """
  Tells NSQD to requeue the message, with delay and backoff. According to the
  go-nsq client (but doc'ed nowhere), a delay of -1 is a special value that
  means nsqd will calculate the duration itself based on the default requeue
  timeout. If backoff=true is set, then the connection will go into "backoff"
  mode, where it stops receiving messages for a fixed duration.
  """
  def req(message, delay \\ -1, backoff \\ false) do
    Logger.debug("(#{inspect message.connection}) requeue msg ID #{message.id}, delay #{delay}, backoff #{backoff}")
    :gen_tcp.send(message.socket, encode({:req, message.id, delay}))
    if backoff do
      GenServer.call(message.consumer, {:start_stop_continue_backoff, :backoff})
    else
      GenServer.call(message.consumer, {:start_stop_continue_backoff, :resume})
    end
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
  defp should_fail_message?(message) do
    message.config.max_attempts > 0 &&
      message.attempts > message.config.max_attempts
  end

  # TODO: Custom error logging/handling?
  defp log_failed_message(message) do
    Logger.warning("msg #{message.id} attempted #{message.attempts} times, giving up")
  end

  # Handler can be either an anonymous function or a module that implements the
  # `handle_message\2` function.
  defp run_handler(handler, message) do
    if is_function(handler) do
      handler.(message.body, message)
    else
      handler.handle_message(message.body, message)
    end
  end

  defp unlink_and_exit(pid) do
    Process.unlink(pid)
    Process.exit(pid, :normal)
  end
end
