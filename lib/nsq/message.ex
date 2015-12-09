defmodule NSQ.Message do
  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  require Logger
  import NSQ.Protocol

  # ------------------------------------------------------- #
  # Struct Definition                                       #
  # ------------------------------------------------------- #
  defstruct [
    :id,
    :timestamp,
    :attempts,
    :body,
    :connection,
    :consumer,
    :socket,
    :config,
    :processing_pid,
    :event_manager_pid,
    :msg_timeout
  ]

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
    message = %{message | processing_pid: pid}

    wait_for_msg_done(message)

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
      rescue
        e ->
          Logger.error "Error running message handler: #{inspect e}"
          {:req, -1, true}
      end

      case result do
        :ok -> fin(message)
        :req -> req(message)
        {:req, delay} -> req(message, delay)
        {:req, delay, backoff} -> req(message, delay, backoff)
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
    GenEvent.notify(message.event_manager_pid, {:message_finished, message})
    GenServer.call(message.consumer, {:start_stop_continue_backoff, :resume})
    GenEvent.notify(message.event_manager_pid, :resume)
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
    if delay == -1 do
      delay = calculate_delay(
        message.attempts, message.config.max_requeue_delay
      )
    end
    :gen_tcp.send(message.socket, encode({:req, message.id, delay}))
    GenEvent.notify(message.event_manager_pid, {:message_requeued, message})
    if backoff do
      GenServer.call(message.consumer, {:start_stop_continue_backoff, :backoff})
      GenEvent.notify(message.event_manager_pid, :backoff)
    else
      GenServer.call(message.consumer, {:start_stop_continue_backoff, :continue})
      GenEvent.notify(message.event_manager_pid, :continue)
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
    Logger.warn("msg #{message.id} attempted #{message.attempts} times, giving up")
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

  # We expect our function will send us a message when it's done. Block until
  # that happens. If it takes too long, requeue the message and cancel
  # processing.
  defp wait_for_msg_done(message) do
    receive do
      {:message_done, _msg} ->
        unlink_and_exit(message.processing_pid)
      {:message_touch, _msg} ->
        # If NSQ.Message.touch(msg) is called, we will send TOUCH msg_id to
        # NSQD, but we also need to reset our timeout on the client to avoid
        # processes that hang forever.
        wait_for_msg_done(message)
    after
      message.msg_timeout ->
        # If we've waited this long, we can assume NSQD will requeue the
        # message on its own.
        unlink_and_exit(message.processing_pid)
    end
  end


  defp unlink_and_exit(pid) do
    Process.unlink(pid)
    Process.exit(pid, :normal)
  end

  defp calculate_delay(attempts, max_requeue_delay) do
    exponential_backoff = :math.pow(2, attempts) * 1000
    jitter = round(0.3 * :random.uniform * exponential_backoff)
    min(
      exponential_backoff + jitter,
      max_requeue_delay
    ) |> round
  end
end
