defmodule NSQ.Message do
  # ------------------------------------------------------- #
  # Directives                                              #
  # ------------------------------------------------------- #
  require Logger
  use GenServer
  import NSQ.Protocol

  # ------------------------------------------------------- #
  # Struct Definition                                       #
  # ------------------------------------------------------- #
  defstruct [:id, :timestamp, :attempts, :body, :connection, :socket, :config]

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
  Tell the message supervisor for this connection to start a `process` task for
  this message.
  """
  def process_async(conn, message, conn_state \\ nil) do
    conn_state = conn_state || NSQ.Connection.get_state(conn_state)
    NSQ.MessageSupervisor.start_child(
      conn_state.msg_sup_pid, conn, message, conn_state
    )
  end

  @doc """
  This is the main entry point when processing a message. It starts the message
  GenServer and immediately kicks of a processing call.
  """
  def process(conn, config, socket, message) do
    message = %NSQ.Message{message |
      connection: conn,
      config: config,
      socket: socket
    }

    if should_fail_message?(message) do
      log_failed_message(message)
      fin(message)
      end_processing(message)
    else
      result = try do
        run_handler(config.message_handler, message)
      catch
        true -> {:req, -1}
      end

      case result do
        :ok -> fin(message)
        :req -> req(message)
        {:req, delay} -> req(message, delay)
      end

      end_processing(message)
    end
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

  defp end_processing(message) do
    GenServer.call(message.connection, {:message_done, message})
    Process.exit(self, :normal)
  end
end
