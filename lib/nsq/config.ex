defmodule NSQ.Config do
  @ms 1
  @seconds 1000 * @ms
  @minutes 60 * @seconds

  @default_config %{
    dial_timeout: 1000,

    # Deadlines for network reads and writes
    read_timeout: 1 * @minutes,
    write_timeout: 1 * @seconds,

    # {host, port} tuples identifying where we should look for nsqd/nsqlookupd.
    nsqds: [],
    nsqlookupds: [],

    # Duration between polling lookupd for new producers, and fractional jitter
    # to add to the lookupd pool loop. this helps evenly distribute requests
    # even if multiple consumers restart at the same time
    #
    # NOTE: when not using nsqlookupd, lookupd_poll_interval represents the
    # duration of time between reconnection attempts
    lookupd_poll_interval: 1 * @minutes,
    lookupd_poll_jitter: 0.3,

    # If nsqlookupd is not being used and a connection to nsqd should fail,
    # it will automatically attempt to reconnect based on the
    # lookupd_poll_interval. This is how many times it will make the attempt
    # before erroring out.
    max_reconnect_attempts: 30,

    # Maximum duration when REQueueing (for doubling of deferred requeue)
    max_requeue_delay: 15 * @minutes,

    # Backoff strategy, defaults to exponential backoff. Overwrite this to
    # define alternative backoff algorithms
    backoff_strategy: :exponential,

    # Maximum amount of time to backoff when processing fails 0 == no backoff
    max_backoff_duration: 2 * @minutes,

    # Unit of time for calculating consumer backoff
    backoff_multiplier: 1 * @seconds,

    # Maximum number of times this consumer will attempt to process a message
    # before giving up
    max_attempts: 5,

    # Duration to wait for a message from a producer when in a state where RDY
    # counts are re-distributed (ie. max_in_flight < num_producers)
    low_rdy_idle_timeout: 10 * @seconds,

    # Duration between redistributing max-in-flight to connections
    rdy_redistribute_interval: 5 * @seconds,

    # Duration to wait to retry RDY, if conn is maxed out
    rdy_retry_delay: 5 * @seconds,

    # Identifiers sent to nsqd representing this client UserAgent is in the
    # spirit of HTTP (default: "<client_library_name>/<version>")
    client_id: nil,
    hostname: nil,
    user_agent: nil,

    # Duration of time between heartbeats. This must be less than read_timeout
    heartbeat_interval: 30 * @seconds,

    # Integer percentage 0-99 to sample the channel (requires nsqd 0.2.25+)
    sample_rate: 0,

    # To set TLS config, use the following options:
    #
    # tls_v1 - Bool enable TLS negotiation
    # tls_root_ca_file - String path to file containing root CA
    # tls_insecure_skip_verify - Bool indicates whether this client should verify server certificates
    # tls_cert - String path to file containing public key for certificate
    # tls_key - String path to file containing private key for certificate
    # tls_min_version - String indicating the minimum version of tls acceptable :sslv3, :tlsv1, :"tlsv1.1", :"tlsv1.2"
    tls_v1: false,
    tls_insecure_skip_verify: false,
    tls_cert: nil,
    tls_key: nil,
    tls_min_version: nil,

    # Compression settings
    deflate: false,
    deflate_level: 6,
    snappy: false,

    # Size of the buffer (in bytes) used by nsqd for buffering writes to this
    # connection
    output_buffer_size: 16384,

    # Timeout used by nsqd before flushing buffered writes (set to 0 to disable).
    #
    # WARNING: configuring clients with an extremely low
    # (< 25ms) output_buffer_timeout has a significant effect
    # on nsqd CPU usage (particularly with > 50 clients connected).
    output_buffer_timeout: 250 * @ms,

    # Maximum number of messages to allow in flight (concurrency knob)
    max_in_flight: 2500,

    # The server-side message timeout for messages delivered to this client.
    # After waiting this long without a TOUCH, NSQD will automatically requeue
    # the message.
    msg_timeout: 60 * @seconds,

    # secret for nsqd authentication (requires nsqd 0.2.29+)
    auth_secret: "",

    # function or module to deal with messages
    message_handler: nil,

    # define a custom event manager to get callbacks as to what NSQ is doing
    event_manager: nil
  }

  @valid_ranges %{
    read_timeout: {100 * @ms, 5 * @minutes},
    write_timeout: {100 * @ms, 5 * @minutes},
    lookupd_poll_interval: {10 * @ms, 5 * @minutes},
    lookupd_poll_jitter: {0, 1},
    max_requeue_delay: {0, :infinity},
    max_backoff_duration: {0, 60 * @minutes},
    backoff_multiplier: {0, 60 * @minutes},
    max_attempts: {0, 65535},
    low_rdy_idle_timeout: {1 * @seconds, 5 * @minutes},
    rdy_redistribute_interval: {1 * @ms, 5 * @seconds},
    sample_rate: {0, 99},
    deflate_level: {1, 9},
    max_in_flight: {0, :infinity},
    msg_timeout: {0, :infinity}
  }

  defstruct Enum.into(@default_config, [])

  @doc """
  Given a config, tell us what's wrong with it. If nothing is wrong, we'll
  return `{:ok, config}`.

  ## Examples

      iex> NSQ.Config.validate(%NSQ.Config{})
      {:ok, %NSQ.Config{}}

      iex> NSQ.Config.validate(%NSQ.Config{max_attempts: -1})
      {:error, ["max_attempts: -1 below minimum 0"]}
  """
  def validate(config) do
    errors = []

    %NSQ.Config{} = config

    errors =
      errors ++
        Enum.map(@valid_ranges, fn {name, {min, max}} ->
          case range_error(Map.get(config, name), min, max) do
            {:error, reason} -> "#{name}: #{reason}"
            :ok -> nil
          end
        end)

    errors = [
      no_match_error(
        config.backoff_strategy,
        [:exponential, :test]
      )
      | errors
    ]

    errors = Enum.reject(errors, fn v -> v == nil end)

    if length(errors) > 0 do
      {:error, errors}
    else
      {:ok, config}
    end
  end

  def normalize(config) do
    config = %NSQ.Config{config | nsqds: normalize_hosts(config.nsqds)}
    config = %NSQ.Config{config | nsqlookupds: normalize_hosts(config.nsqlookupds)}
    {:ok, config}
  end

  def normalize_hosts(hosts) do
    Enum.map(hosts, fn host_with_port ->
      cond do
        is_tuple(host_with_port) ->
          {_host, _port} = host_with_port

        is_binary(host_with_port) ->
          [host, port] = host_with_port |> String.split(":")
          {port, _} = Integer.parse(port)
          {host, port}

        is_list(host_with_port) ->
          {_host, _port} = List.to_tuple(host_with_port)

        true ->
          raise "Invalid host definition #{inspect(host_with_port)}"
      end
    end)
  end

  defp range_error(val, min, max) do
    cond do
      val == nil -> :ok
      val < min -> {:error, "#{val} below minimum #{min}"}
      max != :infinity && val > max -> {:error, "#{val} above maximum #{max}"}
      true -> :ok
    end
  end

  defp matches_any?(val, candidates) do
    Enum.any?(candidates, fn candidate -> candidate == val end)
  end

  defp no_match_error(val, candidates) do
    if matches_any?(val, candidates) do
      nil
    else
      {:error, "#{val} doesn't match any supported values"}
    end
  end
end
