defmodule NSQ.Connection.Initializer do
  alias NSQ.Connection, as: C
  alias NSQ.Connection.MessageHandling
  alias NSQ.Connection.Buffer
  alias NSQ.ConnInfo
  import NSQ.Protocol
  require Logger

  @socket_opts [as: :binary, mode: :passive, packet: :raw]

  @project ElixirNsq.Mixfile.project()
  @user_agent "#{@project[:app]}/#{@project[:version]}"
  @ssl_versions [:sslv3, :tlsv1, :"tlsv1.1", :"tlsv1.2"] |> Enum.with_index()

  @spec connect(%{nsqd: C.host_with_port()}) :: {:ok, C.state()} | {:error, String.t()}
  def connect(%{nsqd: {host, port}} = state) do
    if should_connect?(state) do
      socket_opts =
        @socket_opts
        |> Keyword.merge(
          send: [{:timeout, state.config.write_timeout}],
          timeout: state.config.dial_timeout
        )

      case Socket.TCP.connect(host, port, socket_opts) do
        {:ok, socket} ->
          state.reader |> Buffer.setup_socket(socket, state.config.read_timeout)
          state.writer |> Buffer.setup_socket(socket, state.config.read_timeout)

          state =
            %{state | socket: socket}
            |> do_handshake!
            |> start_receiving_messages!
            |> reset_connects

          {:ok, %{state | connected: true}}

        {:error, reason} ->
          if length(state.config.nsqlookupds) > 0 do
            Logger.warn(
              "(#{inspect(self())}) connect failed; #{reason}; discovery loop should respawn"
            )

            {{:error, reason}, %{state | connect_attempts: state.connect_attempts + 1}}
          else
            if state.config.max_reconnect_attempts > 0 do
              Logger.warn(
                "(#{inspect(self())}) connect failed; #{reason}; discovery loop should respawn"
              )

              {{:error, reason}, %{state | connect_attempts: state.connect_attempts + 1}}
            else
              Logger.error(
                "(#{inspect(self())}) connect failed; #{reason}; reconnect turned off; terminating connection"
              )

              Process.exit(self(), :connect_failed)
            end
          end
      end
    else
      Logger.error("#{inspect(self())}: Failed to connect; terminating connection")
      Process.exit(self(), :connect_failed)
    end
  end

  @doc """
  Immediately after connecting to the NSQ socket, both consumers and producers
  follow this protocol.
  """
  @spec do_handshake(C.state()) :: {:ok, C.state()}
  def do_handshake(conn_state) do
    conn_state |> send_magic_v2

    {:ok, conn_state} = identify(conn_state)

    # Producers don't have a channel, so they won't do this.
    if conn_state.channel do
      subscribe(conn_state)
    end

    {:ok, conn_state}
  end

  def do_handshake!(conn_state) do
    {:ok, conn_state} = do_handshake(conn_state)
    conn_state
  end

  @spec send_magic_v2(C.state()) :: :ok
  defp send_magic_v2(conn_state) do
    Logger.debug("(#{inspect(self())}) sending magic v2...")
    conn_state |> Buffer.send!(encode(:magic_v2))
  end

  @spec identify(C.state()) :: {:ok, binary}
  defp identify(conn_state) do
    Logger.debug("(#{inspect(self())}) identifying...")
    identify_obj = encode({:identify, identify_props(conn_state)})
    conn_state |> Buffer.send!(identify_obj)
    {:response, json} = recv_nsq_response(conn_state)
    {:ok, _conn_state} = update_from_identify_response(conn_state, json)
  end

  @spec identify_props(C.state()) :: map
  defp identify_props(%{nsqd: {host, port}, config: config} = conn_state) do
    %{
      client_id: "#{host}:#{port} (#{inspect(conn_state.parent)})",
      hostname: to_string(:net_adm.localhost()),
      feature_negotiation: true,
      heartbeat_interval: config.heartbeat_interval,
      output_buffer: config.output_buffer_size,
      output_buffer_timeout: config.output_buffer_timeout,
      tls_v1: config.tls_v1,
      snappy: false,
      deflate: config.deflate,
      deflate_level: config.deflate_level,
      sample_rate: 0,
      user_agent: config.user_agent || @user_agent,
      msg_timeout: config.msg_timeout
    }
  end

  def inflate(data) do
    z = :zlib.open()
    :ok = z |> :zlib.inflateInit(-15)
    inflated = z |> :zlib.inflateChunk(data)
    Logger.warn("inflated chunk?")
    Logger.warn(inspect(inflated))
    :ok = z |> :zlib.inflateEnd()
    :ok = z |> :zlib.close()
    inflated
  end

  @spec update_from_identify_response(C.state(), binary) :: {:ok, C.state()}
  defp update_from_identify_response(conn_state, json) do
    {:ok, parsed} = Poison.decode(json)

    # respect negotiated max_rdy_count
    if parsed["max_rdy_count"] do
      ConnInfo.update(conn_state, %{max_rdy: parsed["max_rdy_count"]})
    end

    # respect negotiated msg_timeout
    timeout = parsed["msg_timeout"] || conn_state.config.msg_timeout
    conn_state = %{conn_state | msg_timeout: timeout}

    # wrap our socket with SSL if TLS is enabled
    conn_state =
      if parsed["tls_v1"] == true do
        Logger.debug("Upgrading to TLS...")

        socket =
          Socket.SSL.connect!(conn_state.socket,
            cacertfile: conn_state.config.tls_cert,
            keyfile: conn_state.config.tls_key,
            versions: ssl_versions(conn_state.config.tls_min_version),
            verify: ssl_verify_atom(conn_state.config)
          )

        conn_state = %{conn_state | socket: socket}
        conn_state.reader |> Buffer.setup_socket(socket, conn_state.config.read_timeout)
        conn_state.writer |> Buffer.setup_socket(socket, conn_state.config.read_timeout)
        conn_state |> wait_for_ok!
        conn_state
      else
        conn_state
      end

    # If compression is enabled, we expect to receive a compressed "OK"
    # immediately.
    conn_state.reader |> Buffer.setup_compression(parsed, conn_state.config)
    conn_state.writer |> Buffer.setup_compression(parsed, conn_state.config)

    if parsed["deflate"] == true || parsed["snappy"] == true do
      conn_state |> wait_for_ok!
    end

    if parsed["auth_required"] == true do
      Logger.debug("sending AUTH")
      auth_cmd = encode({:auth, conn_state.config.auth_secret})
      conn_state |> Buffer.send!(auth_cmd)
      {:response, json} = recv_nsq_response(conn_state)
      Logger.debug(json)
    end

    {:ok, conn_state}
  end

  defp ssl_verify_atom(config) do
    if config.tls_insecure_skip_verify == true do
      :verify_none
    else
      :verify_peer
    end
  end

  @spec subscribe(C.state()) :: {:ok, binary}
  defp subscribe(%{topic: topic, channel: channel} = conn_state) do
    Logger.debug("(#{inspect(self())}) subscribe to #{topic} #{channel}")
    conn_state |> Buffer.send!(encode({:sub, topic, channel}))

    Logger.debug("(#{inspect(self())}) wait for subscription acknowledgment")
    conn_state |> wait_for_ok!
  end

  @spec recv_nsq_response(C.state()) :: {:response, binary}
  defp recv_nsq_response(conn_state) do
    <<msg_size::size(32)>> = conn_state |> Buffer.recv!(4)
    raw_msg_data = conn_state |> Buffer.recv!(msg_size)
    {:response, _response} = decode(raw_msg_data)
  end

  defp wait_for_ok!(state) do
    expected = ok_msg()
    ^expected = state |> Buffer.recv!(byte_size(expected))
  end

  @spec ssl_versions(NSQ.Config.t()) :: [atom]
  def ssl_versions(tls_min_version) do
    if tls_min_version do
      min_index = @ssl_versions[tls_min_version]

      @ssl_versions
      |> Enum.drop_while(fn {_, index} -> index < min_index end)
      |> Enum.map(fn {version, _} -> version end)
      |> Enum.reverse()
    else
      @ssl_versions
      |> Enum.map(fn {version, _} -> version end)
      |> Enum.reverse()
    end
  end

  @spec should_connect?(C.state()) :: boolean
  defp should_connect?(state) do
    state.connect_attempts == 0 ||
      state.connect_attempts <= state.config.max_reconnect_attempts
  end

  @spec start_receiving_messages(C.state()) :: {:ok, C.state()}
  defp start_receiving_messages(state) do
    reader_pid = spawn_link(MessageHandling, :recv_nsq_messages, [state, self()])
    state = %{state | reader_pid: reader_pid}
    GenServer.cast(self(), :flush_cmd_queue)
    {:ok, state}
  end

  defp start_receiving_messages!(state) do
    {:ok, state} = start_receiving_messages(state)
    state
  end

  @spec reset_connects(C.state()) :: C.state()
  defp reset_connects(state), do: %{state | connect_attempts: 0}
end
