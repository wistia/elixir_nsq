defmodule NSQ.Connection.Initializer do
  alias NSQ.Connection, as: C
  alias NSQ.Connection.MessageHandling
  alias NSQ.ConnInfo
  import NSQ.Protocol
  require Logger

  @socket_opts [as: :binary, active: false, deliver: :term, packet: :raw]

  @project ElixirNsq.Mixfile.project
  @user_agent "#{@project[:app]}/#{@project[:version]}"
  @ssl_versions [:sslv3, :tlsv1, :"tlsv1.1", :"tlsv1.2"] |> Enum.with_index


  @spec connect(%{nsqd: C.host_with_port}) :: {:ok, C.state} | {:error, String.t}
  def connect(%{nsqd: {host, port}} = state) do
    if should_connect?(state) do
      socket_opts = @socket_opts |> Keyword.merge(
        send_timeout: state.config.write_timeout,
        timeout: state.config.dial_timeout,
        cert: [path: state.config.tls_cert],
        key: [path: state.config.tls_key],
        versions: ssl_versions(state.config.tls_min_version),
        verify: false,
      )

      case Socket.TCP.connect(host, port, socket_opts) do
        {:ok, socket} ->
          state =
            %{state | socket: socket}
            |> do_handshake!
            |> start_receiving_messages!
            |> reset_connects
          {:ok, state}
        {:error, reason} ->
          if length(state.config.nsqlookupds) > 0 do
            Logger.warn "(#{inspect self}) connect failed; #{reason}; discovery loop should respawn"
            {{:error, reason}, %{state | connect_attempts: state.connect_attempts + 1}}
          else
            if state.config.max_reconnect_attempts > 0 do
              Logger.warn "(#{inspect self}) connect failed; #{reason}; discovery loop should respawn"
              {{:error, reason}, %{state | connect_attempts: state.connect_attempts + 1}}
            else
              Logger.error "(#{inspect self}) connect failed; #{reason}; reconnect turned off; terminating connection"
              Process.exit(self, :connect_failed)
            end
          end
      end
    else
      Logger.error "#{inspect self}: Failed to connect; terminating connection"
      Process.exit(self, :connect_failed)
    end
  end


  @doc """
  Immediately after connecting to the NSQ socket, both consumers and producers
  follow this protocol.
  """
  @spec do_handshake(C.state) :: {:ok, C.state}
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


  @spec send_magic_v2(C.state) :: :ok
  defp send_magic_v2(%{socket: socket}) do
    Logger.debug("(#{inspect self}) sending magic v2...")
    socket |> Socket.Stream.send!(encode(:magic_v2))
  end


  @spec identify(C.state) :: {:ok, binary}
  defp identify(conn_state) do
    Logger.debug("(#{inspect self}) identifying...")
    identify_obj = encode({:identify, identify_props(conn_state)})
    conn_state.socket |> Socket.Stream.send!(identify_obj)
    {:response, json} = recv_nsq_response(conn_state)
    {:ok, _conn_state} = update_from_identify_response(conn_state, json)
  end


  @spec identify_props(C.state) :: map
  defp identify_props(%{nsqd: {host, port}, config: config} = conn_state) do
    %{
      client_id: "#{host}:#{port} (#{inspect conn_state.parent})",
      hostname: to_string(:net_adm.localhost),
      feature_negotiation: true,
      heartbeat_interval: config.heartbeat_interval,
      output_buffer: config.output_buffer_size,
      output_buffer_timeout: config.output_buffer_timeout,
      tls_v1: true,
      snappy: false,
      deflate: false,
      sample_rate: 0,
      user_agent: config.user_agent || @user_agent,
      msg_timeout: config.msg_timeout
    }
  end


  @spec update_from_identify_response(C.state, binary) :: {:ok, C.state}
  defp update_from_identify_response(conn_state, json) do
    {:ok, parsed} = Poison.decode(json)

    # respect negotiated max_rdy_count
    if parsed["max_rdy_count"] do
      ConnInfo.update conn_state, %{max_rdy: parsed["max_rdy_count"]}
    end

    # respect negotiated msg_timeout
    if parsed["msg_timeout"] do
      conn_state = %{conn_state | msg_timeout: parsed["msg_timeout"]}
    else
      conn_state = %{conn_state | msg_timeout: conn_state.config.msg_timeout}
    end

    if parsed["tls_v1"] == true do
      socket = Socket.SSL.connect!(conn_state.socket)
      conn_state = %{conn_state | socket: socket}
      socket |> wait_for_ok(conn_state.config.read_timeout)
    end

    {:ok, conn_state}
  end


  @spec subscribe(C.state) :: {:ok, binary}
  defp subscribe(%{socket: socket, topic: topic, channel: channel} = conn_state) do
    Logger.debug "(#{inspect self}) subscribe to #{topic} #{channel}"
    socket |> Socket.Stream.send!(encode({:sub, topic, channel}))

    Logger.debug "(#{inspect self}) wait for subscription acknowledgment"
    socket |> wait_for_ok(conn_state.config.read_timeout)
  end


  @spec recv_nsq_response(C.state) :: {:response, binary}
  defp recv_nsq_response(conn_state) do
    {:ok, <<msg_size :: size(32)>>} =
      conn_state.socket |>
      Socket.Stream.recv(4, timeout: conn_state.config.read_timeout)

    {:ok, raw_msg_data} =
      conn_state.socket |>
      Socket.Stream.recv(msg_size, timeout: conn_state.config.read_timeout)

    {:response, _response} = decode(raw_msg_data)
  end


  defp wait_for_ok(socket, timeout) do
    expected = ok_msg
    {:ok, ^expected} =
      socket |> Socket.Stream.recv(byte_size(expected), timeout: timeout)
  end


  @spec ssl_versions(NSQ.Config.t) :: [atom]
  def ssl_versions(tls_min_version) do
    if tls_min_version do
      min_index = @ssl_versions[tls_min_version]
      @ssl_versions
      |> Enum.drop_while(fn({_, index}) -> index < min_index end)
      |> Enum.map(fn({version, _}) -> version end)
      |> Enum.reverse
    else
      @ssl_versions
      |> Enum.map(fn({version, _}) -> version end)
      |> Enum.reverse
    end
  end


  @spec should_connect?(C.state) :: boolean
  defp should_connect?(state) do
    state.connect_attempts == 0 ||
      state.connect_attempts <= state.config.max_reconnect_attempts
  end


  @spec start_receiving_messages(C.state) :: {:ok, C.state}
  defp start_receiving_messages(%{socket: socket} = state) do
    reader_pid = spawn_link(
      MessageHandling,
      :recv_nsq_messages,
      [socket, self, state.config.read_timeout]
    )
    state = %{state | reader_pid: reader_pid}
    GenServer.cast(self, :flush_cmd_queue)
    {:ok, state}
  end
  defp start_receiving_messages!(state) do
    {:ok, state} = start_receiving_messages(state)
    state
  end


  @spec reset_connects(C.state) :: C.state
  defp reset_connects(state), do: %{state | connect_attempts: 0}
end
