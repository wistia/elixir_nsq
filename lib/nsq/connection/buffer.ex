defmodule NSQ.Connection.Buffer do
  require Logger
  use GenServer

  @initial_state %{
    buffered_data: "",
    buffered_data_size: 0,
    compressed_data: "",
    compression: :plaintext,
    socket: nil,
    timeout: nil,
    type: nil,
    zin: nil,
    zout: nil
  }

  def start_link(type, opts \\ []) do
    {:ok, _pid} = GenServer.start_link(__MODULE__, type, opts)
  end

  def init(type) do
    {:ok, %{@initial_state | type: type}}
  end

  def handle_call({:setup_socket, socket, timeout}, _from, state) do
    {:reply, :ok, %{state | socket: socket, timeout: timeout}}
  end

  def handle_call({:setup_compression, compression}, _from, state) do
    state =
      case compression do
        :plaintext ->
          Logger.debug("Not compressing or decompressing data")
          %{state | compression: :plaintext}

        {:deflate, level} ->
          Logger.debug("Using DEFLATE level #{level} to compress and decompress data")
          state = %{state | compression: :deflate}

          case state.type do
            :reader ->
              %{state | zin: open_zin!()} |> convert_plaintext_buffer(:deflate)

            :writer ->
              %{state | zout: open_zout!(level)}
          end

        :snappy ->
          raise "snappy isn't implemented yet!"
      end

    {:reply, :ok, state}
  end

  def handle_call({:recv, size}, _from, state) do
    {result, state} = state |> do_recv(size)
    {:reply, result, state}
  end

  def handle_call({:send, data}, _from, state) do
    {:reply, state |> do_send(data), state}
  end

  def setup_socket(buffer, socket, timeout) do
    :ok = buffer |> GenServer.call({:setup_socket, socket, timeout})
  end

  def setup_compression(buffer, identify_response, config) do
    case compression_from_identify_response(identify_response, config) do
      {:ok, {:deflate, level}} ->
        :ok = buffer |> GenServer.call({:setup_compression, {:deflate, level}})

      {:ok, :plaintext} ->
        :ok = buffer |> GenServer.call({:setup_compression, :plaintext})
    end
  end

  def recv(%{reader: buffer, config: %NSQ.Config{read_timeout: timeout}}, size) do
    buffer |> recv(size, timeout)
  end

  def recv(buffer, size, timeout) do
    buffer |> GenServer.call({:recv, size}, timeout)
  end

  def recv!(state, size) do
    {:ok, data} = state |> recv(size)
    data
  end

  def recv!(buffer, size, timeout) do
    {:ok, data} = buffer |> recv(size, timeout)
    data
  end

  def send!(%{writer: buffer}, data) do
    buffer |> send!(data)
  end

  def send!(buffer, data) do
    :ok = buffer |> GenServer.call({:send, data})
  end

  defp do_send(state, data) do
    case state.compression do
      :plaintext ->
        state.socket |> Socket.Stream.send!(data)

      :deflate ->
        compressed = state.zout |> :zlib.deflate(data, :sync) |> List.flatten() |> Enum.join("")

        state.socket |> Socket.Stream.send!(compressed)
    end
  end

  defp open_zin! do
    z = :zlib.open()
    :ok = z |> :zlib.inflateInit(-15)
    z
  end

  defp open_zout!(level) do
    z = :zlib.open()
    :ok = z |> :zlib.deflateInit(level, :deflated, -15, 8, :default)
    z
  end

  # This tries to copy the semantics of a standard socket recv. That is, if you
  # give it a size, it will block until we have enough data then return it. If
  # you pass 0 as the size, we'll block until there's X (i.e. an undefined
  # amount > 0) data and return it.
  defp do_recv(state, size) do
    cond do
      state.buffered_data_size > 0 && state.buffered_data_size >= size ->
        <<taken::binary-size(size)>> <> leftover = state.buffered_data
        state = %{state | buffered_data: leftover, buffered_data_size: byte_size(leftover)}
        {{:ok, taken}, state}

      true ->
        case state |> buffer do
          {:error, error} -> {{:error, error}, state}
          {:ok, state} -> state |> do_recv(size)
        end
    end
  end

  # Grabs as much data from the socket as is available, combines it with any
  # compressed data from previous buffering, decompresses it, and stores the
  # output in several state properties.
  defp buffer(state) do
    received = state.socket |> Socket.Stream.recv(0, timeout: state.timeout)

    case received do
      {:error, error} -> {:error, error}
      {:ok, nil} -> {:error, "socket closed"}
      {:ok, raw_chunk} -> {:ok, state |> add_raw_chunk_to_buffer!(raw_chunk)}
    end
  end

  # During initialization, it's highly possible that NSQ sends us compressed
  # messages that are buffered before we start decompressing on the fly. This
  # assumes all messages in the buffer are compressed and decompresses them.
  defp convert_plaintext_buffer(state, compressor) do
    case compressor do
      :deflate ->
        plaintext_data = state.buffered_data

        state
        |> reset_buffer!
        |> add_raw_chunk_to_buffer!(plaintext_data)

      :snappy ->
        raise "Snappy isn't implemented yet!"
    end
  end

  defp reset_buffer!(state) do
    %{state | buffered_data: "", buffered_data_size: 0, compressed_data: ""}
  end

  # Given a chunk of data, decompress as much as we can and store it in its
  # appropriate place in the buffer.
  defp add_raw_chunk_to_buffer!(state, raw_chunk) do
    raw_chunk = state.compressed_data <> raw_chunk

    {decompressed, compressed} =
      case state.compression do
        :plaintext ->
          {raw_chunk, ""}

        :deflate ->
          case state.zin |> :zlib.inflateChunk(raw_chunk) do
            {more, decompressed} ->
              {decompressed, more}

            decompressed ->
              {decompressed, ""}
          end
      end

    decompressed =
      if is_list(decompressed) do
        decompressed |> List.flatten() |> Enum.join("")
      else
        decompressed
      end

    combined_buffer = state.buffered_data <> decompressed

    %{
      state
      | buffered_data: combined_buffer,
        buffered_data_size: byte_size(combined_buffer),
        compressed_data: compressed
    }
  end

  defp compression_from_identify_response(response, config) do
    cond do
      response["snappy"] == true ->
        {:error, "snappy isn't implemented yet!"}

      response["deflate"] == true ->
        {:ok, {:deflate, config.deflate_level}}

      true ->
        {:ok, :plaintext}
    end
  end
end
