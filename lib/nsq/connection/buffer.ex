defmodule NSQ.Connection.Buffer do
  require Logger
  use GenServer

  @initial_state %{
    buffered_data: "",
    buffered_data_size: 0,
    compressed_data: "",
    compression: :plaintext,
    socket: nil,
    zin: nil,
    zout: nil,
    timeout: nil,
  }

  def start_link(opts \\ []) do
    {:ok, _pid} = GenServer.start_link(__MODULE__, :ok, opts)
  end


  def init(:ok) do
    {:ok, @initial_state}
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
          state = %{state |
            compression: :deflate,
            zin: open_zin!,
            zout: open_zout!(level),
          }
          state |> convert_plaintext_buffer(:deflate)
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


  def recv(buffer, size) do
    buffer |> GenServer.call({:recv, size})
  end
  def recv!(buffer, size) do
    {:ok, data} = buffer |> recv(size)
    data
  end


  def send!(buffer, data) do
    :ok = buffer |> GenServer.call({:send, data})
  end


  defp do_send(state, data) do
    case state.compression do
      :plaintext ->
        state.socket |> Socket.Stream.send!(data)

      :deflate ->
        compressed =
          state.zout
          |> :zlib.deflate(data, :sync)
          |> List.flatten |> Enum.join("")

        state.socket |> Socket.Stream.send!(compressed)
    end
  end


  defp open_zin! do
    z = :zlib.open
    :ok = z |> :zlib.inflateInit(-15)
    z
  end


  defp open_zout!(level) do
    z = :zlib.open
    :ok = z |> :zlib.deflateInit(level, :deflated, -15, 8, :default)
    z
  end


  defp do_recv(state, size) do
    cond do
      state.buffered_data_size > 0 && state.buffered_data_size >= size ->
        {taken, leftover} = state.buffered_data |> String.split_at(size)
        state = %{state |
          buffered_data: leftover,
          buffered_data_size: byte_size(leftover)
        }
        {{:ok, taken}, state}
      true ->
        case state |> buffer do
          {:error, error} -> {{:error, error}, state}
          {:ok, state} -> state |> do_recv(size)
        end
    end
  end


  @doc """
  Grabs as much data from the socket as is available, combines it with any
  compressed data from previous buffering, decompresses it, and stores the
  output in several state properties.
  """
  defp buffer(state) do
    received =
      state.socket |> Socket.Stream.recv(0, timeout: state.timeout)

    case received do
      {:error, error} -> {:error, error}
      {:ok, raw_chunk} -> {:ok, state |> add_raw_chunk_to_buffer!(raw_chunk)}
    end
  end


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

    if is_list(decompressed) do
      decompressed = decompressed |> List.flatten |> Enum.join("")
    end

    combined_buffer = state.buffered_data <> decompressed

    %{state |
      buffered_data: combined_buffer,
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
