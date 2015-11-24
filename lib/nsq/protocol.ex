defmodule NSQ.Protocol do
  @valid_topic_channel_name_regex ~r/^[\.a-zA-Z0-9_-]+(#ephemeral)?$/
  @frame_type_response <<0 :: size(32)>>
  @frame_type_error <<1 :: size(32)>>
  @frame_type_message <<2 :: size(32)>>


  def encode(cmd) do
    case cmd do
      :magic_v2 -> "  V2"
      :noop -> "NOP\n"
      {:identify, options} ->
        json = Poison.encode!(options)
        "IDENTIFY\n" <> <<byte_size(json) :: size(32)>> <> json
      {:pub, topic, data} ->
        "PUB #{topic}\n" <> << byte_size(data) :: size(32) >> <> data
      {:sub, topic, channel} -> "SUB #{topic} #{channel}\n"
      {:fin, msg_id} -> "FIN #{msg_id}\n"
      {:req, msg_id, delay} -> "REQ #{msg_id} #{delay}\n"
      {:rdy, count} -> "RDY #{count}\n"
      {:touch, msg_id} -> "TOUCH #{msg_id}\n"
      :cls -> "CLS\n"
    end
  end


  def decode(msg) do
    case msg do
      <<0, 0, 0, 6, @frame_type_response, "OK">> ->
        {:response, "OK"}
      <<@frame_type_response, data::binary>> ->
        {:response, data}
      <<@frame_type_error, data::binary>> ->
        {:error, data}
      <<@frame_type_message, data::binary>> ->
        {:message, data}
      <<frame_type :: size(32), data::binary>> ->
        {:error, "Unknown frame type #{frame_type}", data}
    end
  end


  @spec decode_as_message(binary) :: {atom, Map.t} | {atom, String.t}
  def decode_as_message(data) do
    case data do
      <<timestamp :: size(64), attempts :: size(16), msg_id :: size(128), rest::binary>> ->
        {:ok, %{
          id: <<msg_id :: size(128)>>,
          timestamp: timestamp,
          attempts: attempts,
          body: rest
        }}
      _else ->
        {:error, "Data did not match expected message format"}
    end
  end


  @doc """
  If MPUB is used, a single TCP response can have data for several messages.
  In that case, the given data follows the same format, but it means we need
  to be sensitive to the given message sizes.

      iex> raw_data = <<0, 0, 0, 42, 0, 0, 0, 2, 20, 24, 221, 165, 125, 107, 65, 242, 0, 1>> <> "093cac1231a00224HTTP message" <> <<0, 0, 0, 42, 0, 0, 0, 2, 20, 24, 221, 165, 125, 107, 68, 234, 0, 1>> <> "093cac1231a00225HTTP message"
      ...> NSQ.Protocol.messages_from_data(raw_data)
      [<<0, 0, 0, 2, 20, 24, 221, 165, 125, 107, 65, 242, 0, 1>> <> "093cac1231a00224HTTP message", <<0, 0, 0, 2, 20, 24, 221, 165, 125, 107, 68, 234, 0, 1>> <> "093cac1231a00225HTTP message"]
  """
  def messages_from_data(data) do
    messages_from_data(data, [])
  end


  defp messages_from_data(data, acc) do
    <<msg_size :: size(32)>> <> rest = data
    msg = binary_part(rest, 0, msg_size)
    blob_after_msg = binary_part(rest, byte_size(msg), byte_size(rest) - byte_size(msg))
    if byte_size(blob_after_msg) > 0 do
      messages_from_data(blob_after_msg, [msg | acc])
    else
      Enum.reverse([msg | acc])
    end
  end


  def response_msg(body) do
    data = @frame_type_response <> body
    <<byte_size(data) :: size(32)>> <> @frame_type_response <> body
  end


  def ok_msg do
    response_msg("OK")
  end


  def is_valid_topic_name?(topic) do
    is_valid_name?(topic)
  end


  def is_valid_channel_name?(topic) do
    is_valid_name?(topic)
  end


  defp is_valid_name?(name) do
    len = String.length(name)
    len > 0 && len <= 64 &&
      Regex.match?(@valid_topic_channel_name_regex, name)
  end
end
