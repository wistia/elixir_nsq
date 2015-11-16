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
        <<"IDENTIFY\n", String.length(json) :: size(32), json>>
      {:sub, topic, channel} -> "SUB #{topic} #{channel}\n"
      {:fin, msg_id} -> "FIN #{msg_id}\n"
      {:req, msg_id, delay} -> "REQ #{msg_id} #{delay}\n"
      {:rdy, count} -> "RDY #{count}\n"
    end
  end


  def decode(msg) do
    case msg do
      <<_frame_size :: size(32), @frame_type_response, data::binary>> ->
        {:response, data}
      <<_frame_size :: size(32), @frame_type_error, data::binary>> ->
        {:error, data}
      <<_frame_size :: size(32), @frame_type_message, data::binary>> ->
        {:message, data}
      <<_frame_size :: size(32), frame_type, data::binary>> ->
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
          data: rest
        }}
      _else ->
        {:error, "Data did not match expected message format"}
    end
  end


  def ok_msg?(msg) do
    decode(msg) == {:response, "OK"}
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
