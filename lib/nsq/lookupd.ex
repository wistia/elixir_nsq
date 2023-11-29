defmodule NSQ.Lookupd do
  alias NSQ.Connection, as: C
  require Logger

  @typedoc """
  All lookupd responses should return a map with these values. If the response
  is not of that form, it should be normalized into that form.
  """
  @type response :: %{
          data: binary,
          headers: [any],
          status_code: integer,
          status_txt: binary
        }

  @spec nsqds_with_topic([C.host_with_port()], String.t()) :: [C.host_with_port()]
  def nsqds_with_topic(lookupds, topic) do
    responses = Enum.map(lookupds, &topics_from_lookupd(&1, topic))

    nsqds =
      Enum.map(responses, fn response ->
        Enum.map(response["producers"] || [], fn producer ->
          if producer do
            {producer["broadcast_address"], producer["tcp_port"]}
          else
            nil
          end
        end)
      end)

    nsqds
    |> List.flatten()
    |> Enum.uniq()
    |> Enum.reject(&is_nil/1)
  end

  @spec topics_from_lookupd(C.host_with_port(), String.t()) :: response
  def topics_from_lookupd({host, port}, topic) do
    lookupd_url = "http://#{host}:#{port}/lookup?topic=#{topic}"
    headers = [{"Accept", "application/vnd.nsq; version=1.0"}]

    case Tesla.get(lookupd_url, headers: headers) do
      {:ok, response} ->
        case response.status do
          200 ->
            normalize_200_response(response.headers, response.body)
          404 -> %{} |> normalize_response
          _ ->
            Logger.error("Unexpected status code from #{lookupd_url}: #{response.status}")
            %{status_code: response.status, data: response.body} |> normalize_response
        end
      {:error, reason} ->
        Logger.error("Error connecting to #{lookupd_url}: #{inspect(reason)}")
        normalize_response(%{})
    end
  end

  @spec normalize_200_response([any], binary) :: response
  defp normalize_200_response(headers, body) do
    body = if body == nil || body == "", do: "{}", else: body

    if headers[:"X-Nsq-Content-Type"] == "nsq; version=1.0" do
      Poison.decode!(body)
      |> normalize_response
    else
      %{status_code: 200, status_txt: "OK", data: body}
      |> normalize_response
    end
  end

  @spec normalize_response(map) :: response
  defp normalize_response(m) do
    Map.merge(
      %{
        status_code: nil,
        status_txt: "",
        data: "",
        headers: []
      },
      m
    )
  end
end
