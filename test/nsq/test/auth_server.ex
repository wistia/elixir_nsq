defmodule NSQ.Test.AuthServer do
  defmodule NSQ.Test.Router do
    use Plug.Router

    plug(:match)
    plug(:dispatch)

    get "/auth" do
      json_response = %{
        ttl: 3600,
        identity: "johndoe",
        identity_url: "http://127.0.0.1",
        authorizations: [
          %{
            permissions: ["subscribe", "publish"],
            topic: ".*",
            channels: [".*"]
          }
        ]
      }

      send_resp(conn, 200, Jason.encode!(json_response))
    end

    match _ do
      send_resp(conn, 404, "Not Found")
    end
  end

  def start(port) do
    [:telemetry] |> Enum.each(&Application.start/1)
    Plug.Cowboy.http(NSQ.Test.Router, [], port: port)
  end
end
