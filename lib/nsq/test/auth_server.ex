defmodule NSQ.Test.AuthServer do
  # This sets up an auth server for NSQD (run from Procfile) so that we can
  # test auth properly.
  def start(port) do
    [:ranch, :cowlib, :cowboy, :http_server] |> Enum.each(&Application.start/1)

    HttpServer.start(
      path: "/auth",
      port: port,
      response:
        Poison.encode!(%{
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
        })
    )
  end
end
