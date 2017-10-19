defmodule ElixirNsq.Mixfile do
  use Mix.Project

  def project do
    [app: :elixir_nsq,
     version: "1.0.3",
     elixir: "~> 1.3",
     description: description(),
     package: package(),
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger, :httpotion, :poison, :socket]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      {:poison, "~> 3.1.0"},
      {:httpotion, "~> 3.0"},
      {:uuid, "~> 1.1.7"},
      {:socket, "~> 0.3.11"},

      # testing
      {:secure_random, "~> 0.5", only: :test},

      # Small HTTP server for running tests
      {:http_server, github: "parroty/http_server"},
    ]
  end

  defp description do
    """
    A client library for NSQ, `elixir_nsq` aims to be complete, easy to use,
    and well tested. Developed at Wistia (http://wistia.com).
    """
  end

  defp package do
    [
      files: ["lib", "mix.exs", "README*", "readme*", "LICENSE*", "license*"],
      maintainers: ["Max Schnur (max@wistia.com)"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/wistia/elixir_nsq"
      },
    ]
  end
end
