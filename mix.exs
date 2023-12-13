defmodule ElixirNsq.Mixfile do
  use Mix.Project

  def project do
    [app: :elixir_nsq,
     version: "1.1.0",
     elixir: "~> 1.1",
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
    [
      applications: [:logger, :httpotion, :poison, :socket2, :elixir_uuid],
      extra_applications: extra_applications(Mix.env())
    ]
  end

  defp extra_applications(:test), do: [:secure_random, :plug, :ranch, :plug_cowboy]
  defp extra_applications(_), do: []

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
      {:poison, "~> 4.0"},
      {:httpotion, "~> 3.2"},
      {:elixir_uuid, "~> 1.2"},
      {:socket2, "~> 2.1"},

      # testing
      {:secure_random, "~> 0.5", only: :test},
      {:plug_cowboy, "~> 2.0", only: :test},
      {:plug, "~> 1.15", only: :test },

      {:ex_doc, ">= 0.0.0", only: :dev},
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
      files: ["lib", "mix.exs", "README*", "LICENSE*"],
      maintainers: ["Max Schnur (max@wistia.com)"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/wistia/elixir_nsq"
      },
    ]
  end
end
