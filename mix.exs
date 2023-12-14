defmodule ElixirNsq.Mixfile do
  use Mix.Project

  def project do
    [
      app: :elixir_nsq,
      version: "1.1.0",
      elixir: "~> 1.1",
      description: description(),
      package: package(),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [
      extra_applications: [:logger]
    ]
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
      {:elixir_uuid, "~> 1.2"},
      {:socket2, "~> 2.1"},
      {:jason, "~> 1.4"},
      {:httpoison, "~> 2.0"},

      # testing
      {:secure_random, "~> 0.5", only: :test},
      {:plug_cowboy, "~> 2.0", only: :test},
      {:plug, "~> 1.15", only: :test},
      {:ex_doc, ">= 0.0.0", only: :dev}
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
      }
    ]
  end
end
