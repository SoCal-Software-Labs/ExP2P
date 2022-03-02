defmodule ExP2P.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :ex_p2p,
      version: @version,
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      package: package(),
      docs: docs(),
      deps: deps(),
      source_url: "https://github.com/SoCal-Software-Labs/ExP2P"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp docs() do
    [
      extras: [
        LICENSE: [title: "License"],
        "README.md": [title: "Overview"]
      ],
      main: "readme",
      assets: "assets",
      canonical: "http://hexdocs.pm/ex_p2p",
      source_url: "https://github.com/SoCal-Software-Labs/ExP2P",
      source_ref: "v#{@version}",
      formatters: ["html"]
    ]
  end

  defp package() do
    [
      description: "P2P communication over Quic for Elixir using QP2P",
      files: [
        "lib",
        "native/exp2p/src",
        "native/exp2p/Cargo.toml",
        "LICENSE",
        "mix.exs"
      ],
      maintainers: ["Kyle Hanson"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/SoCal-Software-Labs/ExP2P"
      }
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:rustler, "~> 0.23.0"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
      # {:benchee, "~> 1.0"},
      # {:sorted_set_kv, ">= 0.0.0"}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
