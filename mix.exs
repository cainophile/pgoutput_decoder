defmodule PgoutputDecoder.MixProject do
  use Mix.Project

  def project do
    [
      app: :pgoutput_decoder,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      source_url: "https://github.com/cainophile/pgoutput_decoder",
      package: [
        licenses: ["Apache-2.0"],
        links: %{"GitHub" => "https://github.com/cainophile/pgoutput_decoder"}
      ]
    ]
  end

  defp description do
    "Parses logical replication messages from Postgres pgoutput plugin"
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
