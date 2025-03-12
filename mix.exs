defmodule DarwinexTicks.MixProject do
  use Mix.Project

  def project do
    [
      app: :darwinex_ticks,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :ftp],
      mod: {DarwinexTicks.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:nimble_pool, "~> 1.1"},
      {:explorer, "~> 0.10"},
      {:nimble_parsec, "~> 1.4"}
    ]
  end
end
