defmodule DarwinexTicks.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    pool_name = DarwinexTicks.FTPPool

    pool_opts = [
      worker: {DarwinexTicks.FTPPool, Application.get_env(:darwinex_ticks, :ftp)},
      pool_size: Application.get_env(:darwinex_ticks, :pool_size),
      lazy: true,
      worker_idle_timeout: :timer.minutes(1),
      name: pool_name
    ]

    children = [
      {NimblePool, pool_opts},
      {DarwinexTicks.Cache, pool_name: pool_name}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: DarwinexTicks.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
