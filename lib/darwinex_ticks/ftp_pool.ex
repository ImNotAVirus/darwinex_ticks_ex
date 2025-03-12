defmodule DarwinexTicks.FTPPool do
  @moduledoc false

  require Logger

  alias DarwinexTicks.FTP

  @behaviour NimblePool

  ## Public API

  def run(pool, fun) do
    NimblePool.checkout!(
      pool,
      :checkout,
      fn _from, pid ->
        ref = make_ref()
        send(pid, {:"$run", {self(), ref}, fun})

        receive do
          {:"$result", ^ref, value} ->
            {value, :ok}
        end
      end
    )
  end

  ## NimblePool behaviour

  @impl true
  def init_worker(pool_state) do
    host = Keyword.get(pool_state, :host, "tickdata.darwinex.com")
    port = Keyword.get(pool_state, :port, 21)
    username = Keyword.fetch!(pool_state, :username)
    password = Keyword.fetch!(pool_state, :password)

    pid =
      spawn_link(fn ->
        {:ok, conn} = FTP.connect(host, port, username, password)
        ftp_loop(conn)
      end)

    Logger.debug("#{inspect(pid)} started")

    {:ok, pid, pool_state}
  end

  @impl true
  def handle_checkout(:checkout, _from, pid, pool_state) do
    {:ok, pid, pid, pool_state}
  end

  @impl true
  def handle_checkin(:ok, _from, pid, pool_state) do
    {:ok, pid, pool_state}
  end

  @impl true
  def terminate_worker(_reason, pid, pool_state) do
    Logger.debug("Terminate FTP worker #{inspect(pid)}")
    send(pid, :"$stop")
    {:ok, pool_state}
  end

  @impl true
  def handle_ping(pid, _pool_state) do
    Logger.debug("#{inspect(pid)} now inactive, remove it")
    {:remove, :normal}
  end

  ## Private functions

  defp ftp_loop(conn) do
    receive do
      {:"$run", {pid, ref}, fun} ->
        send(pid, {:"$result", ref, fun.(conn)})
        ftp_loop(conn)

      :"$stop" ->
        :ok = FTP.close(conn)
    end
  end
end
