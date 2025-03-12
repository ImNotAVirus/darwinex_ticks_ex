defmodule DarwinexTicks.Cache do
  @moduledoc false

  use GenServer

  require Logger

  alias DarwinexTicks.{FTP, FTPPool}
  alias DarwinexTicks.LsParser

  @default_timeout :timer.seconds(5)
  @expire_after :timer.hours(1)
  @asset_delimitor "_"

  @dialyzer {:no_improper_lists, ets_prefix: 1}

  ## Public API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def wait_for_cache(timeout \\ @default_timeout) do
    do_wait_for_cache(now() + timeout)
  end

  def list_assets(timeout \\ @default_timeout) do
    with :ok <- wait_for_cache(timeout) do
      assets_table()
      |> :ets.match({:"$1", :_})
      |> List.flatten()
      |> then(&{:ok, &1})
    end
  end

  def list_files(asset, filters \\ [], timeout \\ :timer.minutes(1)) do
    start = now()

    with :ok <- wait_for_cache(timeout),
         :ok <- update_asset_files(asset, start + timeout - now()) do
      {:ok, list_files_from_table(asset, filters)}
    end
  end

  ## GenServer behaviour

  @impl true
  def init(opts) do
    # {asset_id, updated_at}
    :ets.new(assets_table(), [
      :set,
      :protected,
      :named_table,
      read_concurrency: true,
      write_concurrency: :auto
    ])

    # {filename_pattern, filename, bid_or_adk, timestamp}
    :ets.new(asset_files_table(), [
      :ordered_set,
      :protected,
      :named_table,
      read_concurrency: true,
      write_concurrency: :auto
    ])

    state = %{pool: Keyword.fetch!(opts, :pool_name)}

    {:ok, state, {:continue, :update_assets}}
  end

  @impl true
  def handle_continue(:update_assets, %{pool: pool} = state) do
    Logger.debug("starting files update...")
    before = now()

    {:ok, files} = FTPPool.run(pool, &FTP.ls(&1))

    # Insert all asset
    for %{type: :directory, filename: asset} <- files do
      true = :ets.insert(assets_table(), {asset, _updated_at = nil})
    end

    # Cache is now ready
    :ok = :persistent_term.put(ready_persistent_term(), true)

    Logger.debug("files update finished (took: #{now() - before}ms)")

    {:noreply, state}
  end

  @impl true
  def handle_call({:update_files, asset}, _from, %{pool: pool} = state) do
    Logger.debug("starting files update for #{asset}...")
    before = now()

    {:ok, files} = FTPPool.run(pool, &FTP.ls(&1, asset))

    # Insert all asset files
    for %{type: :file, filename: filename} <- files do
      with {:ok, [^asset, bid_or_adk, datetime]} <- LsParser.filename_only(filename) do
        filename_pattern = String.to_charlist(filename)
        timestamp = DateTime.to_unix(datetime, :second)

        tuple = {filename_pattern, filename, bid_or_adk, timestamp}
        true = :ets.insert(asset_files_table(), tuple)
      else
        _ ->
          Logger.warning("invalid file found for asset #{asset}: #{filename}")
      end
    end

    # Update updated_at for asset
    true = :ets.insert(assets_table(), {asset, before})

    Logger.debug("files update finished for #{asset} (took: #{now() - before}ms)")

    {:reply, :ok, state}
  end

  ## Private functions

  defp now(), do: System.monotonic_time(:millisecond)

  defp assets_table(), do: :ets_darwinex_assets
  defp asset_files_table(), do: :ets_darwinex_asset_files
  defp ready_persistent_term(), do: :darwinex_cache_ready

  defp do_wait_for_cache(expire_at) do
    with {:expired, false} <- {:expired, now() >= expire_at},
         true <- :persistent_term.get(ready_persistent_term(), false) do
      :ok
    else
      {:expired, true} ->
        {:error, :timeout}

      false ->
        Process.sleep(10)
        do_wait_for_cache(expire_at)
    end
  end

  defp update_asset_files(asset, timeout) do
    now = now()

    case :ets.lookup(assets_table(), asset) do
      [{^asset, updated_at}] when now < updated_at + @expire_after ->
        :ok

      [{^asset, _updated_at}] ->
        :ok = GenServer.call(__MODULE__, {:update_files, asset}, timeout)

      [] ->
        {:error, :not_found}
    end
  catch
    :exit, {:timeout, _} -> {:error, :timeout}
  end

  defp list_files_from_table(asset, filters) do
    prefix =
      asset
      |> Kernel.<>(@asset_delimitor)
      |> String.to_charlist()
      |> ets_prefix()

    guards = to_ets_guard(filters)

    :ets.select(asset_files_table(), [{{prefix, :"$1", :"$2", :"$3"}, guards, [:"$1"]}])
  end

  defp ets_prefix([]), do: :_
  defp ets_prefix([h | t]), do: [h | ets_prefix(t)]

  defp to_ets_guard(filters, acc \\ [])
  defp to_ets_guard([], acc), do: acc

  defp to_ets_guard([filter | remaining], acc) do
    guard =
      case filter do
        {op, :type, value} -> {op, :"$2", value}
        {op, :time, value} -> {op, :"$3", value}
      end

    updated_acc =
      case acc do
        [] -> [guard]
        [prev_guard] -> [{:andalso, prev_guard, guard}]
      end

    to_ets_guard(remaining, updated_acc)
  end
end
