defmodule DarwinexTicks.Assets do
  @moduledoc false

  require Explorer.DataFrame, as: DF

  alias DarwinexTicks.Cache
  alias DarwinexTicks.DataFrameExporter
  alias DarwinexTicks.{FTP, FTPPool}

  @assets_timeout :timer.seconds(5)
  @files_timeout :timer.minutes(5)

  ## Public API

  defdelegate list(timeout \\ @assets_timeout), to: Cache, as: :list_assets

  defdelegate list_files(asset, filters \\ [], timeout \\ @files_timeout),
    to: Cache,
    as: :list_files

  def list!(timeout \\ @assets_timeout) do
    case list(timeout) do
      {:ok, assets} -> assets
      error -> raise "failed with error #{inspect(error)}"
    end
  end

  def list_files!(asset, filters \\ [], timeout \\ @files_timeout) do
    case list_files(asset, filters, timeout) do
      {:ok, files} -> files
      error -> raise "failed with error #{inspect(error)}"
    end
  end

  def read_file!(asset, filename) when is_binary(filename) do
    case FTPPool.run(DarwinexTicks.FTPPool, &FTP.cat(&1, asset, filename)) do
      {:ok, content} -> inflate_gzip(content)
      error -> raise "get_file! failed (reason: #{inspect(error)})"
    end
  end

  def stream_files!(asset, filenames, timeout \\ :timer.minutes(1))
      when is_list(filenames) do
    opts = [max_concurrency: DarwinexTicks.pool_size(), ordered: true, timeout: timeout]

    filenames
    |> Task.async_stream(&read_file!(asset, &1), opts)
    |> Stream.map(fn {:ok, value} -> value end)
    |> Stream.map(&DataFrameExporter.load_csv!/1)
    |> Stream.flat_map(&DF.to_rows_stream(&1, atom_keys: true))
  end

  def file_to_dataframe!(asset, filename) when is_binary(filename) do
    asset
    |> read_file!(filename)
    |> DataFrameExporter.load_csv!()
  end

  def files_to_dataframe!(asset, filenames, timeout \\ :timer.minutes(1))
      when is_list(filenames) do
    opts = [max_concurrency: DarwinexTicks.pool_size(), ordered: true, timeout: timeout]

    filenames
    |> Task.async_stream(&read_file!(asset, &1), opts)
    |> Stream.map(fn {:ok, value} -> value end)
    |> Stream.map(&DataFrameExporter.load_csv!/1)
    |> Enum.to_list()
    |> DF.concat_rows()
  end

  ## Private functions

  defp inflate_gzip(bin) do
    list = :erlang.binary_to_list(bin)

    z = :zlib.open()
    :zlib.inflateInit(z, 16 + 15)
    result = :zlib.inflate(z, list)
    :zlib.close(z)

    :erlang.list_to_binary(result)
  end
end
