defmodule DarwinexTicks.DataFrameHelpers do
  @moduledoc """
  TODO: Documentation
  """

  require Explorer.DataFrame, as: DF
  require Explorer.Series, as: S

  import DarwinexTicks.TimeFrameHelpers, only: [is_intraday: 1]

  alias DarwinexTicks.TimeFrameHelpers

  ## Public API

  def wildcard(path) do
    path |> Path.join("*.{csv,parquet.gz}") |> Path.wildcard()
  end

  def extname(filename) do
    # Path.extname/1 desn't works with multi extensions
    case String.split(filename, ".", parts: 2) do
      [_, "csv"] -> ".csv"
      [_, "parquet.gz"] -> ".parquet.gz"
    end
  end

  def format_extname(format) do
    case format do
      :parquet -> "parquet.gz"
      :csv -> "csv"
    end
  end

  def dataframe_from_file!(filename) do
    case extname(filename) do
      ".csv" -> DF.from_csv!(filename)
      ".parquet.gz" -> DF.from_parquet!(filename)
    end
  end

  def dataframe_to_file!(df, filename, format) do
    case format do
      :parquet -> DF.to_parquet!(df, filename, compression: {:gzip, 9})
      :csv -> DF.to_csv!(df, filename)
    end
  end

  def load_csv(content) do
    result =
      DF.load_csv(
        content,
        header: false,
        dtypes: [time: :s64, price: :f32, size: :f32],
        lazy: true
      )

    with {:ok, df} <- result do
      df
      # Polars can't parse timestamp directly so we need a cast :/
      |> DF.mutate(time: cast(time, {:naive_datetime, :millisecond}))
      |> DF.collect()
      |> then(&{:ok, &1})
    end
  end

  def load_csv!(content) do
    case load_csv(content) do
      {:ok, df} -> df
      error -> raise "load_csv! failed (reason: #{inspect(error)})"
    end
  end

  def split_by_chunks(df, chunk) do
    df_with_id =
      case chunk do
        :hour -> DF.mutate(df, chunk_id: quotient(cast(time, :s64), :timer.hours(1)))
        :day -> DF.mutate(df, chunk_id: day_of_year(time))
        :week -> DF.mutate(df, chunk_id: week_of_year(time))
        :month -> DF.mutate(df, chunk_id: month(time))
        :year -> DF.mutate(df, chunk_id: year(time))
      end

    chunks = df_with_id["chunk_id"] |> S.distinct() |> S.to_list()
    do_split_by_chunks(df_with_id, chunks)
  end

  def to_ohlc(%DF{} = df, timeframe, opts \\ []) when is_intraday(timeframe) do
    price = Keyword.get(opts, :price, :mid)
    columns = DF.names(df)

    cond do
      Enum.member?(columns, "price") ->
        do_to_ohlc(df, timeframe, opts)

      price == :ask and Enum.member?(columns, "ask") ->
        df
        |> DF.rename(ask: "price")
        |> do_to_ohlc(timeframe, opts)

      price == :bid and Enum.member?(columns, "bid") ->
        df
        |> DF.rename(bid: "price")
        |> do_to_ohlc(timeframe, opts)

      price == :mid and Enum.member?(columns, "ask") and Enum.member?(columns, "bid") ->
        df
        |> DF.lazy()
        |> DF.mutate(ask: fill_missing(ask, :forward))
        |> DF.mutate(bid: fill_missing(bid, :forward))
        |> DF.mutate(price: (ask + bid) / 2)
        |> do_to_ohlc(timeframe, opts)

      true ->
        raise ArgumentError, "invalid dataframe"
    end
  end

  ## Private functions

  defp do_split_by_chunks(df, chunk_ids, acc \\ [])

  defp do_split_by_chunks(_df, [], [remaining_df | chunks]) do
    {Enum.reverse(chunks), remaining_df}
  end

  defp do_split_by_chunks(df, [chunk_id | remaining], acc) do
    chunk =
      df
      |> DF.filter(chunk_id == ^chunk_id)
      |> DF.discard("chunk_id")

    do_split_by_chunks(df, remaining, [chunk | acc])
  end

  defp do_to_ohlc(df, timeframe, opts) do
    market_open_hour = Keyword.get(opts, :opening_hour, 0)

    offset = :timer.hours(rem(24 - market_open_hour, 24))
    day_ms = :timer.hours(24)

    timeframe_ms = TimeFrameHelpers.timeframe_to_ms(timeframe)

    df
    |> DF.lazy()
    |> DF.mutate(time_ms: cast(time, :s64))
    # Ajusted time (time + market open)
    |> DF.mutate(time_ajusted_ms: time_ms + ^offset)
    |> DF.mutate(time_ajusted_ms: cast(time_ajusted_ms, :s64))
    # Market open
    |> DF.mutate(time_ajusted_open_ms: time_ajusted_ms - remainder(time_ajusted_ms, ^day_ms))
    |> DF.mutate(time_open_ms: time_ajusted_open_ms - ^offset)
    # Calculate bar open
    |> DF.mutate(time_since_open_ms: time_ms - time_open_ms)
    |> DF.mutate(bar_mult: quotient(time_since_open_ms, ^timeframe_ms))
    |> DF.mutate(bar_open: time_open_ms + bar_mult * ^timeframe_ms)
    # Create OHLC bars
    |> DF.group_by("bar_open")
    |> DF.summarise(
      open: first(price),
      high: max(price),
      low: min(price),
      close: last(price)
    )
    |> DF.rename(bar_open: "time")
    |> DF.mutate(time: cast(time, {:naive_datetime, :millisecond}))
    |> DF.collect()
  end
end
