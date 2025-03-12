defmodule DarwinexTicks.DataFrameExporter do
  @moduledoc """
  TODO: Documentation
  """

  require Explorer.DataFrame, as: DF

  ## Helpers

  defguardp is_intraday(tf) when binary_part(tf, 0, 1) in ["s", "m", "h"]

  ## Public API

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

  def to_ohlc(%DF{} = df, timeframe, market_open_hour \\ 0) when is_intraday(timeframe) do
    offset = :timer.hours(rem(24 - market_open_hour, 24))
    day_ms = :timer.hours(24)

    timeframe_ms = timeframe_to_ms(timeframe)

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
      close: last(price),
      size: sum(size)
    )
    |> DF.rename(bar_open: "time")
    |> DF.mutate(time: cast(time, {:naive_datetime, :millisecond}))
    |> DF.collect()
  end

  ## Private functions

  defp timeframe_to_ms(<<unit::8, mult::binary>>) do
    mult = String.to_integer(mult)

    case unit do
      ?s -> :timer.seconds(mult)
      ?m -> :timer.minutes(mult)
      ?h -> :timer.hours(mult)
    end
  end
end
