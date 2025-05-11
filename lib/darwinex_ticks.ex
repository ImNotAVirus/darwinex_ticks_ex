defmodule DarwinexTicks do
  @moduledoc """
  Documentation for `DarwinexTicks`.
  """

  require Logger
  require Explorer.DataFrame, as: DF

  alias DarwinexTicks.Assets
  alias DarwinexTicks.{FTP, FTPPool}
  alias DarwinexTicks.DataFrameHelpers
  alias DarwinexTicks.LsParser
  alias DarwinexTicks.TimeFrameHelpers

  alias Explorer.Series

  ## Public API

  def download(asset, out_dir, opts \\ []) do
    timeframe = validate_timeframe!(opts)
    chunk = validate_chunk!(opts)
    format = validate_format!(opts)
    from = validate_date!(opts, :from)
    to = validate_date!(opts, :to)
    price = validate_price!(opts)
    on_error = validate_on_error!(opts)
    opening_hour = validate_opening_hour!(opts)
    force? = Keyword.get(opts, :force, false)
    timeout = Keyword.get(opts, :timeout, :timer.minutes(1))

    opts = [max_concurrency: pool_size(), ordered: false, timeout: timeout]
    ohlc_opts = [opening_hour: opening_hour, price: price]

    query_filters = build_filters(timeframe, price, from, to)

    with {:ok, files} <- Assets.list_files(asset, query_filters) do
      stream = stream_file_tuples!(files)
      chunks = chunk_stream(stream, chunk)

      :ok = File.mkdir_p!(out_dir)

      :ok =
        chunks
        |> Stream.map(&{chunk_filename(out_dir, format, &1), &1})
        |> maybe_filter_existing(force?)
        |> Task.async_stream(
          fn {filename, chunk} ->
            {filename, chunk_to_ticks_dataframe!(asset, chunk, on_error)}
          end,
          opts
        )
        |> Stream.map(fn {:ok, value} -> value end)
        |> maybe_to_ohlc(timeframe, ohlc_opts)
        |> Enum.each(fn {filename, df} -> save_dataframe(df, filename, format) end)
    end
  end

  def resample(input, out_dir, timeframe, opts \\ []) do
    input_files = validate_input_files!(input)
    input_timeframe = validate_or_fetch_input_timeframe!(opts, input_files)
    output_timeframe = validate_resample_timeframe!(input_timeframe, timeframe)
    chunk = validate_chunk!(opts)
    format = validate_format!(opts)
    price = validate_price!(opts)
    opening_hour = validate_opening_hour!(opts)
    force? = Keyword.get(opts, :force, false)

    :ok = File.mkdir_p!(out_dir)

    asset =
      input_files
      |> hd()
      |> Path.basename()
      |> String.split("_", parts: 2)
      |> hd()

    ohlc_opts = [opening_hour: opening_hour, price: price]

    :ok =
      input_files
      |> dataframe_chunk_stream(chunk)
      |> Stream.map(&DataFrameHelpers.to_ohlc(&1, output_timeframe, ohlc_opts))
      |> Stream.map(&{dataframe_filename(&1, asset, out_dir, format), &1})
      |> maybe_filter_existing(force?)
      |> Enum.each(fn {filename, df} -> save_dataframe(df, filename, format) end)
  end

  ## Internal API

  @doc false
  def pool_size() do
    Application.get_env(:darwinex_ticks, :pool_size)
  end

  ## Private functions

  defp validate_timeframe!(opts) do
    opts
    |> Keyword.get(:timeframe, "tick")
    |> TimeFrameHelpers.parse_timeframe!()
  end

  defp validate_chunk!(opts) do
    case Keyword.get(opts, :chunk, :hour) do
      chunk when chunk in [:hour, :day, :week, :month, :year] -> chunk
      chunk -> raise ArgumentError, "invalid chunk #{inspect(chunk)}"
    end
  end

  defp validate_format!(opts) do
    case Keyword.get(opts, :format, :parquet) do
      format when format in [:parquet, :csv] -> format
      format -> raise ArgumentError, "invalid format #{inspect(format)}"
    end
  end

  defp validate_date!(opts, name) do
    case Keyword.get(opts, name) do
      nil -> nil
      %DateTime{} = date -> date
    end
  end

  defp validate_price!(opts) do
    case Keyword.get(opts, :price, :mid) do
      price when price in [:mid, :ask, :bid] -> price
      price -> raise ArgumentError, "invalid price #{inspect(price)}"
    end
  end

  defp validate_on_error!(opts) do
    case Keyword.get(opts, :on_error, :raise) do
      on_error when on_error in [:raise, :ignore] -> on_error
      on_error -> raise ArgumentError, "invalid on_error #{inspect(on_error)}"
    end
  end

  defp validate_opening_hour!(opts) do
    case Keyword.get(opts, :opening_hour, 0) do
      opening_hour when is_integer(opening_hour) and opening_hour < 24 -> opening_hour
      opening_hour -> raise ArgumentError, "invalid opening_hour #{inspect(opening_hour)}"
    end
  end

  defp validate_input_files!(input) do
    if not File.exists?(input) do
      raise ArgumentError, "#{input} doesn't exists"
    end

    files =
      case File.dir?(input) do
        false -> [input]
        true -> DataFrameHelpers.wildcard(input)
      end

    case files do
      [] -> raise ArgumentError, "input (#{input}) can not be an empty folder"
      files -> files
    end
  end

  defp validate_or_fetch_input_timeframe!(opts, input_files) do
    case Keyword.get(opts, :input_timeframe, :auto) do
      :auto -> fetch_input_timeframe!(input_files)
      tf -> TimeFrameHelpers.parse_timeframe!(tf)
    end
  end

  defp fetch_input_timeframe!([]) do
    raise "unable to find the input timeframe"
  end

  defp fetch_input_timeframe!([input | remaining]) do
    df = DataFrameHelpers.dataframe_from_file!(input)
    times = df["time"] |> Series.head(2) |> Series.to_list()

    case times do
      [t1, t2] -> TimeFrameHelpers.calc_timeframe!(t1, t2)
      _ -> fetch_input_timeframe!(remaining)
    end
  end

  defp validate_resample_timeframe!(input_timeframe, timeframe) do
    output_timeframe = TimeFrameHelpers.parse_timeframe!(timeframe)

    case TimeFrameHelpers.compare(input_timeframe, output_timeframe) do
      :lt -> output_timeframe
      :eq -> raise "timeframes are already equals"
      :gt -> raise "output timeframe must be greater than input"
    end
  end

  defp build_filters(timeframe, price, from, to) do
    type_filter =
      case {timeframe, price} do
        {{:tick, _mult}, _} -> []
        {_, :mid} -> []
        {_, type} -> [{:==, :type, type}]
      end

    from_filter =
      case from do
        nil -> []
        _ -> [{:>=, :time, DateTime.to_unix(from, :second)}]
      end

    to_filter =
      case to do
        nil -> []
        _ -> [{:<, :time, DateTime.to_unix(to, :second)}]
      end

    List.flatten([type_filter, from_filter, to_filter])
  end

  defp chunk_filename(out_dir, format, [tuple | _]) do
    ext = DataFrameHelpers.format_extname(format)

    filename =
      case tuple do
        {ask_name, nil} -> ask_name
        {_ask_name, bid_name} -> bid_name
      end

    {:ok, [asset, _type, datetime]} = LsParser.filename_only(filename)
    Path.join(out_dir, "#{asset}_#{format_datetime(datetime)}.#{ext}")
  end

  defp dataframe_filename(df, asset, out_dir, format) do
    ext = DataFrameHelpers.format_extname(format)
    datetime = Series.at(df["time"], 0)

    Path.join(out_dir, "#{asset}_#{format_datetime(datetime)}.#{ext}")
  end

  defp format_datetime(datetime) do
    date =
      case datetime do
        %DateTime{} -> datetime |> DateTime.to_date()
        %NaiveDateTime{} -> datetime |> NaiveDateTime.to_date()
      end

    hour = datetime.hour |> Integer.to_string() |> String.pad_leading(2, "0")

    "#{Date.to_string(date)}_#{hour}"
  end

  defp maybe_filter_existing(stream, _force = true), do: stream

  defp maybe_filter_existing(stream, _force = false) do
    Stream.reject(stream, fn {local, _data} ->
      exists? = File.exists?(local)

      if exists? do
        Logger.debug("Skipping file: #{Path.basename(local)} (already exists)")
      end

      exists?
    end)
  end

  defp maybe_to_ohlc(stream, _timeframe = {:tick, 1}, _ohlc_opts), do: stream

  defp maybe_to_ohlc(stream, timeframe, ohlc_opts) do
    Stream.map(stream, fn {filename, df} ->
      {filename, DataFrameHelpers.to_ohlc(df, timeframe, ohlc_opts)}
    end)
  end

  defp stream_file_tuples!(files) do
    {ask, bid} =
      Enum.split_while(files, fn name ->
        [_asset, bid_or_ask, _date] = String.split(name, "_", parts: 3)
        bid_or_ask == "ASK"
      end)

    ask_length = length(ask)
    bid_length = length(bid)

    {ask, bid} =
      case {ask_length, bid_length} do
        {0, _value} -> {Stream.cycle([nil]), bid}
        {_value, 0} -> {ask, Stream.cycle([nil])}
        {value, value} -> {ask, bid}
        {bid, ask} -> raise "files count doesn't match (bid: #{bid}, ask: #{ask})"
      end

    stream = Stream.zip(ask, bid)

    case ask_length == 0 or bid_length == 0 do
      true ->
        stream

      false ->
        Stream.map(stream, fn {ask_name, bid_name} = tuple ->
          if bid_name == String.replace(ask_name, "_ASK_", "_BID_") do
            tuple
          else
            raise "files name doesn't match: #{ask_name} - #{bid_name}"
          end
        end)
    end
  end

  # [:hour, :day, :week, :month, :year]
  defp chunk_stream(stream, :hour) do
    # Tick files are already splited by hours
    Stream.map(stream, &[&1])
  end

  defp chunk_stream(stream, chunk_type) do
    index =
      case Enum.at(stream, 0) do
        {_ask_name, nil} -> 0
        {_ask_name, _bid_name} -> 1
      end

    Stream.chunk_by(stream, fn tuple ->
      {:ok, [_asset, _type, datetime]} = tuple |> elem(index) |> LsParser.filename_only()

      # {year, month, day}
      date = Date.to_erl(datetime)

      case chunk_type do
        :day -> elem(date, 2)
        :week -> :calendar.iso_week_number(date)
        :month -> elem(date, 1)
        :year -> elem(date, 0)
      end
    end)
  end

  defp chunk_to_ticks_dataframe!(asset, filenames, on_error) do
    filenames
    |> Enum.map(fn {ask_filename, bid_filename} ->
      {
        maybe_filename_to_dataframe(asset, ask_filename, :ask, on_error),
        maybe_filename_to_dataframe(asset, bid_filename, :bid, on_error)
      }
    end)
    |> Enum.map(&maybe_merge_dataframes/1)
    |> DF.concat_rows()
    |> DF.mutate(bid: fill_missing(bid, :forward), ask: fill_missing(ask, :forward))
    # Sometime rows are empty (time, bid and ask)
    # eg. XAUUSD_BID_2017-10-27_20.log.gz
    |> DF.drop_nil()
  end

  defp maybe_filename_to_dataframe(_asset, nil, _, _), do: nil

  defp maybe_filename_to_dataframe(asset, filename, name, on_error) do
    {:ok, content} = FTPPool.run(DarwinexTicks.FTPPool, &FTP.cat(&1, asset, filename))

    content
    |> DataFrameHelpers.load_csv!()
    |> DF.rename(price: name, size: "#{name}_size")
  rescue
    exception ->
      Logger.error("error on #{asset}/#{filename}: #{inspect(exception)}")

      case on_error do
        :raise ->
          reraise exception, __STACKTRACE__

        :ignore ->
          DF.new(
            [{"time", []}, {"#{name}", []}, {"#{name}_size", []}],
            dtypes: [
              {"time", {:naive_datetime, :millisecond}},
              {"#{name}", :f32},
              {"#{name}_size", :f32}
            ]
          )
      end
  end

  defp maybe_merge_dataframes({ask_df, nil}), do: ask_df
  defp maybe_merge_dataframes({nil, bid_df}), do: bid_df

  defp maybe_merge_dataframes({ask_df, bid_df}) do
    ask_df
    |> DF.join(bid_df, how: :outer)
    |> DF.mutate(time: coalesce(time, time_right))
    |> DF.discard("time_right")
    |> DF.sort_by(time)
  end

  defp save_dataframe(df, filename, format) do
    IO.inspect(filename)
    Logger.debug("Writing file: #{Path.basename(filename)}")
    DataFrameHelpers.dataframe_to_file!(df, filename, format)
  end

  defp dataframe_chunk_stream(input_files, chunk) do
    input_length = length(input_files)

    Stream.transform(
      input_files,
      {_prev_df = nil, _counter = 1},
      fn filename, {prev_df, counter} ->
        df = DataFrameHelpers.dataframe_from_file!(filename)

        df =
          case prev_df do
            nil -> df
            _ -> DF.concat_rows(prev_df, df)
          end

        {chunks, remaining_df} = DataFrameHelpers.split_by_chunks(df, chunk)

        case counter < input_length do
          false -> {chunks ++ [remaining_df], {:halt, {nil, counter + 1}}}
          true -> {chunks, {remaining_df, counter + 1}}
        end
      end
    )
  end
end
