defmodule DarwinexTicks do
  @moduledoc """
  Documentation for `DarwinexTicks`.
  """

  require Logger
  require Explorer.DataFrame, as: DF

  alias DarwinexTicks.Assets
  alias DarwinexTicks.{FTP, FTPPool}
  alias DarwinexTicks.DataFrameExporter
  alias DarwinexTicks.LsParser

  alias Explorer.DataFrame, as: DF

  ## Public API 

  def download(asset, out_dir, opts \\ []) do
    timeframe = validate_timeframe!(opts)
    chunk = validate_chunk!(opts)
    format = validate_format!(opts)
    from = validate_date!(opts, :from)
    to = validate_date!(opts, :to)
    price = validate_price!(opts)
    on_error = validate_on_error!(opts)
    force? = Keyword.get(opts, :force, false)
    timeout = Keyword.get(opts, :timeout, :timer.minutes(1))

    opts = [max_concurrency: pool_size(), ordered: false, timeout: timeout]

    query_filters = build_filters(timeframe, price, from, to)

    with {:ok, files} <- Assets.list_files(asset, query_filters) do
      stream = stream_file_tuples!(files)
      chunks = chunk_stream(stream, chunk)

      :ok = File.mkdir_p!(out_dir)

      :ok =
        chunks
        |> Stream.map(&{chunk_filename(chunk, out_dir, format, &1), &1})
        |> maybe_filter_existing(force?)
        |> Task.async_stream(
          fn {filename, chunk} ->
            {filename, chunk_to_ticks_dataframe!(asset, chunk, on_error)}
          end,
          opts
        )
        |> Stream.map(fn {:ok, value} -> value end)
        |> Enum.each(&save_chunk(&1, format))
    end
  end

  ## Internal API

  @doc false
  def pool_size() do
    Application.get_env(:darwinex_ticks, :pool_size)
  end

  ## Private functions 

  defp validate_timeframe!(opts) do
    validate_mult = fn mult, tf ->
      case Integer.parse(mult, 10) do
        {_int, ""} -> tf
        _ -> raise ArgumentError, "invalid timeframe #{inspect(tf)}"
      end
    end

    tf =
      case Keyword.get(opts, :timeframe, "tick") do
        tf when is_atom(tf) -> Atom.to_string(tf)
        tf -> tf
      end

    case tf do
      "tick" -> "tick"
      "ticks" -> "tick"
      "s" <> mult = tf -> validate_mult.(mult, tf)
      "m" <> mult = tf -> validate_mult.(mult, tf)
      "h" <> mult = tf -> validate_mult.(mult, tf)
      "D" <> mult = tf -> validate_mult.(mult, tf)
      "W" <> mult = tf -> validate_mult.(mult, tf)
      "M" <> mult = tf -> validate_mult.(mult, tf)
      _ -> raise ArgumentError, "invalid timeframe #{tf}"
    end
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

  defp build_filters(timeframe, price, from, to) do
    type_filter =
      case {timeframe, price} do
        {"tick", _} -> []
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

  defp chunk_filename(chunk, out_dir, format, [tuple | _]) do
    ext =
      case format do
        :parquet -> "parquet.gz"
        :csv -> "csv"
      end

    filename =
      case tuple do
        {ask_name, nil} -> ask_name
        {_ask_name, bid_name} -> bid_name
      end

    {:ok, [asset, _type, datetime]} = LsParser.filename_only(filename)
    date = datetime |> DateTime.to_date() |> Date.to_string()

    format_hour = fn hour ->
      hour |> Integer.to_string() |> String.pad_leading(2, "0")
    end

    filename =
      case chunk do
        :hour -> "#{asset}_#{date}_#{format_hour.(datetime.hour)}.#{ext}"
        _ -> "#{asset}_#{date}.#{ext}"
      end

    Path.join(out_dir, filename)
  end

  defp maybe_filter_existing(stream, _force = true), do: stream

  defp maybe_filter_existing(stream, _force = false) do
    Stream.reject(stream, fn {local, _remotes} ->
      exists? = File.exists?(local)

      if exists? do
        Logger.debug("Skipping file: #{Path.basename(local)} (already exists)")
      end

      exists?
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
  end

  defp maybe_filename_to_dataframe(_asset, nil, _, _), do: nil

  defp maybe_filename_to_dataframe(asset, filename, name, on_error) do
    {:ok, content} = FTPPool.run(DarwinexTicks.FTPPool, &FTP.cat(&1, asset, filename))

    content
    |> DataFrameExporter.load_csv!()
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
    |> DF.sort_by(time)
    |> DF.mutate(bid: fill_missing(bid, :forward), ask: fill_missing(ask, :forward))
    |> DF.discard("time_right")
  end

  defp save_chunk({filename, df}, format) do
    Logger.debug("Writing file: #{Path.basename(filename)}")

    case format do
      :parquet -> DF.to_parquet!(df, filename, compression: {:gzip, 9})
      :csv -> DF.to_csv!(df, filename)
    end
  end
end
