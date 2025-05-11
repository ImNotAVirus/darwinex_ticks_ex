defmodule DarwinexTicks.TimeFrameHelpers do
  @moduledoc false

  @type timeframe :: {unit, multiplier :: pos_integer()}
  @type unit :: :tick | :s | :m | :h | :D | :W | :M

  @units [:tick, :s, :m, :h, :D, :W, :M]

  ## Public API

  defguard is_intraday(tf) when elem(tf, 0) in [:tick, :s, :m, :h]

  @spec parse_timeframe(atom() | String.t()) :: {:ok, timeframe()} | {:error, :invalid_timeframe}
  def parse_timeframe(timeframe) do
    case timeframe do
      tf when is_atom(tf) ->
        tf
        |> Atom.to_string()
        |> do_parse_timeframe()

      tf ->
        do_parse_timeframe(tf)
    end
  end

  @spec parse_timeframe(atom() | String.t()) :: timeframe()
  def parse_timeframe!(timeframe) do
    case parse_timeframe(timeframe) do
      {:ok, tf} -> tf
      {:error, :invalid_timeframe} -> raise "invalid timeframe: #{inspect(timeframe)}"
    end
  end

  @spec calc_timeframe!(NaiveDateTime.t(), NaiveDateTime.t()) :: timeframe()
  def calc_timeframe!(t1, t2) do
    case NaiveDateTime.compare(t1, t2) do
      :lt -> :ok
      :eq -> raise ArgumentError, "timeframes cannot be equals"
      :gt -> raise ArgumentError, "t1 should be earlier (t1: #{t1} - t2: #{t2})"
    end

    case {t1, t2} do
      {%NaiveDateTime{microsecond: {m, _precision}}, _t2} when m != 0 -> diff(t1, t2, :tick)
      {_t1, %NaiveDateTime{microsecond: {m, _precision}}} when m != 0 -> diff(t1, t2, :tick)
      {%NaiveDateTime{second: s}, _t2} when s != 0 -> diff(t1, t2, :s)
      {_t1, %NaiveDateTime{second: s}} when s != 0 -> diff(t1, t2, :s)
      {%NaiveDateTime{minute: m}, _t2} when m != 0 -> diff(t1, t2, :m)
      {_t1, %NaiveDateTime{minute: m}} when m != 0 -> diff(t1, t2, :m)
      {%NaiveDateTime{hour: h}, _t2} when h != 0 -> diff(t1, t2, :h)
      {_t1, %NaiveDateTime{hour: h}} when h != 0 -> diff(t1, t2, :h)
      {%NaiveDateTime{day: 1}, %NaiveDateTime{day: 1}} -> diff(t1, t2, :M)
      # Can be Daily or Weekly
      _ -> diff(t1, t2, :D)
    end
  end

  @spec compare(timeframe(), timeframe()) :: :lt | :eq | :gt
  def compare(tf1, tf2) do
    case {tf1, tf2} do
      {{unit, m1}, {unit, m2}} when m1 < m2 -> :lt
      {{unit, m1}, {unit, m2}} when m1 == m2 -> :eq
      {{unit, m1}, {unit, m2}} when m1 > m2 -> :gt
      {{u1, _m1}, {u2, _m2}} -> compare_unit(u1, u2)
    end
  end

  @spec timeframe_to_ms(timeframe()) :: pos_integer()
  def timeframe_to_ms({unit, mult}) do
    case unit do
      :s -> :timer.seconds(mult)
      :m -> :timer.minutes(mult)
      :h -> :timer.hours(mult)
    end
  end

  ## Private functions

  defp do_parse_timeframe("tick"), do: {:ok, {:tick, 1}}
  defp do_parse_timeframe("ticks"), do: {:ok, {:tick, 1}}

  defp do_parse_timeframe(<<unit::binary-size(1), mult::binary>>)
       when unit in ["s", "m", "h", "D", "W", "M"] do
    case Integer.parse(mult, 10) do
      {mult, ""} -> {:ok, {String.to_existing_atom(unit), mult}}
      _ -> {:error, :invalid_timeframe}
    end
  end

  defp do_parse_timeframe(_) do
    {:error, :invalid_timeframe}
  end

  defp diff(_t1, _t2, :tick), do: {:tick, 1}
  defp diff(t1, t2, :s), do: {:s, NaiveDateTime.diff(t2, t1, :second)}
  defp diff(t1, t2, :m), do: {:s, NaiveDateTime.diff(t2, t1, :minute)}
  defp diff(t1, t2, :h), do: {:s, NaiveDateTime.diff(t2, t1, :hour)}
  defp diff(t1, t2, :M), do: {:M, t2.month - t1.month}

  defp diff(t1, t2, :D) do
    diff = NaiveDateTime.diff(t2, t1, :day)

    case rem(diff, 7) do
      0 -> {:W, div(diff, 7)}
      _ -> {:D, diff}
    end
  end

  defp compare_unit(u1, u2) do
    i1 = Enum.find_index(@units, &(&1 == u1))
    i2 = Enum.find_index(@units, &(&1 == u2))

    case i1 < i2 do
      true -> :lt
      false -> :gt
    end
  end
end
