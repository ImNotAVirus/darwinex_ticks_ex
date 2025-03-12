defmodule DarwinexTicks.LsParser do
  @moduledoc false

  import NimbleParsec

  file_type =
    choice([
      string("d") |> replace(:directory),
      string("-") |> replace(:file)
    ])

  permission =
    choice([
      string("r") |> replace(:read),
      string("w") |> replace(:write),
      string("x") |> replace(:execute),
      string("-") |> replace(:none)
    ])

  delimitor = repeat(string(" "))
  line_delimitor = choice([string("\r\n"), string("\n")])

  string = ascii_string([not: ?\s, not: ?\r, not: ?\n], min: 1)

  time = integer(2) |> string(":") |> integer(2)
  year = integer(4)

  date =
    string
    |> concat(ignore(delimitor))
    |> integer(min: 1, max: 2)
    |> concat(ignore(delimitor))
    |> concat(choice([time, year]))

  line =
    file_type
    |> unwrap_and_tag(:file_type)
    |> concat(duplicate(permission, 3) |> tag(:user_permissions))
    |> concat(duplicate(permission, 3) |> tag(:group_permissions))
    |> concat(duplicate(permission, 3) |> tag(:others_permissions))
    |> ignore(delimitor)
    |> concat(integer(min: 1) |> unwrap_and_tag(:sub_directories))
    |> ignore(delimitor)
    |> concat(string |> unwrap_and_tag(:user))
    |> ignore(delimitor)
    |> concat(string |> unwrap_and_tag(:group))
    |> ignore(delimitor)
    |> concat(integer(min: 1) |> unwrap_and_tag(:size))
    |> ignore(delimitor)
    |> concat(date |> tag(:updated_at))
    |> ignore(delimitor)
    |> concat(string |> unwrap_and_tag(:filename))

  lines =
    line
    |> wrap()
    |> concat(ignore(line_delimitor))
    |> repeat()
    |> eos()

  defparsec :lines, lines

  asset_name = ascii_string([not: ?_], min: 1)
  filename_delimitor = string("_")

  bid_or_ask =
    choice([
      string("BID") |> replace(:bid),
      string("ASK") |> replace(:ask)
    ])

  filename_date =
    year
    |> ignore(string("-"))
    # month
    |> integer(2)
    |> ignore(string("-"))
    # day
    |> integer(2)
    |> ignore(string("_"))
    # hour
    |> integer(2)
    |> wrap()
    |> map(:parse_filename_date)

  filename =
    asset_name
    |> ignore(filename_delimitor)
    |> concat(bid_or_ask)
    |> ignore(filename_delimitor)
    |> concat(filename_date)
    |> ignore(string(".log.gz"))
    |> eos()

  # XAUUSD_ASK_2017-10-03_17.log.gz
  defparsec :filename, filename

  ## Public API

  def lines_only(data) do
    with {:ok, result, "", %{}, _, _} <- lines(data) do
      {:ok, result}
    end
  end

  def filename_only(data) do
    with {:ok, result, "", %{}, _, _} <- filename(data) do
      {:ok, result}
    end
  end

  ## Private functions

  defp parse_filename_date([year, month, day, hour]) do
    DateTime.new!(
      Date.new!(year, month, day),
      Time.new!(hour, 0, 0)
    )
  end
end
