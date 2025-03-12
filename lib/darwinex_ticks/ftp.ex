defmodule DarwinexTicks.FTP do
  @moduledoc false

  alias DarwinexTicks.LsParser

  ## Public API

  def connect(host, port, username, password) do
    host = String.to_charlist(host)
    username = String.to_charlist(username)
    password = String.to_charlist(password)

    with {:ok, pid} <- :ftp.open(host, port: port),
         :ok <- :ftp.user(pid, username, password),
         :ok <- :ftp.type(pid, :binary) do
      {:ok, pid}
    end
  end

  def close(pid) do
    :ftp.close(pid)
  end

  def cat(pid, asset, filename) do
    :ftp.recv_bin(pid, ~c"#{asset}/#{filename}")
  end

  def ls(pid, path \\ nil) do
    args =
      case path do
        nil -> [pid]
        _ -> [pid, String.to_charlist(path)]
      end

    with {:ok, data} <- apply(:ftp, :ls, args),
         data = List.to_string(data),
         {:ok, parsed_files} <- LsParser.lines_only(data) do
      files =
        parsed_files
        |> Enum.map(fn file ->
          [
            {:file_type, type},
            _user_permissions,
            _group_permission,
            _others_permissions,
            _sub_directories,
            _user,
            _group,
            _size,
            _updated_at,
            {:filename, filename}
          ] = file

          %{type: type, filename: filename}
        end)
        |> Enum.reject(&(&1.filename in [".", ".."]))

      {:ok, files}
    end
  end
end
