defmodule NSQ.Logger do
  require Logger

  defdelegate debug(message), to: Logger
  defdelegate info(message), to: Logger
  defdelegate error(message), to: Logger

  case Version.compare(System.version(), "1.11.0") do
    :lt -> defdelegate warn(message), to: Logger
    _ -> defdelegate warn(message), to: Logger, as: :warning
  end

  def configure(opts) do
    case {opts, Version.compare(System.version(), "1.11.0")} do
      {[level: :warn], :gt} ->
        Logger.configure(Keyword.merge(opts, level: :warning))

      _ ->
        Logger.configure(opts)
    end
  end
end
