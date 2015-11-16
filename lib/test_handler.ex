defmodule TestHandler do
  def message(data, attempts) do
    IO.inspect {"TestHandler.message", data, attempts}
  end
end
