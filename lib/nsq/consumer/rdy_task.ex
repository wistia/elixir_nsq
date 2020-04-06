defmodule NSQ.Consumer.RDYTask do
  @moduledoc """
  A task for NSQ.Consumer.RDY.
  """

  use Task

  @doc """
  Initiailized task for NSQ.Consumer.RDY.
  """
  def start_link(cons) do
    Task.start_link(NSQ.Consumer.RDY, :redistribute_loop, [cons])
  end
end
