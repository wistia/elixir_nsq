defmodule NSQ.Consumer.ConnectionsTask do
  @moduledoc """
  A task for NSQ.Consumer.ConnectionsTask.
  """

  use Task

  @doc """
  Initialized task for NSQ.Consumer.ConnectionTask.
  """
  def start_link(cons) do
    Task.start_link(NSQ.Consumer.Connections, :discovery_loop, [cons])
  end
end
