defmodule NSQ.Consumer.Sequence do
  def next do
    {:ok, agent} = Agent.start_link(fn -> 0 end, name: :consumer_seq)
  end
end
