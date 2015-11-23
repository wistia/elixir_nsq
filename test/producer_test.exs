defmodule NSQ.ProducerTest do
  use ExUnit.Case, async: false
  doctest NSQ.Producer


  @test_topic "__nsq_producer_test_topic__"


  test "#start_link starts a new producer, discoverable via nsqlookupd" do
    nsqds = [{"127.0.0.1", 6750}, {"127.0.0.1", 6760}]
    {:ok, producer} = NSQ.Producer.start_link(nsqds, "__nsq_producer_test_topic__", %NSQ.Config{
      nsqds: nsqds
    })
    lookupds = [{"127.0.0.1", 6771}, {"127.0.0.1", 6781}]
    nsqds = NSQ.Connection.nsqds_from_lookupds(lookupds, "__nsq_producer_test_topic__")
    IO.puts "nsqds #{inspect nsqds}"
  end
end
