defmodule NSQ.ProducerTest do
  use ExUnit.Case, async: true
  doctest NSQ.Producer

  @test_topic "__nsq_producer_test_topic__"
  @test_channel1 "__nsq_producer_test_channel1__"
  @configured_nsqds ["127.0.0.1:6750", "127.0.0.1:6760"]

  setup do
    Logger.configure(level: :warn)
    HTTPotion.post("http://127.0.0.1:6751/topic/delete?topic=#{@test_topic}")
    HTTPotion.post("http://127.0.0.1:6761/topic/delete?topic=#{@test_topic}")
    HTTPotion.post("http://127.0.0.1:6771/topic/delete?topic=#{@test_topic}")
    HTTPotion.post("http://127.0.0.1:6781/topic/delete?topic=#{@test_topic}")
    :ok
  end

  test "#new starts a new producer, discoverable via nsqlookupd" do
    {:ok, producer} = NSQ.Producer.Supervisor.start_link(
      @test_topic, %NSQ.Config{nsqds: @configured_nsqds}
    )

    # Produce a ton of messages so we're "guaranteed" both our nsqds have
    # messages and are therefore discoverable.
    Enum.map 0..100, fn(_i) -> NSQ.Producer.pub(producer, "test 1") end

    lookupds = [{"127.0.0.1", 6771}, {"127.0.0.1", 6781}]
    discovered_nsqds =
      lookupds |> NSQ.Lookupd.nsqds_with_topic("__nsq_producer_test_topic__")

    # Sort the arrays so we can compare them.
    configured_nsqds = Enum.sort_by(NSQ.Config.normalize_hosts(@configured_nsqds), &inspect(&1))
    discovered_nsqds = Enum.sort_by(discovered_nsqds, &inspect(&1))

    assert configured_nsqds == discovered_nsqds
  end

  test "messages added via pub are handled by a consumer" do
    {:ok, producer} = NSQ.Producer.Supervisor.start_link(
      @test_topic, %NSQ.Config{nsqds: @configured_nsqds}
    )

    test_pid = self()
    NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: @configured_nsqds,
      message_handler: fn(body, msg) ->
        assert body == "test abc"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })

    NSQ.Producer.pub(producer, "test abc")
    assert_receive(:handled, 2000)
  end

  test "messages added via mpub are handled by a consumer" do
    {:ok, producer} = NSQ.Producer.Supervisor.start_link(
      @test_topic, %NSQ.Config{nsqds: @configured_nsqds}
    )

    test_pid = self()
    {:ok, bodies} = Agent.start_link(fn -> [] end)
    NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: @configured_nsqds,
      message_handler: fn(body, _msg) ->
        Agent.update bodies, fn(list) -> [body|list] end
        send(test_pid, :handled)
        :ok
      end
    })

    NSQ.Producer.mpub(producer, ["def", "ghi"])
    assert_receive(:handled, 2000)
    assert_receive(:handled, 2000)
    assert Agent.get(bodies, fn(list) -> list end) == ["def", "ghi"]
  end
end
