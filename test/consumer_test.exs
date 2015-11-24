defmodule NSQ.ConsumerTest do
  use ExUnit.Case, async: true
  doctest NSQ.Consumer

  @test_topic "__nsq_consumer_test_topic__"
  @test_channel1 "__nsq_consumer_test_channel1__"
  @test_channel2 "__nsq_consumer_test_channel2__"

  setup do
    Logger.configure(level: :warn)
    HTTPotion.post("http://127.0.0.1:6751/topic/delete?topic=#{@test_topic}")
    :ok
  end


  test "#start_link establishes a connection to NSQ and processes messages" do
    test_pid = self
    NSQ.Consumer.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })

    HTTPotion.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)

    HTTPotion.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)
  end


  test "#start_link exits when given a bad address and not able to reconnect" do
    test_pid = self
    Process.flag(:trap_exit, true)
    NSQ.Consumer.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 7777}],
      max_reconnect_attempts: 0,
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })
    assert_receive({:EXIT, _pid, {:econnrefused, _last_call}})
  end


  test "#start_link lives when given a bad address but able to reconnect" do
    test_pid = self
    Process.flag(:trap_exit, true)
    NSQ.Consumer.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 7777}],
      max_reconnect_attempts: 1,
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })
    refute_receive({:EXIT, _pid, {:connect_failed, _last_call}}, 2000)
  end


  test "receives messages from mpub" do
    test_pid = self
    NSQ.Consumer.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      message_handler: fn(body, _msg) ->
        assert body == "mpubtest"
        send(test_pid, :handled)
        :ok
      end
    })

    HTTPotion.post("http://127.0.0.1:6751/mpub?topic=#{@test_topic}", [body: "mpubtest\nmpubtest\nmpubtest"])
    assert_receive(:handled, 2000)
    assert_receive(:handled, 2000)
    assert_receive(:handled, 2000)
  end
end
