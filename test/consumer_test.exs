defmodule NSQ.ConsumerTest do
  use ExUnit.Case
  # doctest NSQ.Consumer

  @test_topic "__nsq_consumer_test_topic__"
  @test_channel1 "__nsq_consumer_test_channel1__"
  @test_channel2 "__nsq_consumer_test_channel2__"

  def new_test_consumer(handler) do
    NSQ.Consumer.start_link(@test_topic, @test_channel1, %{
      nsqds: [{"127.0.0.1", 6750}],
      message_handler: handler
    })
  end

  setup do
    HTTPotion.start
    HTTPotion.post("http://127.0.0.1:6751/topic/delete?topic=#{@test_topic}")
    :ok
  end

  test "#start_link establishes a connection to NSQ and processes messages" do
    test_pid = self
    new_test_consumer fn(body, msg) ->
      assert body == "HTTP message"
      assert msg.attempts == 1
      send(test_pid, :handled)
      {:ok}
    end

    HTTPotion.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled)

    HTTPotion.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled)
  end
end
