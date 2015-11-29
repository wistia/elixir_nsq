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


  test "#new establishes a connection to NSQ and processes messages" do
    test_pid = self
    NSQ.Consumer.new(@test_topic, @test_channel1, %NSQ.Config{
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


  test "#new exits when given a bad address and not able to reconnect" do
    test_pid = self
    Process.flag(:trap_exit, true)
    NSQ.Consumer.new(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 7777}],
      max_reconnect_attempts: 0,
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })
    assert_receive({:EXIT, _pid, {:shutdown, {:failed_to_start_child, NSQ.Consumer, {:econnrefused, _}}}})
  end


  test "#new lives when given a bad address but able to reconnect" do
    test_pid = self
    Process.flag(:trap_exit, true)
    NSQ.Consumer.new(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 7777}],
      max_reconnect_attempts: 1,
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })
    refute_receive({:EXIT, _pid, {:shutdown, _}}, 2000)
  end


  test "receives messages from mpub" do
    test_pid = self
    NSQ.Consumer.new(@test_topic, @test_channel1, %NSQ.Config{
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

  def assert_receive_n_times(msg, times, delay) do
    if times > 0 do
      assert_receive(msg, delay)
      assert_receive_n_times(msg, times - 1, delay)
    end
  end

  test "processes many messages concurrently" do
    test_pid = self
    NSQ.Consumer.new(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      message_handler: fn(body, _msg) ->
        :timer.sleep(1000)
        send(test_pid, :handled)
        :ok
      end
    })

    Enum.map 1..1000, fn(_i) ->
      HTTPotion.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    end

    assert_receive_n_times(:handled, 1000, 2000)
  end

  test "when a message raises an exception, goes through the backoff process" do
    test_pid = self
    {:ok, run_counter} = Agent.start_link(fn -> 0 end)
    {:ok, sup} = NSQ.Consumer.new(@test_topic, @test_channel1, %NSQ.Config{
      backoff_strategy: :quick_test, # fixed 200ms for testing
      max_in_flight: 100,
      nsqds: [{"127.0.0.1", 6750}],
      message_handler: fn(_body, _msg) ->
        Agent.update(run_counter, fn(count) -> count + 1 end)
        send(test_pid, :handled)
        if Agent.get(run_counter, fn(count) -> count end) == 1 do
          raise "whoops"
        else
          :ok
        end
      end
    })
    :timer.sleep(200)
    consumer = NSQ.Consumer.get(sup)
    cons_state = NSQ.Consumer.get_state(consumer)
    [conn] = NSQ.Consumer.connections(cons_state)
    conn_state = NSQ.Connection.get_state(conn)

    # We're in a normal processing state to start.
    assert cons_state.total_rdy_count == 1
    assert cons_state.backoff_counter == 0
    assert cons_state.backoff_duration == 0
    assert conn_state.rdy_count == 1
    assert conn_state.last_rdy == 1

    # A message throws an unhandled exception, so we automatically requeue and
    # enter into a backoff state.
    HTTPotion.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    receive do
      :handled -> :ok
    end
    :timer.sleep(50)

    # Assert that we're now in backoff mode.
    cons_state = NSQ.Consumer.get_state(consumer)
    conn_state = NSQ.Connection.get_state(conn)
    assert cons_state.backoff_counter == 1
    assert cons_state.backoff_duration == 200
    assert conn_state.rdy_count == 0
    assert conn_state.last_rdy == 0
    assert cons_state.total_rdy_count == 0

    # Wait ~200ms for resume to be called, which should put us in "test the
    # waters" mode. NSQD will immediately follow up by sending the message we
    # requeued again.
    :timer.sleep(250)
    cons_state = NSQ.Consumer.get_state(consumer)
    conn_state = NSQ.Connection.get_state(conn)
    assert conn_state.rdy_count == 1
    assert conn_state.last_rdy == 1
    assert cons_state.total_rdy_count == 1

    # After the message handler runs successfully, we move back to a normal
    # state.
    receive do
      :handled -> :ok
    end
    :timer.sleep(50)
    cons_state = NSQ.Consumer.get_state(consumer)
    conn_state = NSQ.Connection.get_state(conn)
    assert conn_state.rdy_count == 100
    assert conn_state.last_rdy == 100
    assert cons_state.total_rdy_count == 100
  end
end
