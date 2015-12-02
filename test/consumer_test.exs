defmodule NSQ.ConsumerTest do
  use ExUnit.Case, async: true
  doctest NSQ.Consumer
  alias NSQ.Consumer, as: Cons
  alias HTTPotion, as: HTTP
  alias NSQ.Connection, as: Conn
  import NSQ.SharedConnectionInfo
  require Logger

  @test_topic "__nsq_consumer_test_topic__"
  @test_channel1 "__nsq_consumer_test_channel1__"
  @test_channel2 "__nsq_consumer_test_channel2__"

  setup do
    Logger.configure(level: :warn)
    HTTP.post("http://127.0.0.1:6751/topic/delete?topic=#{@test_topic}")
    HTTP.post("http://127.0.0.1:6761/topic/delete?topic=#{@test_topic}")
    HTTP.post("http://127.0.0.1:6771/topic/delete?topic=#{@test_topic}")
    HTTP.post("http://127.0.0.1:6781/topic/delete?topic=#{@test_topic}")
    :ok
  end

  test "a connection is terminated, cleaned up, and restarted when the tcp connection closes" do
    test_pid = self
    {:ok, cons_sup_pid} = NSQ.Consumer.new(@test_topic, @test_channel1, %NSQ.Config{
      lookupd_poll_interval: 500,
      nsqds: [{"127.0.0.1", 6750}],
      message_handler: fn(body, msg) ->
        send(test_pid, :handled)
        :ok
      end
    })

    # Send a message so we can be sure the connection is up and working first.
    HTTP.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)

    # Abruptly close the connection
    cons = Cons.get(cons_sup_pid)
    [conn1] = Cons.get_connections(cons)
    conn_state = Conn.get_state(conn1)

    Logger.warn "Closing socket as part of test..."
    :gen_tcp.close(conn_state.socket)

    # Wait for the lookupd loop to run again, at which point it will remove the
    # dead connection and spawn a new one.
    :timer.sleep(500)
    assert length(Cons.get_connections(cons)) == 0

    # Wait for the new connection to come up. It should be different from the
    # old one.
    :timer.sleep(1000)
    [conn2] = Cons.get_connections(cons)
    assert conn1 != conn2

    # Send another message so we can verify the new connection is working.
    HTTP.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)
  end

  test "establishes a connection to NSQ and processes messages" do
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

    HTTP.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)

    HTTP.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)
  end

  test "discovery via nsqlookupd" do
    test_pid = self
    {:ok, cons_sup_pid} = NSQ.Consumer.new(@test_topic, @test_channel1, %NSQ.Config{
      lookupd_poll_interval: 500,
      nsqlookupds: ["127.0.0.1:6771", "127.0.0.1:6781"],
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })

    HTTP.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    HTTP.post("http://127.0.0.1:6761/put?topic=#{@test_topic}", [body: "HTTP message"])

    cons = Cons.get(cons_sup_pid)
    assert_receive(:handled, 2000)
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

    HTTP.post("http://127.0.0.1:6751/mpub?topic=#{@test_topic}", [body: "mpubtest\nmpubtest\nmpubtest"])
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
      message_handler: fn(_body, _msg) ->
        :timer.sleep(1000)
        send(test_pid, :handled)
        :ok
      end
    })

    Enum.map 1..1000, fn(_i) ->
      HTTP.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    end

    assert_receive_n_times(:handled, 1000, 2000)
  end

  test "when a message raises an exception, goes through the backoff process" do
    test_pid = self
    {:ok, run_counter} = Agent.start_link(fn -> 0 end)
    {:ok, sup_pid} = NSQ.Consumer.new(@test_topic, @test_channel1, %NSQ.Config{
      backoff_strategy: :test, # fixed 200ms for testing
      max_in_flight: 100,
      nsqds: [{"127.0.0.1", 6750}, {"127.0.0.1", 6760}],

      # This handler should always requeue with backoff twice, then succeed.
      # This will let us test the full backoff flow: start, continue, and
      # resume.
      message_handler: fn(_body, _msg) ->
        Agent.update(run_counter, fn(count) -> count + 1 end)
        send(test_pid, :handled)
        if Agent.get(run_counter, fn(count) -> count end) < 3 do
          {:req, 1000, true}
        else
          :ok
        end
      end
    })
    :timer.sleep(200)
    consumer = Cons.get(sup_pid)
    cons_state = Cons.get_state(consumer)
    [conn1, conn2] = Cons.get_connections(cons_state)

    # We start off with RDY=1 for each connection. It would get naturally
    # bumped when it runs maybe_update_rdy after processing the first message.
    assert Cons.total_rdy_count(cons_state) == 2
    assert cons_state.backoff_counter == 0
    assert cons_state.backoff_duration == 0
    [1, 1] = fetch_conn_info(cons_state, conn1, [:rdy_count, :last_rdy])
    [1, 1] = fetch_conn_info(cons_state, conn2, [:rdy_count, :last_rdy])

    # Our message handler enters into backoff mode and requeues the message
    # 1 second from now.
    HTTP.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    receive do
      :handled -> :ok
    after
      5100 -> raise "message took too long to run"
    end
    :timer.sleep(50)

    # Assert that we're now in backoff mode.
    cons_state = Cons.get_state(consumer)
    assert cons_state.backoff_counter == 1
    assert cons_state.backoff_duration == 200
    [0, 0] = fetch_conn_info(cons_state, conn1, [:rdy_count, :last_rdy])
    [0, 0] = fetch_conn_info(cons_state, conn2, [:rdy_count, :last_rdy])
    assert Cons.total_rdy_count(cons_state) == 0

    # Wait ~200ms for resume to be called, which should put us in "test the
    # waters" mode. In this mode, one random connection has RDY set to 1. NSQD
    # will immediately follow up by sending the message we requeued again.
    :timer.sleep(250)
    cons_state = Cons.get_state(consumer)
    assert 1 == fetch_conn_info(cons_state, conn1, :rdy_count) +
      fetch_conn_info(cons_state, conn2, :rdy_count)
    assert 1 == fetch_conn_info(cons_state, conn1, :last_rdy) +
      fetch_conn_info(cons_state, conn2, :last_rdy)
    assert Cons.total_rdy_count(cons_state) == 1
    receive do
      :handled -> :ok
    after
      5100 -> raise "waited too long for retry to run"
    end

    # When the message handler fails again, we go back to backoff mode.
    :timer.sleep(50)
    cons_state = Cons.get_state(consumer)
    [0, 0] = fetch_conn_info(cons_state, conn1, [:rdy_count, :last_rdy])
    [0, 0] = fetch_conn_info(cons_state, conn2, [:rdy_count, :last_rdy])
    assert Cons.total_rdy_count(cons_state) == 0

    # Then we'll go into "test the waters mode" again in 200ms.
    :timer.sleep(250)
    cons_state = Cons.get_state(consumer)
    assert 1 == fetch_conn_info(cons_state, conn1, :rdy_count) +
      fetch_conn_info(cons_state, conn2, :rdy_count)
    assert 1 == fetch_conn_info(cons_state, conn1, :last_rdy) +
      fetch_conn_info(cons_state, conn2, :last_rdy)
    assert Cons.total_rdy_count(cons_state) == 1

    # After the message handler runs successfully, it decrements the
    # backoff_counter. We need one more successful message to decrement the
    # counter to 0 and leave backoff mode, so let's send one.
    receive do
      :handled -> :ok
    after
      5100 -> raise "waited too long for retry to run"
    end

    # Send a successful message and leave backoff mode! (I hope!)
    HTTP.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    receive do
      :handled -> :ok
    after
      5100 -> raise "waited too long for retry to run"
    end
    :timer.sleep(500)
    cons_state = Cons.get_state(consumer)
    [50, 50] = fetch_conn_info(cons_state, conn1, [:rdy_count, :last_rdy])
    [50, 50] = fetch_conn_info(cons_state, conn2, [:rdy_count, :last_rdy])
    assert Cons.total_rdy_count(cons_state) == 100
  end

  test "retry_rdy flow" do
    # Easiest way to trigger this flow is for max_in_flight to be 0, then try
    # to send RDY 1. That will trigger a retry, after which we can change the
    # max_in_flight so it succeeds next time.

    test_pid = self
    {:ok, cons_sup_pid} = Cons.new(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      max_in_flight: 0,
      rdy_retry_delay: 300,
      message_handler: fn(_body, _msg) ->
        send(test_pid, :handled)
        :ok
      end
    })
    cons = Cons.get(cons_sup_pid)
    [conn] = Cons.get_connections(cons)

    HTTP.post("http://127.0.0.1:6751/put?topic=#{@test_topic}", [body: "HTTP message"])
    refute_receive :handled, 500
    cons_state = Cons.get_state(cons)
    assert fetch_conn_info(cons_state, conn, :retry_rdy_pid) == nil

    Cons.update_rdy(cons, conn, 1)
    refute_receive :handled, 500
    cons_state = Cons.get_state(cons)
    assert fetch_conn_info(cons_state, conn, :retry_rdy_pid) |> is_pid

    Cons.change_max_in_flight(cons_sup_pid, 1)
    assert_receive :handled, 500
    cons_state = Cons.get_state(cons)
    assert fetch_conn_info(cons_state, conn, :retry_rdy_pid) == nil
  end

  test "rdy redistribution when number of connections > max in flight" do
    test_pid = self
    {:ok, cons_sup_pid} = Cons.new(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}, {"127.0.0.1", 6760}],
      rdy_redistribute_interval: 500,
      low_rdy_idle_timeout: 1000,
      max_in_flight: 1,
      message_handler: fn(_body, _msg) ->
        send(test_pid, :handled)
        :ok
      end
    })
    cons = Cons.get(cons_sup_pid)
    cons_state = Cons.get_state(cons)
    [conn1, conn2] = Cons.get_connections(cons)
    assert Cons.total_rdy_count(cons_state) == 1
    assert 1 == fetch_conn_info(cons_state, conn1, :rdy_count) +
      fetch_conn_info(cons_state, conn2, :rdy_count)
    :timer.sleep(1500)

    IO.puts "Letting RDY redistribute 10 times..."
    {conn1_rdy, conn2_rdy} = Enum.reduce 1..10, {0, 0}, fn(i, {rdy1, rdy2}) ->
      :timer.sleep(1000)
      IO.puts i
      {
        rdy1 + fetch_conn_info(cons_state, conn1, :rdy_count),
        rdy2 + fetch_conn_info(cons_state, conn2, :rdy_count)
      }
    end

    IO.puts "Distribution: #{conn1_rdy} : #{conn2_rdy}"
    assert conn1_rdy > 0
    assert conn2_rdy > 0
    assert conn1_rdy + conn2_rdy == 10
    assert abs(conn1_rdy - conn2_rdy) < 6
  end
end
