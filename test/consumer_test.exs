defmodule NSQ.ConsumerTest do
  defmodule EventForwarder do
    use GenEvent

    def handle_event(event, parent) do
      send parent, event
      {:ok, parent}
    end
  end

  use ExUnit.Case, async: false
  doctest NSQ.Consumer
  alias NSQ.Consumer, as: Cons
  alias NSQ.Consumer.Helpers, as: H
  alias HTTPotion, as: HTTP
  alias NSQ.Consumer.Connections
  alias NSQ.Connection, as: Conn
  alias NSQ.ConnInfo
  require Logger

  @test_topic "__nsq_consumer_test_topic__"
  @test_channel1 "__nsq_consumer_test_channel1__"

  setup do
    Logger.configure(level: :warn)
    HTTP.post("http://127.0.0.1:6751/topic/delete?topic=#{@test_topic}")
    HTTP.post("http://127.0.0.1:6761/topic/delete?topic=#{@test_topic}")
    HTTP.post("http://127.0.0.1:6771/topic/delete?topic=#{@test_topic}")
    HTTP.post("http://127.0.0.1:6781/topic/delete?topic=#{@test_topic}")
    :ok
  end

  test "msg_timeout" do
    test_pid = self()
    {:ok, consumer} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      msg_timeout: 1000,
      message_handler: fn(body, _msg) ->
        if body == "too_slow" do
          :timer.sleep(1500)
          send(test_pid, :handled)
        else
          send(test_pid, :handled)
        end
        :ok
      end
    })

    NSQ.Consumer.event_manager(consumer)
      |> GenEvent.add_handler(NSQ.ConsumerTest.EventForwarder, self())

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "hello"])
    assert_receive {:message_finished, _}, 2000

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "too_slow"])
    assert_receive {:message_requeued, _}, 2000
  end

  test "NSQ.Message.touch extends timeout" do
    test_pid = self()
    {:ok, consumer} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      msg_timeout: 1000,
      message_handler: fn(_body, msg) ->
        Task.start_link fn ->
          :timer.sleep 900
          NSQ.Message.touch(msg)
        end
        :timer.sleep(1500)
        send(test_pid, :handled)
        :ok
      end
    })

    NSQ.Consumer.event_manager(consumer)
      |> GenEvent.add_handler(NSQ.ConsumerTest.EventForwarder, self())

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "hello"])

    # Without touch, this message would fail after 1 second. So we test that
    # it takes longer than 1 second but succeeds.
    refute_receive({:message_requeued, _}, 1200)
    refute_received({:message_finished, _})
    assert_receive({:message_finished, _}, 1000)
  end

  test "we don't go over max_in_flight, and keep processing after saturation" do
    test_pid = self()
    {:ok, consumer} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}, {"127.0.0.1", 6760}],
      max_in_flight: 4,
      message_handler: fn(_body, _msg) ->
        send(test_pid, :handled)
        :timer.sleep(300)
        :ok
      end
    })

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "hello"])
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "hello"])
    HTTP.post("http://127.0.0.1:6761/pub?topic=#{@test_topic}", [body: "hello"])
    HTTP.post("http://127.0.0.1:6761/pub?topic=#{@test_topic}", [body: "hello"])
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "hello"])
    HTTP.post("http://127.0.0.1:6761/pub?topic=#{@test_topic}", [body: "hello"])

    :timer.sleep(100)
    [info1, info2] = NSQ.Consumer.conn_info(consumer) |> Map.values
    assert info1.messages_in_flight + info2.messages_in_flight == 4

    assert_receive :handled, 2000
    assert_receive :handled, 2000
    assert_receive :handled, 2000
    assert_receive :handled, 2000
    assert_receive :handled, 2000
    assert_receive :handled, 2000

    :timer.sleep(1000)
    [_info1, _info2] = NSQ.Consumer.conn_info(consumer) |> Map.values
  end

  test "closing the connection waits for outstanding messages and cleanly exits" do
    test_pid = self()
    {:ok, consumer} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      message_handler: fn(body, _msg) ->
        case body do
          "slow" ->
            :timer.sleep(1000)
          "medium" ->
            :timer.sleep(500)
          "fast" ->
            send(test_pid, :handled)
        end
        :ok
      end
    })

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "fast"])
    assert_receive(:handled, 2000)

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "slow"])
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "medium"])
    NSQ.Consumer.close(consumer)
    :timer.sleep(50)
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "fast"])
    refute_receive(:handled, 2000)
  end

  test "notifies the event manager of relevant events" do
    test_pid = self()
    {:ok, consumer} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      message_handler: fn(_body, _msg) ->
        send(test_pid, :handled)
        :ok
      end
    })

    NSQ.Consumer.event_manager(consumer)
      |> GenEvent.add_handler(NSQ.ConsumerTest.EventForwarder, self())

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)

    assert_receive({:message, %NSQ.Message{}}, 2000)
    assert_receive({:message_finished, %NSQ.Message{}}, 2000)
  end

  test "updating connection stats" do
    {:ok, consumer} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      lookupd_poll_interval: 500,
      nsqds: ["127.0.0.1:6750"],
      message_handler: fn(body, _msg) ->
        :timer.sleep(1000)
        case body do
          "ok" -> :ok
          "req" -> :req
          "req2000" -> {:req, 2000}
          "backoff" -> {:req, 2000, true}
          "fail" -> :fail
        end
      end
    })

    NSQ.Consumer.event_manager(consumer)
      |> GenEvent.add_handler(NSQ.ConsumerTest.EventForwarder, self())

    [info] = NSQ.Consumer.conn_info(consumer) |> Map.values
    previous_timestamp = info.last_msg_timestamp
    :timer.sleep(1000)

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "ok"])
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "req"])
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "req2000"])
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "fail"])
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "backoff"])

    assert_receive({:message, _}, 2000)
    assert_receive({:message, _}, 2000)
    assert_receive({:message, _}, 2000)
    assert_receive({:message, _}, 2000)
    assert_receive({:message, _}, 2000)
    [info] = NSQ.Consumer.conn_info(consumer) |> Map.values
    assert info.messages_in_flight == 5

    assert_receive({:message_finished, _}, 2000)
    assert_receive({:message_requeued, _}, 2000)
    assert_receive({:message_requeued, _}, 2000)
    assert_receive({:message_finished, _}, 2000)
    assert_receive({:message_requeued, _}, 2000)

    :timer.sleep(50)
    [info] = NSQ.Consumer.conn_info(consumer) |> Map.values
    assert info.messages_in_flight == 0
    assert info.requeued_count == 3
    assert info.finished_count == 1
    assert info.failed_count == 1
    assert info.last_msg_timestamp > previous_timestamp
  end

  test "a connection is terminated, cleaned up, and restarted when the tcp connection closes" do
    test_pid = self()
    {:ok, cons_sup_pid} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      lookupd_poll_interval: 500,
      nsqds: [{"127.0.0.1", 6750}],
      message_handler: fn(_body, _msg) ->
        send(test_pid, :handled)
        :ok
      end
    })

    # Send a message so we can be sure the connection is up and working first.
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)

    # Abruptly close the connection
    cons = Cons.get(cons_sup_pid)
    [conn1] = Connections.get(cons)
    conn_state = Conn.get_state(conn1)

    Logger.warn "Closing socket as part of test..."
    Socket.Stream.close(conn_state.socket)

    # Normally dead connections hang around until the next discovery loop run,
    # but if we waited that long, we wouldn't be able to check if it was
    # actually dead. So we clear dead connections manually here.
    :timer.sleep(100)
    GenServer.call(cons, :delete_dead_connections)
    assert length(Connections.get(cons)) == 0

    # Wait for the new connection to come up. It should be different from the
    # old one.
    :timer.sleep(600)
    [conn2] = Connections.get(cons)
    assert conn1 != conn2

    # Send another message so we can verify the new connection is working.
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)
  end

  test "establishes a connection to NSQ and processes messages" do
    test_pid = self()
    NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)
  end

  test "discovery via nsqlookupd" do
    test_pid = self()
    {:ok, _} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      lookupd_poll_interval: 500,
      nsqlookupds: ["127.0.0.1:6771", "127.0.0.1:6781"],
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    HTTP.post("http://127.0.0.1:6761/pub?topic=#{@test_topic}", [body: "HTTP message"])

    assert_receive(:handled, 2000)
    assert_receive(:handled, 2000)
  end

  test "#start_link lives when given a bad address and not able to reconnect" do
    test_pid = self()
    Process.flag(:trap_exit, true)
    {:ok, consumer} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 7777}],
      max_reconnect_attempts: 0,
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })
    GenServer.call(NSQ.Consumer.get(consumer), :delete_dead_connections)
    [] = NSQ.Consumer.conn_info(consumer) |> Map.values
  end


  test "#start_link lives when given a bad address but able to reconnect" do
    test_pid = self()
    {:ok, consumer} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 7777}],
      max_reconnect_attempts: 2,
      lookupd_poll_interval: 500,
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })
    [conn] = Connections.get(NSQ.Consumer.get(consumer))
    conn_state = NSQ.Connection.get_state(conn)
    assert conn_state.connect_attempts == 1
    :timer.sleep(700)
    conn_state = NSQ.Connection.get_state(conn)
    assert conn_state.connect_attempts == 2
  end


  test "receives messages from mpub" do
    test_pid = self()
    NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
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
    test_pid = self()
    NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      message_handler: fn(_body, _msg) ->
        :timer.sleep(1000)
        send(test_pid, :handled)
        :ok
      end
    })

    Enum.map 1..1000, fn(_i) ->
      HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    end

    assert_receive_n_times(:handled, 1000, 2000)
  end

  # NOTE: For this test, it's important that the requeue delay is a few hundred
  # milliseconds higher than the backoff interval (200ms) so that we have a
  # chance to measure the RDY count for each connection.
  test "when a message raises an exception, goes through the backoff process" do
    {:ok, run_counter} = Agent.start_link(fn -> 0 end)
    {:ok, sup_pid} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      backoff_strategy: :test, # fixed 200ms for testing
      max_in_flight: 100,
      nsqds: [{"127.0.0.1", 6750}, {"127.0.0.1", 6760}],

      # This handler should always requeue with backoff twice, then succeed.
      # This will let us test the full backoff flow: start, continue, and
      # resume.
      message_handler: fn(_body, _msg) ->
        Agent.update(run_counter, fn(count) -> count + 1 end)
        run_count = Agent.get(run_counter, fn(count) -> count end)
        cond do
          run_count == 1 ->
            :ok
          run_count < 4 ->
            {:req, 500, true}
          true ->
            :ok
        end
      end
    })

    NSQ.Consumer.event_manager(sup_pid)
      |> GenEvent.add_handler(NSQ.ConsumerTest.EventForwarder, self())

    consumer = Cons.get(sup_pid)
    cons_state = Cons.get_state(consumer)
    [conn1, conn2] = Connections.get(cons_state)

    # We start off with RDY=1 for each connection. It would get naturally
    # bumped when it runs maybe_update_rdy after processing the first message.
    assert H.total_rdy_count(cons_state) == 2
    assert cons_state.backoff_counter == 0
    assert cons_state.backoff_duration == 0
    [1, 1] = ConnInfo.fetch(cons_state, conn1, [:rdy_count, :last_rdy])
    [1, 1] = ConnInfo.fetch(cons_state, conn2, [:rdy_count, :last_rdy])

    # Send one successful message through so our subsequent timing is more
    # predictable.
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive({:message_finished, _}, 5000)

    # Our message handler enters into backoff mode and requeues the message.
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive({:message_requeued, _}, 2000)
    assert_receive(:backoff, 1000)

    # Assert that we're now in backoff mode.
    cons_state = Cons.get_state(consumer)
    assert cons_state.backoff_counter == 1
    assert cons_state.backoff_duration == 200
    [0, 0] = ConnInfo.fetch(cons_state, conn1, [:rdy_count, :last_rdy])
    [0, 0] = ConnInfo.fetch(cons_state, conn2, [:rdy_count, :last_rdy])
    assert H.total_rdy_count(cons_state) == 0

    # Wait ~200ms for "test the waters" mode. In this mode, one random
    # connection has RDY set to 1. NSQD will immediately follow up by sending
    # the message we requeued again.
    :timer.sleep(250)
    cons_state = Cons.get_state(consumer)
    assert 1 == ConnInfo.fetch(cons_state, conn1, :rdy_count) +
      ConnInfo.fetch(cons_state, conn2, :rdy_count)
    assert 1 == ConnInfo.fetch(cons_state, conn1, :last_rdy) +
      ConnInfo.fetch(cons_state, conn2, :last_rdy)
    assert H.total_rdy_count(cons_state) == 1
    assert_receive({:message_requeued, _}, 5000)
    assert_receive(:backoff, 100)

    # When the message handler fails again, we go back to backoff mode.
    cons_state = Cons.get_state(consumer)
    [0, 0] = ConnInfo.fetch(cons_state, conn1, [:rdy_count, :last_rdy])
    [0, 0] = ConnInfo.fetch(cons_state, conn2, [:rdy_count, :last_rdy])
    assert H.total_rdy_count(cons_state) == 0

    # Then we'll go into "test the waters mode" again in 200ms.
    :timer.sleep(250)
    cons_state = Cons.get_state(consumer)
    assert 1 == ConnInfo.fetch(cons_state, conn1, :rdy_count) +
      ConnInfo.fetch(cons_state, conn2, :rdy_count)
    assert 1 == ConnInfo.fetch(cons_state, conn1, :last_rdy) +
      ConnInfo.fetch(cons_state, conn2, :last_rdy)
    assert H.total_rdy_count(cons_state) == 1

    # After the message handler runs successfully, it decrements the
    # backoff_counter. We need one more successful message to decrement the
    # counter to 0 and leave backoff mode, so let's send one.
    assert_receive({:message_finished, _}, 2000)

    # Send a successful message and leave backoff mode! (I hope!)
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive({:message_finished, _}, 2000)
    assert_receive(:resume, 100)
    cons_state = Cons.get_state(consumer)
    [50, 50] = ConnInfo.fetch(cons_state, conn1, [:rdy_count, :last_rdy])
    [50, 50] = ConnInfo.fetch(cons_state, conn2, [:rdy_count, :last_rdy])
    assert H.total_rdy_count(cons_state) == 100
  end

  test "retry_rdy flow" do
    # Easiest way to trigger this flow is for max_in_flight to be 0, then try
    # to send RDY 1. That will trigger a retry, after which we can change the
    # max_in_flight so it succeeds next time.

    test_pid = self()
    {:ok, cons_sup_pid} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      max_in_flight: 0,
      rdy_retry_delay: 300,
      message_handler: fn(_body, _msg) ->
        send(test_pid, :handled)
        :ok
      end
    })
    cons = Cons.get(cons_sup_pid)
    [conn] = Connections.get(cons)

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    refute_receive :handled, 500
    cons_state = Cons.get_state(cons)
    assert ConnInfo.fetch(cons_state, conn, :retry_rdy_pid) == nil

    GenServer.call(cons, {:update_rdy, conn, 1})
    refute_receive :handled, 500
    cons_state = Cons.get_state(cons)
    assert ConnInfo.fetch(cons_state, conn, :retry_rdy_pid) |> is_pid

    Cons.change_max_in_flight(cons_sup_pid, 1)
    assert_receive :handled, 500
    cons_state = Cons.get_state(cons)
    assert ConnInfo.fetch(cons_state, conn, :retry_rdy_pid) == nil
  end

  test "rdy redistribution when number of connections > max in flight" do
    test_pid = self()
    {:ok, cons_sup_pid} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}, {"127.0.0.1", 6760}],
      rdy_redistribute_interval: 100,
      low_rdy_idle_timeout: 1000,
      max_in_flight: 1,
      message_handler: fn(_body, _msg) ->
        send(test_pid, :handled)
        :ok
      end
    })
    cons = Cons.get(cons_sup_pid)
    cons_state = Cons.get_state(cons)
    [conn1, conn2] = Connections.get(cons)
    assert H.total_rdy_count(cons_state) == 1
    assert 1 == ConnInfo.fetch(cons_state, conn1, :rdy_count) +
      ConnInfo.fetch(cons_state, conn2, :rdy_count)
    :timer.sleep(1500)

    IO.puts "Letting RDY redistribute 10 times..."
    {conn1_rdy, conn2_rdy} = Enum.reduce 1..10, {0, 0}, fn(i, {rdy1, rdy2}) ->
      :timer.sleep(100)
      result = {
        rdy1 + ConnInfo.fetch(cons_state, conn1, :rdy_count),
        rdy2 + ConnInfo.fetch(cons_state, conn2, :rdy_count)
      }
      IO.puts "#{i}: #{inspect result}"
      result
    end

    IO.puts "Distribution: #{conn1_rdy} : #{conn2_rdy}"
    assert conn1_rdy == 5
    assert conn2_rdy == 5
  end

  test "works with tls" do
    test_pid = self()
    NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      tls_v1: true,
      tls_insecure_skip_verify: true,
      tls_cert: "#{__DIR__}/ssl_keys/elixir_nsq.crt",
      tls_key: "#{__DIR__}/ssl_keys/elixir_nsq.key",
      tls_min_version: :tlsv1,
      max_reconnect_attempts: 0,
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)
  end

  unless System.get_env("CI") == "true" do
    test "fails when tls_insecure_skip_verify is false" do
      test_pid = self()
      NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
        nsqds: [{"127.0.0.1", 6750}],
        tls_v1: true,
        tls_insecure_skip_verify: false,
        tls_cert: "#{__DIR__}/ssl_keys/elixir_nsq.crt",
        tls_key: "#{__DIR__}/ssl_keys/elixir_nsq.key",
        max_reconnect_attempts: 0,
        message_handler: fn(body, msg) ->
          assert body == "HTTP message"
          assert msg.attempts == 1
          send(test_pid, :handled)
          :ok
        end
      })

      HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
      refute_receive(:handled, 2000)
    end
  end

  test "starved" do
    {:ok, consumer} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      max_in_flight: 2,
      message_handler: fn(_, _) ->
        :timer.sleep(1000)
        :ok
      end
    })

    NSQ.Consumer.event_manager(consumer)
      |> GenEvent.add_handler(NSQ.ConsumerTest.EventForwarder, self())

    # Nothing is in flight, not starved
    assert NSQ.Consumer.starved?(consumer) == false

    # One message in flight, 50% of last_rdy, not starved
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive({:message, _}, 2000)
    assert NSQ.Consumer.starved?(consumer) == false

    # Two messages in flight, 100% of last_rdy, __starved__
    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive({:message, _}, 2000)
    assert NSQ.Consumer.starved?(consumer) == true

    # Messages are done, back to 0 in flight, not starved
    assert_receive({:message_finished, _}, 2000)
    assert_receive({:message_finished, _}, 2000)
    :timer.sleep 100
    assert NSQ.Consumer.starved?(consumer) == false
  end

  test "auth" do
    test_pid = self()
    NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6765}],
      auth_secret: "abc",
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })

    HTTP.post("http://127.0.0.1:6766/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)

    HTTP.post("http://127.0.0.1:6766/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)
  end

  test "deflate" do
    test_pid = self()
    NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6750}],
      deflate: true,
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)

    HTTP.post("http://127.0.0.1:6751/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)
  end

  test "deflate + ssl + auth" do
    test_pid = self()
    NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      nsqds: [{"127.0.0.1", 6765}],
      deflate: true,
      tls_v1: true,
      tls_insecure_skip_verify: true,
      tls_cert: "#{__DIR__}/ssl_keys/elixir_nsq.crt",
      tls_key: "#{__DIR__}/ssl_keys/elixir_nsq.key",
      auth_secret: "abc",
      max_reconnect_attempts: 0,
      message_handler: fn(body, msg) ->
        assert body == "HTTP message"
        assert msg.attempts == 1
        send(test_pid, :handled)
        :ok
      end
    })

    HTTP.post("http://127.0.0.1:6766/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)

    HTTP.post("http://127.0.0.1:6766/pub?topic=#{@test_topic}", [body: "HTTP message"])
    assert_receive(:handled, 2000)
  end


  test "stop_connections actually stops connections, without throwing an error" do
    {:ok, cons_sup_pid} = NSQ.Consumer.Supervisor.start_link(@test_topic, @test_channel1, %NSQ.Config{
      msg_timeout: 1000,
      nsqds: [{"127.0.0.1", 6750}],
      message_handler: fn(_body, _msg) -> :ok end
    })

    consumer = NSQ.Consumer.get(cons_sup_pid)
    state = NSQ.Consumer.get_state(consumer)
    connections = NSQ.Consumer.Connections.get(consumer)

    assert ConnInfo.all(state.conn_info_pid) != %{}
    NSQ.Consumer.Connections.stop_connections(connections, consumer, state)
    assert ConnInfo.all(state.conn_info_pid) == %{}
    assert NSQ.Consumer.Connections.get(consumer) == []
  end
end
