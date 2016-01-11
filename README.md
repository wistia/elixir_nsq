# elixir_nsq - UNDER CONSTRUCTION

[![Build Status](https://travis-ci.org/wistia/elixir_nsq.svg?branch=master)](https://travis-ci.org/wistia/elixir_nsq)

The goal of this project is to create a production-ready Elixir client library
for NSQ. It is currently in a phase of rapid prototyping, and is _not ready_
for production yet. When that time comes, the readme will reflect it.

See these resources for more info on building client libraries:

- [http://nsq.io/clients/building_client_libraries.html](http://nsq.io/clients/building_client_libraries.html)
- [http://nsq.io/clients/tcp_protocol_spec.html](http://nsq.io/clients/tcp_protocol_spec.html)

### TODO:

- [x] RDY redistribution tests
- [x] RDY retry tests
- [x] Backoff tests
- [x] Test message processing concurrency
- [x] Test discovery via nsqlookupd for consumers
- [x] Test connection failure behavior
- [x] Set connection's max_rdy from IDENTIFY result
- [x] Respect configured read_timeout
- [x] Respect configured write_timeout
- [x] Handle errors reported by NSQD
- [x] Include Procfile for running nsqd/nsqlookupd for tests
- [x] Graceful connection closing
- [x] TLS support
- [ ] Auth support
- [x] Delegates
- [x] Test Message TOUCH
- [x] Remove Connection dependency
- [ ] Implement `NSQ.Consumer.starved?/1`

## Publish Messages

```elixir
{:ok, producer} = NSQ.ProducerSupervisor.start_link("my-topic", %NSQ.Config{
  nsqds: ["127.0.0.1:4150", "127.0.0.1:4151"]
})

# publish to the default topic "my-topic"
NSQ.Producer.pub(producer, "a message")
NSQ.Producer.mpub(producer, ["one", "two", "three"])

# specify a topic
NSQ.Producer.pub(producer, "different-topic", "another message")
NSQ.Producer.mpub(producer, "different-topic", ["four", "five", "six"])

NSQ.Producer.close(producer)
```

## Quick Start

### Consume Messages

The handler should return `:ok` to finish normally, `:req` to requeue the
message, or `{:req, delay}` to specify your own requeue delay. If your message
handler throws an exception, it will automatically be requeued and delayed with
a timeout based on attempts.

```elixir
{:ok, consumer} = NSQ.ConsumerSupervisor.start_link("my-topic", "my-channel", %NSQ.Config{
  nsqlookupds: ["127.0.0.1:4160", "127.0.0.1:4161"],
  message_handler: fn(body, msg) ->
    IO.puts "id: #{msg.id}"
    IO.puts "attempts: #{msg.attempts}"
    IO.puts "timestamp: #{msg.timestamp}"
    :ok
  end
})
```

The message handler can also be a module that implements `handle_message/2`:

```elixir
defmodule MyMsgHandler do
  def handle_message(body, msg) do
    IO.puts "Handled in a module! #{msg.id}"
    :ok
  end
end

{:ok, consumer} = NSQ.ConsumerSupervisor.start_link("my-topic", "my-channel", %NSQ.Config{
  nsqlookupds: ["127.0.0.1:4160", "127.0.0.1:4161"],
  message_handler: MyMsgHandler
})
```

If your message is especially long-running and you know it's not dead, you can
touch it so that NSQ doesn't automatically fail and requeue it.

```elixir
def MyMsgHandler do
  def handle_message(body, msg) do
    spawn_link fn ->
      :timer.sleep(30_000)
      NSQ.Message.touch(msg)
    end
    long_running_operation()
    :ok
  end
end
```

If you're not using nsqlookupd, you can specify nsqds directly:

```elixir
{:ok, consumer} = NSQ.ConsumerSupervisor.start_link("my-topic", "my-channel", %NSQ.Config{
  nsqds: ["127.0.0.1:4150", "127.0.0.1:4151"],
  message_handler: fn(body, msg) ->
    :ok
  end
})
```

## Get notified

NSQ.Consumer and NSQ.Producer provide the function `event_manager/1` so that
you can receive events from the NSQ client. You can keep your own stats/logs
and perform actions based on that info.

```elixir
defmodule EventForwarder do
  use GenEvent

  def handle_event(event, parent) do
    send parent, event
    {:ok, parent}
  end
end

def setup_consumer do
  {:ok, consumer} = NSQ.ConsumerSupervisor.start_link("my-topic", "my-channel", %NSQ.Config{
    nsqds: ["127.0.0.1:4150", "127.0.0.1:4151"],
    message_handler: fn(body, msg) ->
      :ok
    end
  })

  # subscribe to events from the event manager
  NSQ.Consumer.event_manager(consumer)
  |> GenEvent.add_handler(EventForwarder, self)
end
```

Potential event formats are:

- `{:message, NSQ.Message.t}`
- `{:message_finished, NSQ.Message.t}`
- `{:message_requeued, NSQ.Message.t}`
- `:resume`
- `:continue`
- `:backoff`
- `:heartbeat`
- `{:response, binary}`
- `{:error, binary}`
- `{:error, String.t, binary}`

### Supervision Tree

For your convenience, this is the overall process structure of `elixir_nsq`.
In practice, the ConnectionSupervisors and Task.Supervisors don't do much
automatic restarting because NSQ itself is built to handle that. But they are
useful for propagating exit commands and keeping track of all running
processes.

    Consumer Supervisor
      Consumer
        ConnInfo Agent
        ConnectionSupervisor
          Connection
            Task.Supervisor
              Message
              Message
          Connection
            Task.Supervisor
              Message
              Message
      Discovery loop
      RDY redistribution loop

    Producer Supervisor
      Producer
        ConnInfo Agent
        ConnectionSupervisor
          Connection
          Connection

## Running the Tests

The included tests require two nsqds and two nsqlookupds. A Procfile for use
with foreman is included to start these up. If you don't have
[foreman](https://github.com/ddollar/foreman), you'll need to find a way to run
those commands if you want to run the tests.

```bash
foreman start
mix test
```

Note that some tests intentionally cause processes to exit, so you might see
some error logging as part of the tests. As long as they're still passing, that
is considered normal behavior.
