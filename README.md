# elixir_nsq

`elixir_nsq` is a client library for NSQ. Use it in your Elixir or Erlang
applications to handle messages asynchronously. This library seeks to be
complete, well-tested, and easy to use.

This library used [go-nsq](https://github.com/nsqio/go-nsq) and
[pynsq](https://github.com/nsqio/pynsq) for reference, but is structured to
better fit common Elixir workflows.

To use this, you will need to have [NSQ](http://nsq.io/).

## Publish Messages

```elixir
{:ok, producer} = NSQ.Producer.Supervisor.start_link("my-topic", %NSQ.Config{
  nsqds: ["127.0.0.1:4150", "127.0.0.1:4151"]
})

# publish to the default topic "my-topic"
NSQ.Producer.pub(producer, "a message")
NSQ.Producer.mpub(producer, ["one", "two", "three"])

# specify a topic
NSQ.Producer.pub(producer, "different-topic", "another message")
NSQ.Producer.mpub(producer, "different-topic", ["four", "five", "six"])
```

## Quick Start

### Add to mix.exs

```elixir
defp deps do
  [{:elixir_nsq, "~> 1.2.0"}]
end

defp applications do
  [:logger, :elixir_nsq]
end
```

### Consume Messages

The handler should return `:ok` to finish normally, `:req` to requeue the
message, or `{:req, delay}` to specify your own requeue delay. If your message
handler throws an exception, it will automatically be requeued and delayed with
a timeout based on attempts.

```elixir
{:ok, consumer} = NSQ.Consumer.Supervisor.start_link("my-topic", "my-channel", %NSQ.Config{
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

{:ok, consumer} = NSQ.Consumer.Supervisor.start_link("my-topic", "my-channel", %NSQ.Config{
  nsqlookupds: ["127.0.0.1:4160", "127.0.0.1:4161"],
  message_handler: MyMsgHandler
})
```

If your message is especially long-running and you know it's not dead, you can
touch it so that NSQ doesn't automatically fail and requeue it.

```elixir
def MyMsgHandler do
  def handle_message(body, msg) do
    Task.start_link fn ->
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
{:ok, consumer} = NSQ.Consumer.Supervisor.start_link("my-topic", "my-channel", %NSQ.Config{
  nsqds: ["127.0.0.1:4150", "127.0.0.1:4151"],
  message_handler: fn(body, msg) ->
    :ok
  end
})
```

## Configuration

Check https://github.com/wistia/elixir_nsq/blob/master/lib/nsq/config.ex for
supported config values.

## Get notified

NSQ.Consumer and NSQ.Producer provide the function `event_manager/1` so that
you can receive events from the NSQ client. You can keep your own stats/logs
and perform actions based on that info.

```elixir
defmodule EventForwarder do
    @behaviour :gen_event

    def init(args) do
      {:ok, args}
    end

    def handle_event(event, parent) do
      send parent, event
      {:ok, parent}
    end

    def handle_call(_event, _state) do
      raise "not implemented"
    end
end

def setup_consumer do
  {:ok, consumer} = NSQ.Consumer.Supervisor.start_link("my-topic", "my-channel", %NSQ.Config{
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
- `{:error, String.t, binary}`

### Supervision Tree

For your convenience, this is the overall process structure of `elixir_nsq`.
In practice, the Connection.Supervisors and Task.Supervisors don't do much
automatic restarting because NSQ itself is built to handle that. But they are
useful for propagating exit commands and keeping track of all running
processes.

    Consumer Supervisor
      Consumer
        ConnInfo Agent
        Connection.Supervisor
          Connection
            Message.Supervisor
              Message
              Message
          Connection
            Message.Supervisor
              Message
              Message
      Connection discovery loop
      RDY redistribution loop

    Producer Supervisor
      Producer
        ConnInfo Agent
        Connection.Supervisor
          Connection
          Connection

## Running the Tests

The included tests require two nsqds and two nsqlookupds. A Procfile for use
with foreman is included to start these up. If you don't have
[foreman](https://github.com/ddollar/foreman), you'll need to find a way to run
those commands if you want to run the tests.

If you are using nsq < 1.0.0

```bash
WORKER_ID=worker-id foreman start
mix test
```

If you are using nsq >= 1.0.0

```bash
WORKER_ID=node-id foreman start
mix test
```

Note that some tests intentionally cause processes to exit, so you might see
some error logging as part of the tests. As long as they're still passing, that
is considered normal behavior.


## Known Issues

- Snappy cannot be supported because existing NIFs cannot correctly decompress
  the nsqd stream. I believe they need support for skipping the checksum.
