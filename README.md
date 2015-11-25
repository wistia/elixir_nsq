# elixir_nsq - UNDER CONSTRUCTION

The goal of this project is to create a production-ready Elixir client library
for NSQ. It is currently in a phase of rapid prototyping, and is _not ready_
for production yet. When that time comes, the readme will reflect it.

See these resources for more info on building client libraries:

- [http://nsq.io/clients/building_client_libraries.html](http://nsq.io/clients/building_client_libraries.html)
- [http://nsq.io/clients/tcp_protocol_spec.html](http://nsq.io/clients/tcp_protocol_spec.html)

TODO:

- [ ] RDY redistribution tests
- [ ] RDY retry tests
- [ ] Backoff tests
- [ ] Test message processing concurrency
- [ ] Test connection failure behavior
- [ ] Handle errors reported by NSQD
- [ ] TLS support
- [ ] Auth support
- [ ] Delegates

## Publish Messages

```elixir
config = %NSQ.Config{
  nsqds: ["127.0.0.1:4150", "127.0.0.1:4151"]
}
{:ok, producer} = NSQ.Producer.new(config, "my-topic")

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
config = %NSQ.Config{
  nsqlookupds: ["127.0.0.1:4160", "127.0.0.1:4161"],
  message_handler: fn(body, msg) ->
    IO.puts "id: #{msg.id}"
    IO.puts "attempts: #{msg.attempts}"
    IO.puts "timestamp: #{msg.timestamp}"
    :ok
  end
}
{:ok, consumer} = NSQ.Consumer.new(config, "my-topic", "my-channel")
```

The message handler can also be a module that implements `handle_message/2`:

```elixir
defmodule MyMsgHandler do
  def handle_message(body, msg) do
    IO.puts "Handled in a module! #{msg.id}"
    :ok
  end
end

config = %NSQ.Config{
  nsqlookupds: ["127.0.0.1:4160", "127.0.0.1:4161"],
  message_handler: MyMsgHandler
}
{:ok, consumer} = NSQ.Consumer.new(config, "my-topic", "my-channel")
```

If your message is especially long-running and you know it's not dead, you can
touch it so that NSQ doesn't automatically fail and requeue it.

```
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
config = %NSQ.Config{
  nsqds: ["127.0.0.1:4150", "127.0.0.1:4151"],
  message_handler: fn(body, msg) ->
    :ok
  end
}
{:ok, consumer} = NSQ.Consumer.new(config, "my-topic", "my-channel")
```

### Supervision Tree

    Consumer Supervisor
      Consumer
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
        ConnectionSupervisor
          Connection
          Connection
