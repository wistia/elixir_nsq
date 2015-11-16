defmodule NSQ.Consumer.Connection do
  import NSQ.Protocol
  use Connection


  @initial_state %{
    consumer: nil,
    queue: :queue.new,
    socket: nil,
    config: nil
  }


  def start_link(consumer) do
    state = %{@initial_state | consumer: consumer, config: consumer.config}
    Connection.start_link(__MODULE__, state)
  end


  def init(state) do
    {:connect, nil, state}
  end


  def connect(_info, %{consumer: consumer, config: config} = state) do
    opts = [:binary, active: false, deliver: :term, packet: :raw]
    [{host, port}|_t] = config.nsqds
    case :gen_tcp.connect(to_char_list(host), port, opts) do
      {:ok, socket} ->
        do_handshake(socket, consumer.topic, consumer.channel)
        {:ok, %{state | socket: socket}}
      {:error, reason} ->
        IO.puts "TCP connection error: #{inspect reason}"
        {:backoff, 1000, state}
    end
  end


  def handle_call({:command, cmd}, from, %{queue: q} = state) do
    :ok = :gen_tcp.send(state.socket, cmd)
    state = %{state | queue: :queue.in(from, q)}
    {:noreply, state}
  end


  def handle_call({:done, message}, from, state) do
    IO.puts "got 'done' for #{message.id}; decrease num in flight messages"
  end


  def handle_info({:tcp, socket, msg}, state) do
    {client, new_queue} = pop_queue(state.queue)

    case decode(msg) do
      {:response, "_heartbeat_"} ->
        :gen_tcp.send(socket, "NOP\n")

      {:response, data} ->
        IO.inspect {"handle response", data}

      {:error, data} ->
        IO.inspect {"handle error", data}

      {:message, data} ->
        message = NSQ.Message.from_data(data)
        NSQ.Message.process(message, socket, state.consumer.config.handler)
        IO.inspect {"handle message", data}

      anything ->
        IO.inspect {"unhandled", anything}
    end

    {:noreply, %{state | queue: new_queue}}
  end


  @spec pop_queue(:queue.t) :: {any, :queue.t}
  defp pop_queue(queue) do
    {elem, new_queue} = :queue.out(queue)

    case elem do
      :empty -> {nil, new_queue}
      {:value, value} -> {value, new_queue}
      anything ->
        IO.inspect {"Unexpected queue element", anything}
        {nil, new_queue}
    end
  end


  defp do_handshake(socket, topic, channel) do
    IO.puts "Send magic protocol version"
    :ok = :gen_tcp.send(socket, encode(:magic_v2))

    IO.puts "Subscribe to #{topic} #{channel}"
    :gen_tcp.send(socket, encode({:sub, topic, channel}))

    IO.puts "Wait for subscription acknowledgment"
    {:ok, ack} = :gen_tcp.recv(socket, 0)
    unless ok_msg?(ack), do: raise "Expected OK, got #{ack}"

    IO.puts "Set mode to active: true so we receive tcp messages as erlang messages."
    :inet.setopts(socket, active: true)

    :gen_tcp.send(socket, encode({:rdy, 1}))
  end
end
