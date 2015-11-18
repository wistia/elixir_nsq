defmodule NSQ.Connection do
  import NSQ.Protocol
  use Connection


  @initial_state %{
    consumer: nil,
    queue: :queue.new,
    socket: nil,
    config: %{},
    num_in_flight: 0,
    nsqd: nil,
    topic: nil,
    channel: nil,
    backoff_counter: 0,
    backoff_duration: 0,
    rdy: 0,
    last_rdy: 0,
    max_rdy: 2500
  }


  def start_link(consumer, nsqd, config, topic, channel \\ nil) do
    state = %{@initial_state | consumer: consumer, nsqd: nsqd, config: config, topic: topic, channel: channel}
    Connection.start_link(__MODULE__, state)
  end


  def init(state) do
    {:connect, nil, state}
  end


  def connect(_info, %{nsqd: {host, port}} = state) do
    opts = [:binary, active: false, deliver: :term, packet: :raw]
    case :gen_tcp.connect(to_char_list(host), port, opts) do
      {:ok, socket} ->
        do_handshake(socket, state.topic, state.channel)
        {:ok, %{state | socket: socket}}
      {:error, reason} ->
        IO.puts "TCP connection error: #{inspect reason}"
        {:backoff, 1000, state}
    end
  end


  def handle_call({:command, cmd}, from, %{queue: q} = state) do
    unless is_binary(cmd), do: cmd = encode(cmd)
    :ok = :gen_tcp.send(state.socket, cmd)
    state = %{state | queue: :queue.in(from, q)}
    {:reply, :ok, state}
  end


  def handle_call({:message_done, _message}, _from, state) do
    {:reply, :ack, %{state | num_in_flight: state.num_in_flight - 1}}
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
        state = %{state | num_in_flight: state.num_in_flight + 1}
        NSQ.Message.process(message, socket, state.config.handler)

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


  def handle_call({:state, prop}, _from, state) do
    {:reply, state[prop], state}
  end


  def get_state(consumer, prop) do
    GenServer.call(consumer, {:state, prop})
  end
end
