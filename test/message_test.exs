defmodule NSQ.MessageTest do
  use ExUnit.Case, async: true
  doctest NSQ.Message

  def now do
    :calendar.datetime_to_gregorian_seconds(:calendar.universal_time())
  end

  def build_raw_nsq_data(attrs \\ %{}) do
    timestamp = attrs[:timestamp] || now()
    attempts = attrs[:attempts] || 0
    msg_id = attrs[:id] || String.ljust(SecureRandom.hex(8), 16, ?0)
    data = attrs[:body] || "test data"

    <<timestamp::size(64)>> <>
      <<attempts::size(16)>> <>
      msg_id <>
      data
  end

  test "#from_data given raw data, returns an instance of %NSQ.Message" do
    raw_data = build_raw_nsq_data(body: "hello world", attempts: 1)
    message = NSQ.Message.from_data(raw_data)
    assert %NSQ.Message{} = message
    assert message.body == "hello world"
    assert message.attempts == 1
    assert String.length(message.id) == 16
  end
end
