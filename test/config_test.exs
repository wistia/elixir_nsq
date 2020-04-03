defmodule NSQ.ConfigTest do
  use ExUnit.Case, async: false
  doctest NSQ.Config

  @nsqds_binary ["127.0.0.1:6750", "127.0.0.1:6760"]
  @nsqds_tuple [{"127.0.0.1", 6750}, {"127.0.0.1", 6760}]
  @nsqds_list [["127.0.0.1", 6750], ["127.0.0.1", 6760]]
  @nsqds_invalid [0, 1]

  test "nsqds and nsqlookupds config are correctly normalized" do
    expected = %NSQ.Config{
      nsqds: @nsqds_tuple,
      nsqlookupds: @nsqds_tuple
    }

    {:ok, normalized} = NSQ.Config.normalize(%NSQ.Config{
      nsqds: @nsqds_binary,
      nsqlookupds: @nsqds_binary})
    assert normalized == expected

    {:ok, normalized} = NSQ.Config.normalize(%NSQ.Config{
      nsqds: @nsqds_tuple,
      nsqlookupds: @nsqds_tuple})
    assert normalized == expected

    {:ok, normalized} = NSQ.Config.normalize(%NSQ.Config{
      nsqds: @nsqds_list,
      nsqlookupds: @nsqds_list})
    assert normalized == expected

    assert_raise(RuntimeError, "Invalid host definition 0", fn ->
      NSQ.Config.normalize(%NSQ.Config{
        nsqds: @nsqds_invalid,
        nsqlookupds: @nsqds_invalid
      }) end)
  end

  test "backoff config" do
    valid = %NSQ.Config{backoff_strategy: :exponential}
    invalid = %NSQ.Config{backoff_strategy: :invalid}

    assert NSQ.Config.validate(valid) == {:ok, valid}
    assert NSQ.Config.validate(invalid) == {:error,
      [error: "invalid doesn't match any supported values"]}
  end
end
