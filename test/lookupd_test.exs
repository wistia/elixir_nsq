defmodule NSQ.LookupdTest do
  use ExUnit.Case, async: true
  require Logger

  @test_topic "__nsq_producer_test_topic__"

  test "connect to invalid lookupd" do
    assert NSQ.Lookupd.topics_from_lookupd({"127.0.0.1", 123456},
        @test_topic) == %{data: nil, status_code: nil, status_txt: nil}

  end
end