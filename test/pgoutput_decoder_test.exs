defmodule PgoutputDecoderTest do
  use ExUnit.Case
  alias PgoutputDecoder.Messages.{Begin}

  test "decodes begin messages" do
    {:ok, expected_dt_no_microseconds, 0} = DateTime.from_iso8601("2019-07-18T17:02:35Z")
    expected_dt = DateTime.add(expected_dt_no_microseconds, 726_322, :microsecond)

    assert PgoutputDecoder.decode_message(
             <<66, 0, 0, 0, 2, 167, 244, 168, 128, 0, 2, 48, 246, 88, 88, 213, 242, 0, 0, 2, 107>>
           ) == %Begin{commit_timestamp: expected_dt, final_lsn: {2, 2_817_828_992}, xid: 619}
  end
end
