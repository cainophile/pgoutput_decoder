defmodule PgoutputDecoderTest do
  use ExUnit.Case
  alias PgoutputDecoder.Messages.{Begin, Commit, Origin}

  test "decodes begin messages" do
    {:ok, expected_dt_no_microseconds, 0} = DateTime.from_iso8601("2019-07-18T17:02:35Z")
    expected_dt = DateTime.add(expected_dt_no_microseconds, 726_322, :microsecond)

    assert PgoutputDecoder.decode_message(
             <<66, 0, 0, 0, 2, 167, 244, 168, 128, 0, 2, 48, 246, 88, 88, 213, 242, 0, 0, 2, 107>>
           ) == %Begin{commit_timestamp: expected_dt, final_lsn: {2, 2_817_828_992}, xid: 619}
  end

  test "decodes commit messages" do
    {:ok, expected_dt_no_microseconds, 0} = DateTime.from_iso8601("2019-07-18T17:02:35Z")
    expected_dt = DateTime.add(expected_dt_no_microseconds, 726_322, :microsecond)

    assert PgoutputDecoder.decode_message(
             <<67, 0, 0, 0, 0, 2, 167, 244, 168, 128, 0, 0, 0, 2, 167, 244, 168, 176, 0, 2, 48,
               246, 88, 88, 213, 242>>
           ) == %Commit{
             flags: [],
             lsn: {2, 2_817_828_992},
             end_lsn: {2, 2_817_829_040},
             commit_timestamp: expected_dt
           }
  end

  test "decodes origin messages" do
    assert PgoutputDecoder.decode_message(<<79, 0, 0, 0, 2, 167, 244, 168, 128>> <> "Elmer Fud") ==
             %Origin{
               origin_commit_lsn: {2, 2_817_828_992},
               name: "Elmer Fud"
             }
  end
end
