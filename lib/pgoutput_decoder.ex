defmodule PgoutputDecoder do
  defmodule Messages do
    defmodule(Begin, do: defstruct([:final_lsn, :commit_timestamp, :xid]))
    defmodule(Commit, do: defstruct([:flags, :lsn, :end_lsn, :commit_timestamp]))
    defmodule(Origin, do: defstruct([:origin_commit_lsn, :name]))
  end

  @pg_epoch DateTime.from_iso8601("2000-01-01T00:00:00Z")

  alias Messages.{Begin, Commit, Origin}

  @moduledoc """
  Documentation for PgoutputDecoder.
  """

  @doc """
  Parses logical replication messages from Postgres pgoutput plugin

  ## Examples

      iex> PgoutputDecoder.decode_message(<<66, 0, 0, 0, 2, 167, 244, 168, 128, 0, 2, 48, 246, 88, 88, 213, 242, 0, 0, 2, 107>>)
      %PgoutputDecoder.Messages.Begin{commit_timestamp: #DateTime<2019-07-18 17:02:35Z>, final_lsn: {2, 2817828992}, xid: 619}

  """
  def decode_message(message) when is_binary(message) do
    decode_message_impl(message)
  end

  defp decode_message_impl(
         "B" <>
           <<lsn::binary-8, timestamp::integer-64, xid::integer-32>>
       ) do
    %Begin{
      final_lsn: decode_lsn(lsn),
      commit_timestamp: pgtimestamp_to_timestamp(timestamp),
      xid: xid
    }
  end

  defp decode_message_impl(
         "C" <>
           <<_flags::binary-1, lsn::binary-8, end_lsn::binary-8, timestamp::integer-64>>
       ) do
    %Commit{
      flags: [],
      lsn: decode_lsn(lsn),
      end_lsn: decode_lsn(end_lsn),
      commit_timestamp: pgtimestamp_to_timestamp(timestamp)
    }
  end

  # TODO: Verify this is correct with real data from Postgres
  defp decode_message_impl(
         "O" <>
           <<lsn::binary-8, name::binary>>
       ) do
    %Origin{
      origin_commit_lsn: decode_lsn(lsn),
      name: name
    }
  end

  defp pgtimestamp_to_timestamp(microsecond_offset) when is_integer(microsecond_offset) do
    {:ok, epoch, 0} = @pg_epoch

    DateTime.add(epoch, microsecond_offset, :microsecond)
  end

  defp decode_lsn(<<xlog_file::integer-32, xlog_offset::integer-32>>),
    do: {xlog_file, xlog_offset}
end
