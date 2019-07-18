defmodule PgoutputDecoder do
  defmodule Messages do
    defmodule(Begin, do: defstruct([:final_lsn, :commit_timestamp, :xid]))
    defmodule(Commit, do: defstruct([:flags, :lsn, :end_lsn, :commit_timestamp]))
    defmodule(Origin, do: defstruct([:origin_commit_lsn, :name]))
    defmodule(Relation, do: defstruct([:id, :namespace, :name, :replica_identity, :columns]))
    defmodule(Unsupported, do: defstruct([:data]))

    defmodule(Relation.Column,
      do: defstruct([:flags, :name, :type, :type_modifier])
    )
  end

  @pg_epoch DateTime.from_iso8601("2000-01-01T00:00:00Z")

  alias Messages.{Begin, Commit, Origin, Relation, Unsupported, Relation.Column}
  alias PgoutputDecoder.OidDatabase

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

  defp decode_message_impl(
         "R" <>
           <<id::integer-32, rest::binary>>
       ) do
    [
      namespace
      | [name | [<<replica_identity::binary-1, _number_of_columns::integer-16, columns::binary>>]]
    ] = String.split(rest, <<0>>, parts: 3)

    # TODO: Handle case where pg_catalog is blank, we should still return the schema as pg_catalog
    friendly_replica_identity =
      case replica_identity do
        "d" -> :default
        "n" -> :nothing
        "f" -> :all_columns
        "i" -> :index
      end

    %Relation{
      id: id,
      namespace: namespace,
      name: name,
      replica_identity: friendly_replica_identity,
      columns: decode_columns(columns)
    }
  end

  defp decode_message_impl(binary), do: %Unsupported{data: binary}

  defp decode_columns(binary, accumulator \\ [])
  defp decode_columns(<<>>, accumulator), do: Enum.reverse(accumulator)

  defp decode_columns(<<flags::integer-8, rest::binary>>, accumulator) do
    [name | [<<data_type_id::integer-32, type_modifier::integer-32, columns::binary>>]] =
      String.split(rest, <<0>>, parts: 2)

    decoded_flags =
      case flags do
        1 -> [:key]
        _ -> []
      end

    decode_columns(columns, [
      %Column{
        name: name,
        flags: decoded_flags,
        type: OidDatabase.name_for_type_id(data_type_id),
        type_modifier: type_modifier
      }
      | accumulator
    ])
  end

  defp pgtimestamp_to_timestamp(microsecond_offset) when is_integer(microsecond_offset) do
    {:ok, epoch, 0} = @pg_epoch

    DateTime.add(epoch, microsecond_offset, :microsecond)
  end

  defp decode_lsn(<<xlog_file::integer-32, xlog_offset::integer-32>>),
    do: {xlog_file, xlog_offset}
end
