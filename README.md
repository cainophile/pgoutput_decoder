# PgoutputDecoder

Parses logical replication messages from Postgres pgoutput plugin

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `pgoutput_decoder` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:pgoutput_decoder, "~> 0.1.0"}
  ]
end
```

## Usage

To decode messages from Postgres, simply pass them to `PgoutputDecoder.decode_message` like so:

```
iex(1)> PgoutputDecoder.decode_message(
             <<82, 0, 0, 96, 0, 112, 117, 98, 108, 105, 99, 0, 102, 111, 111, 0, 100, 0, 2, 0, 98,
               97, 114, 0, 0, 0, 0, 25, 255, 255, 255, 255, 1, 105, 100, 0, 0, 0, 0, 23, 255, 255,
               255, 255>>
           )
%PgoutputDecoder.Messages.Relation{
  columns: [
    %PgoutputDecoder.Messages.Relation.Column{
      flags: [],
      name: "bar",
      type: :text,
      type_modifier: 4294967295
    },
    %PgoutputDecoder.Messages.Relation.Column{
      flags: [:key],
      name: "id",
      type: :int4,
      type_modifier: 4294967295
    }
  ],
  id: 24576,
  name: "foo",
  namespace: "public",
  replica_identity: :default
}
```

