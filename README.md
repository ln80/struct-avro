# struct-avro

[![Go Reference](https://pkg.go.dev/badge/github.com/ln80/struct-avro.svg)](https://pkg.go.dev/github.com/ln80/struct-avro)
[![Go Module](https://github.com/ln80/struct-avro/actions/workflows/module.yml/badge.svg)](https://github.com/ln80/struct-avro/actions/workflows/module.yml)

Avro schemas from Go structs, with schema evolution you can rely on.

- `structavro`: generate a record schema from a Go type, or an envelope record
  whose payload is a union of record types.
- `registry`: frame a schema ID into each payload, and decode data written with
  an older schema into the current one.
- `fs`: a schema store on disk or in an `embed.FS`.
- `tool`: check compatibility, persist new versions, and generate Go types and
  an `embed.FS` from the stored versions.

Encoding uses [`github.com/ln80/avro/v2`](https://github.com/ln80/avro).

## Install

```bash
go get github.com/ln80/struct-avro
```

Go 1.25 or later.

## Usage

```go
type Envelope struct {
	ID   string `avro:"ID"`
	Data any    `avro:"Data" ev:",inject=union"`
}

type AccountOpened struct {
	AccountID string
	Owner     string `pii:"data"`
}

api := structavro.NewAPI()
schema, err := structavro.BuildEnvelopeSchema(api, structavro.EnvelopeConfig{
	Namespace: "bank",
	RootType:  reflect.TypeOf(Envelope{}),
	RootName:  "events",
	InjectKey: "union",
	Entries: []structavro.RecordEntry{
		{Name: "bank.AccountOpened", Type: reflect.TypeOf(AccountOpened{})},
	},
})

// Schemas are persisted ahead of deployment (see Tooling) and embedded.
reg, err := registry.Open(ctx, fs.NewAdapter(events.EmbedFS(), ""),
	registry.WithAPI(api),
	registry.WithCurrent(schema),
)

b, err := reg.Encode(Envelope{ID: "1", Data: AccountOpened{AccountID: "a1", Owner: "Ada"}})

var out Envelope
_, err = reg.Decode(ctx, b, &out) // out.Data is *AccountOpened
```

`registry.Open` fails if the store does not hold the current schema. Data
written with an older version is resolved against the current one when decoded.
A reader that handles every namespace and has no current schema uses
`registry.WithLatestReader()` to decode into the latest stored schema of each
namespace.

## Tags

| Tag | Effect |
|---|---|
| `ev:",aliases=Old"` | Field renamed from `Old`. On a blank `_` field: record renamed. |
| `ev:",inject=<key>"` | On the envelope: the field holding the record union. |
| `avro:"Name"` | On the envelope: the Avro field name. |
| `pii`, `sensitive`, `sens` | [struct-sensitive](https://github.com/ln80/struct-sensitive) tags, kept in the schema as the `sensitive` property. Set `EnvelopeConfig.Annotator` to read other tags. |

A change to a field default or annotation changes the schema ID, so it is
persisted as a new version.

## Tooling

```go
store := fs.NewDirAdapter("./schemas") // must exist

err := tool.NewJobExecuter(nil).
	GenerateSchemas(build). // func(ctx) (tool.SchemaMap, error)
	CheckCompatibility(store).
	PersistSchemas(store, store).
	EmbedSchemas(store, tool.EmbedConfig{Out: "./gen/events"}).
	Execute(ctx)
```

The job fails if the current schema cannot read every stored version. It
writes `gen/events/<namespace>/<version>@<id>.json`, `<namespace>/events.go`
and `embed.go`. Versions in file names are zero-padded (`0001@…`) so that a
directory listing is in version order; unpadded names from earlier versions are
read, and renamed when the tool copies them.

## Testing a store

A schema store passes `registrytest.Run` against real storage:

```go
func TestStore(t *testing.T) {
	registrytest.Run(t, func(t *testing.T) registrytest.Store { return newStore(t) })
}
```

## Development

```bash
make test lint security fuzz
```

See [SECURITY.md](SECURITY.md) to report a vulnerability, and
[CHANGELOG.md](CHANGELOG.md) for changes.

## License

[Apache 2.0](LICENSE)
