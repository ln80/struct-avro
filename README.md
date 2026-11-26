# struct-avro

Go structs → Avro schemas, with envelope unions, schema persistence, and
backward-compatible evolution.

Module: `github.com/ln80/struct-avro`

This library generates Avro schemas from Go types, builds a root “envelope”
record whose payload field is a union of record types, fingerprints defaults and
`pii` annotations so schema IDs change when those change, and can persist /
resolve schemas through a filesystem-backed registry. It does **not** encode or
decode bytes itself.

## Marshal and unmarshal

Encoding lives in [`github.com/ln80/avro/v2`](https://github.com/ln80/avro).
`NewAPI()` returns a configured `avro.API`; you call `Marshal` / `Unmarshal` on
that API with a schema this library produced.

```text
Go structs  →  SchemaOf / BuildEnvelopeSchema  →  Avro schema
                                                      │
                                                      ▼
                                            NewAPI().Marshal / Unmarshal
                                                      │
                                                      ▼
                                                 Avro bytes
```

Optional schema-id framing (`fs.WireFormatter` + `registry.Registry`) prefixes
the schema ID onto those bytes so a reader can look up the writer schema and
resolve it against the current reader schema.

`api.Register(fullName, value)` (register both the value and a pointer) is
needed only when the payload is a **union**: without it, Avro decodes unknown
union branches as maps instead of your Go structs.

What stayed in event-store: `EventSerializer` and the `avroEvent` envelope type
(`event-store/avro/serializer.go`, `event.go`). Those compose this library’s
API + registry with `event.Envelope`. You do not need them to use struct-avro
standalone.

## Install

```bash
go get github.com/ln80/struct-avro
```

## Example 1 — one record

Flat struct, no envelope, no registry. `avro` tags are not used here: field-name
overrides via `` `avro:"..."` `` apply only on the envelope root.

```go
package main

import (
	"fmt"
	"reflect"

	stravro "github.com/ln80/struct-avro"
)

type User struct {
	ID    string
	Email string
}

func main() {
	schema, err := stravro.SchemaOf(
		reflect.TypeOf(User{}),
		stravro.WithName("User"),
		stravro.WithNamespace("demo"),
	)
	if err != nil {
		panic(err)
	}

	api := stravro.NewAPI()

	in := User{ID: "u1", Email: "a@example.com"}
	b, err := api.Marshal(schema, in)
	if err != nil {
		panic(err)
	}

	var out User
	if err := api.Unmarshal(schema, b, &out); err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", out)
}
```

## Example 2 — envelope union

A root struct holds metadata plus a `Data` field whose schema is injected as a
`[null, …records]` union. Register each record’s full name so unmarshal returns
a concrete struct.

```go
package main

import (
	"fmt"
	"reflect"

	stravro "github.com/ln80/struct-avro"
)

type Envelope struct {
	ID   string `avro:"ID"`
	Type string `avro:"Type"`
	Data any    `avro:"Data" ev:",inject=union"`
}

type AccountOpened struct {
	AccountID string
	Owner     string
}

type MoneyDeposited struct {
	AccountID string
	Amount    int
}

func main() {
	api := stravro.NewAPI()

	entries := []stravro.RecordEntry{
		{
			Name:   "demo.AccountOpened",
			Type:   reflect.TypeOf(AccountOpened{}),
			Sample: AccountOpened{},
		},
		{
			Name:   "demo.MoneyDeposited",
			Type:   reflect.TypeOf(MoneyDeposited{}),
			Sample: MoneyDeposited{},
		},
	}

	// Register value + pointer so the union resolver maps records to Go types.
	for _, e := range entries {
		api.Register(e.Name, e.Sample)
		ptr := reflect.New(e.Type)
		ptr.Elem().Set(reflect.ValueOf(e.Sample))
		api.Register(e.Name, ptr.Interface())
	}

	schema, err := stravro.BuildEnvelopeSchema(api, stravro.EnvelopeConfig{
		Namespace: "demo",
		RootType:  reflect.TypeOf(Envelope{}),
		RootName:  "events",
		InjectKey: "union",
		Entries:   entries,
		Validate:  true,
	})
	if err != nil {
		panic(err)
	}

	in := Envelope{
		ID:   "evt-1",
		Type: "demo.AccountOpened",
		Data: AccountOpened{AccountID: "acc-1", Owner: "alice"},
	}
	b, err := api.Marshal(schema, in)
	if err != nil {
		panic(err)
	}

	var out Envelope
	if err := api.Unmarshal(schema, b, &out); err != nil {
		panic(err)
	}

	opened := out.Data.(*AccountOpened)
	fmt.Printf("%s %+v\n", out.Type, opened)
}
```

## Schema ID framing (optional)

After marshal, prefix the schema ID. Before unmarshal, extract it and resolve
the schema through the registry. `GetSchema` resolves an older writer schema
against the current reader schema when `Setup` has installed one. That is the
rest of the old event-store serializer (minus `event.Envelope`).

```go
import (
	"github.com/ln80/struct-avro/fs"
	"github.com/ln80/struct-avro/registry"
)

wf := fs.NewWireFormatter()
framed, err := wf.AppendSchemaID(payload, schemaID)

id, raw, err := wf.ExtractSchemaID(framed)
_, schema, _, err := reg.GetSchema(ctx, id)
err = api.Unmarshal(schema, raw, &dest)
```

Typical wiring: `fs.NewDirAdapter(dir)` as both `Fetcher` and `Persister`, then
`registry.New(adapter, adapter, fs.NewWireFormatter())`, then
`reg.Setup(ctx, currentSchema, …)`.

## Tooling — generate, check, persist, embed

The `tool` package runs schema maintenance as a job, typically from a small
`go run ./cmd/schemas` command in CI or before a release. You supply the current
schemas (one envelope per namespace); the job does the rest, always in this
order:

1. **GenerateSchemas** — calls your `SchemaBuilder` to produce the current schema
   of each namespace.
2. **CheckCompatibility** — walks every persisted version of each namespace and
   fails if the current schema cannot read data written with *any* of them
   (backward, transitive).
3. **PersistSchemas** — stores the current schema as a new version, unless a
   schema with the same fingerprint already exists.
4. **EmbedSchemas** — copies every version into `Out/<namespace>/`, generates Go
   types from the latest version of each namespace (`Out/<namespace>/events.go`),
   and writes `Out/embed.go`, whose `EmbedFS()` exposes all versions as an
   `embed.FS`. `Out` may be the schema store itself.

Generated files start with `// Code generated by struct-avro. DO NOT EDIT.` and
are formatted like `goimports` output. The base name of `Out` and every
namespace must be valid Go package names. Running the job twice with no schema
change rewrites nothing. A stored version is never overwritten: persisting the
same version with a different schema fails.

```go
package main

import (
	"context"
	"log"
	"os"
	"reflect"

	stravro "github.com/ln80/struct-avro"
	"github.com/ln80/struct-avro/fs"
	"github.com/ln80/struct-avro/tool"
)

func main() {
	ctx := context.Background()

	// The schema store: one folder per namespace, one "<version>@<id>.json" file per version.
	store := fs.NewDirAdapter("./schemas")

	build := func(context.Context) (tool.SchemaMap, error) {
		sch, err := stravro.BuildEnvelopeSchema(stravro.NewAPI(), stravro.EnvelopeConfig{
			Namespace: "demo",
			RootType:  reflect.TypeOf(Envelope{}),
			RootName:  "events",
			InjectKey: "union",
			Entries: []stravro.RecordEntry{
				{Name: "demo.AccountOpened", Type: reflect.TypeOf(AccountOpened{}), Sample: AccountOpened{}},
				{Name: "demo.MoneyDeposited", Type: reflect.TypeOf(MoneyDeposited{}), Sample: MoneyDeposited{}},
			},
			Validate: true,
		})
		if err != nil {
			return nil, err
		}
		return tool.SchemaMap{"demo": sch}, nil
	}

	err := tool.NewJobExecuter(&tool.DefaultPrinter{Output: os.Stdout, Err: os.Stderr}).
		GenerateSchemas(build).
		CheckCompatibility(store).
		PersistSchemas(store, store).
		EmbedSchemas(store, tool.EmbedConfig{Out: "./gen/events"}).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
```

(`Envelope`, `AccountOpened` and `MoneyDeposited` are the types from Example 2.)

Notes:

- Versions are numbered per namespace (`1@…`, `2@…`, …). "Latest" is the highest
  number, not the last file name in lexical order (`9@` < `12@`).
- The printer is optional: `tool.NewJobExecuter(nil)` runs silently
  (`tool.NoPrinter`). Implement `tool.TaskPrinter` for custom output.
- The generated types are plain structs with `avro` (and `pii`) tags. They give
  a consumer, such as a projection, the latest shape of another namespace's
  events without importing the package that owns them. Readers decode older
  versions through the registry, which resolves each writer schema against the
  reader schema.
- `EmbedConfig.Template` replaces the code-generation template, e.g. to add an
  `init()` that registers the generated types with your own event registry.
  `EmbedConfig.UnionField` names the envelope field holding the union (default
  `Data`), and `EmbedConfig.Module` optionally makes `Out` its own Go module.

## Notes

- Pointer fields become Avro `[null, T]` unions; nil maps to null.
- `time.Time` maps to `long` with logical type timestamp-millis.
- Go `int` / `int32` / small unsigned ints map to Avro `int`; `int64` to `long`.
- Field defaults come from the Go zero value unless `RecordEntry.Sample` (or
  `WithDefault`) supplies a richer sample.
- Rename a field for readers with `` `ev:",aliases=OldName"` `` on the new field.
- Changing a field default or a field annotation changes two synthetic
  fingerprint fields on the envelope, which changes the schema ID. Avro's
  canonical form, from which IDs are computed, drops defaults and properties.
  The fields are named `defxyz<hash>` and `sensitivexyz<hash>` unless
  `EnvelopeConfig.Fingerprint` sets other prefixes. Renaming them adds a schema
  version; old data stays readable.
- Field annotations default to the struct-sensitive tags (`sensitive`, `pii`,
  `sens`), written to the `sensitive` Avro property. Set
  `EnvelopeConfig.Annotator` (or `WithFieldAnnotator`) to read your own tags,
  or `NoAnnotations` to turn them off. An invalid tag fails schema generation.
- Integer / duration mappings in `schemaOfPrimitives` are intentionally frozen
  for wire compatibility with already-persisted data; do not change them without
  an explicit schema migration.
