package registry_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	avro "github.com/ln80/avro/v2"
	structavro "github.com/ln80/struct-avro"
	"github.com/ln80/struct-avro/fs"
	"github.com/ln80/struct-avro/registry"
	"github.com/ln80/struct-avro/testdata/rich"
)

type fuzzEnvelope struct {
	ID    string        `avro:"ID"`
	Type  string        `avro:"Type"`
	At    int64         `avro:"At"`
	Dests []string      `avro:"Dests"`
	TTL   time.Duration `avro:"TTL"`
	Data  any           `avro:"Data" ev:",inject=union"`
}

// FuzzDecode decodes untrusted bytes with a rich envelope schema. Decoding must
// fail or succeed without panicking or allocating beyond the API limits.
func FuzzDecode(f *testing.F) {
	ctx := context.Background()
	api := structavro.NewAPI()
	sch, err := structavro.BuildEnvelopeSchema(api, structavro.EnvelopeConfig{
		Namespace: "rich",
		RootType:  reflect.TypeOf(fuzzEnvelope{}),
		RootName:  "events",
		InjectKey: "union",
		Entries: []structavro.RecordEntry{
			{Name: "rich.ProfileCreated", Type: reflect.TypeOf(rich.ProfileCreated{}), Sample: rich.SampleCreated},
			{Name: "rich.ProfileDeleted", Type: reflect.TypeOf(rich.ProfileDeleted{}), Sample: rich.SampleDeleted},
			{Name: "rich.ProfileUpdated", Type: reflect.TypeOf(rich.ProfileUpdated{}), Sample: rich.SampleUpdated},
		},
	})
	if err != nil {
		f.Fatal(err)
	}
	store := fs.NewDirAdapter(f.TempDir())
	reg, err := registry.Open(ctx, store, registry.WithAPI(api), registry.WithCurrent(sch), registry.WithPersistCurrent())
	if err != nil {
		f.Fatal(err)
	}
	one, err := reg.Encode(fuzzEnvelope{ID: "1", Type: "rich.ProfileCreated", Data: rich.SampleCreated})
	if err != nil {
		f.Fatal(err)
	}
	batch, err := reg.EncodeBatch([]fuzzEnvelope{{ID: "1", Data: rich.SampleDeleted}, {ID: "2"}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(one, false)
	f.Add(batch, true)
	f.Add([]byte{}, false)

	f.Fuzz(func(t *testing.T, data []byte, isBatch bool) {
		if isBatch {
			var out []fuzzEnvelope
			_, _ = reg.DecodeBatch(ctx, data, &out)
			return
		}
		var out fuzzEnvelope
		_, _ = reg.Decode(ctx, data, &out)
	})
}

type fuzzFetcher struct{ schema string }

func (s fuzzFetcher) Get(context.Context, string) (string, error) { return s.schema, nil }
func (s fuzzFetcher) GetByDefinition(context.Context, avro.Schema) (string, error) {
	return "", registry.ErrSchemaNotFound
}

// FuzzFetchedSchema feeds arbitrary schema text from a store to the registry.
// It must be rejected or accepted, never panic.
func FuzzFetchedSchema(f *testing.F) {
	f.Add(`{"type":"record","name":"events","namespace":"a","fields":[{"name":"ID","type":"string"}]}`)
	f.Add(`"string"`)
	f.Add(`{"type":"record","name":"r","fields":[{"name":"x","type":{"type":"array","items":"r"}}]}`)
	f.Add(`{`)
	current := avro.MustParse(`{"type":"record","name":"events","namespace":"a","fields":[{"name":"ID","type":"string"},{"name":"At","type":"long","default":0}]}`).(*avro.RecordSchema)
	f.Fuzz(func(t *testing.T, schema string) {
		reg, err := registry.Open(context.Background(), fuzzFetcher{schema}, registry.WithWireFormatter(fs.NewWireFormatter()), registry.WithReaderSchema(current))
		if err != nil {
			t.Fatal(err)
		}
		_, _, _, _ = reg.GetSchema(context.Background(), "0123456789abcdef")
	})
}
