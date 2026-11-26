package registry_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	avro "github.com/ln80/avro/v2"
	"github.com/ln80/struct-avro/fs"
	"github.com/ln80/struct-avro/registry"
)

const recV1 = `{"type":"record","name":"events","namespace":"bank","fields":[{"name":"ID","type":"string"}]}`
const recV2 = `{"type":"record","name":"events","namespace":"bank","fields":[{"name":"ID","type":"string"},{"name":"At","type":"long","default":7}]}`

type v1 struct {
	ID string `avro:"ID"`
}

type v2 struct {
	ID string `avro:"ID"`
	At int64  `avro:"At"`
}

func parse(t *testing.T, s string) *avro.RecordSchema {
	t.Helper()
	return avro.MustParse(s).(*avro.RecordSchema)
}

// store returns a file store holding v1 and v2.
func store(t *testing.T) (*fs.Adapter, string, string) {
	t.Helper()
	a := fs.NewDirAdapter(t.TempDir())
	id1, err := a.Persist(context.Background(), parse(t, recV1))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := a.Persist(context.Background(), parse(t, recV2))
	if err != nil {
		t.Fatal(err)
	}
	return a, id1, id2
}

func TestOpenAndResolveOldData(t *testing.T) {
	ctx := context.Background()
	a, id1, id2 := store(t)

	old, err := registry.Open(ctx, a, registry.WithCurrent(parse(t, recV1)))
	if err != nil {
		t.Fatal(err)
	}
	b, err := old.Encode(v1{ID: "a"})
	if err != nil {
		t.Fatal(err)
	}

	cur, err := registry.Open(ctx, a, registry.WithCurrent(parse(t, recV2)))
	if err != nil {
		t.Fatal(err)
	}
	if id, _, ok := cur.Current(); !ok || id != id2 {
		t.Fatalf("current: want %s, got %s (%v)", id2, id, ok)
	}

	// v1 data decodes into the v2 shape, with the v2 default.
	var got v2
	id, err := cur.Decode(ctx, b, &got)
	if err != nil {
		t.Fatal(err)
	}
	if id != id1 || got != (v2{ID: "a", At: 7}) {
		t.Fatalf("decode: id %s, value %+v", id, got)
	}

	// Batches.
	bb, err := cur.EncodeBatch([]v2{{ID: "x", At: 1}, {ID: "y", At: 2}})
	if err != nil {
		t.Fatal(err)
	}
	var batch []v2
	if _, err := cur.DecodeBatch(ctx, bb, &batch); err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[1] != (v2{ID: "y", At: 2}) {
		t.Fatalf("batch: %+v", batch)
	}

	// Re-encode with the writer schema of the decoded data.
	again, err := cur.EncodeWithID(ctx, id1, v1{ID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(b) {
		t.Fatal("EncodeWithID must reproduce the original bytes")
	}
}

func TestOpenFailsClosed(t *testing.T) {
	ctx := context.Background()
	a := fs.NewDirAdapter(t.TempDir())

	if _, err := registry.Open(ctx, nil); !errors.Is(err, registry.ErrUnableToSetupRegistry) {
		t.Fatalf("nil store: %v", err)
	}
	var nilAdapter *fs.Adapter
	if _, err := registry.Open(ctx, nilAdapter); !errors.Is(err, registry.ErrUnableToSetupRegistry) {
		t.Fatalf("typed nil store: %v", err)
	}
	if _, err := registry.Open(ctx, stubFetcher{}); !errors.Is(err, registry.ErrUnableToSetupRegistry) {
		t.Fatalf("store without wire format: %v", err)
	}
	// The current schema must be stored.
	_, err := registry.Open(ctx, a, registry.WithCurrent(parse(t, recV1)))
	if !errors.Is(err, registry.ErrUnableToSetupRegistry) || !errors.Is(err, registry.ErrSchemaNotFound) {
		t.Fatalf("missing current schema: %v", err)
	}
	if _, err := registry.Open(ctx, a, registry.WithPersistCurrent()); err == nil {
		t.Fatal("WithPersistCurrent without WithCurrent must fail")
	}
	// Without a persister, persisting fails.
	if _, err := registry.Open(ctx, stubFetcher{}, registry.WithWireFormatter(fs.NewWireFormatter()),
		registry.WithCurrent(parse(t, recV1)), registry.WithPersistCurrent()); !errors.Is(err, registry.ErrCreateOrUpdateSchemaFailed) {
		t.Fatalf("persist without persister: %v", err)
	}
	// With one, the schema is persisted.
	r, err := registry.Open(ctx, a, registry.WithCurrent(parse(t, recV1)), registry.WithPersistCurrent())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := r.Current(); !ok {
		t.Fatal("expect a current schema")
	}
}

func TestReaderOnlyAndNoCurrent(t *testing.T) {
	ctx := context.Background()
	a, id1, _ := store(t)
	w, err := registry.Open(ctx, a, registry.WithCurrent(parse(t, recV1)))
	if err != nil {
		t.Fatal(err)
	}
	b, err := w.Encode(v1{ID: "a"})
	if err != nil {
		t.Fatal(err)
	}

	// A reader schema not in the store: decode only.
	reader := `{"type":"record","name":"events","namespace":"bank","fields":[{"name":"ID","type":"string"},{"name":"At","type":"long","default":9}]}`
	ro, err := registry.Open(ctx, a, registry.WithReaderSchema(parse(t, reader)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ro.Encode(v2{}); !errors.Is(err, registry.ErrNoCurrentSchema) {
		t.Fatalf("reader-only encode: %v", err)
	}
	var got v2
	if _, err := ro.Decode(ctx, b, &got); err != nil || got.At != 9 {
		t.Fatalf("reader-only decode: %+v %v", got, err)
	}

	// No current schema: decode with the writer schema.
	plain, err := registry.Open(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	var w1 v1
	if id, err := plain.Decode(ctx, b, &w1); err != nil || id != id1 || w1.ID != "a" {
		t.Fatalf("plain decode: %s %+v %v", id, w1, err)
	}
	if _, err := plain.Encode(v1{}); !errors.Is(err, registry.ErrNoCurrentSchema) {
		t.Fatalf("plain encode: %v", err)
	}
}

func TestDecodeUnknownSchemaID(t *testing.T) {
	ctx := context.Background()
	a, _, _ := store(t)
	r, err := registry.Open(ctx, a, registry.WithCurrent(parse(t, recV2)))
	if err != nil {
		t.Fatal(err)
	}
	var out v2
	if _, err := r.Decode(ctx, []byte("ffffffffffffffffpayload"), &out); !errors.Is(err, registry.ErrSchemaNotFound) {
		t.Fatalf("unknown id: %v", err)
	}
	if _, err := r.Decode(ctx, []byte("short"), &out); !errors.Is(err, registry.ErrInvalidDataWireFormat) {
		t.Fatalf("short payload: %v", err)
	}
}

// TestRegistryConcurrentDecode decodes from many goroutines to surface data
// races in the cache (run with -race).
func TestRegistryConcurrentDecode(t *testing.T) {
	ctx := context.Background()
	a, _, _ := store(t)
	old, err := registry.Open(ctx, a, registry.WithCurrent(parse(t, recV1)))
	if err != nil {
		t.Fatal(err)
	}
	b, err := old.Encode(v1{ID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := registry.Open(ctx, a, registry.WithCurrent(parse(t, recV2)))
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out v2
			if _, err := r.Decode(ctx, b, &out); err != nil {
				t.Errorf("concurrent decode: %v", err)
			}
		}()
	}
	wg.Wait()
}

type stubFetcher struct{ schema string }

func (s stubFetcher) Get(context.Context, string) (string, error) { return s.schema, nil }
func (s stubFetcher) GetByDefinition(context.Context, avro.Schema) (string, error) {
	return "", registry.ErrSchemaNotFound
}

func TestGetSchema_InvalidFetchedSchemaFailsWithoutPanic(t *testing.T) {
	for name, body := range map[string]string{
		"malformed":    `{"type":`,
		"not a record": `"string"`,
	} {
		t.Run(name, func(t *testing.T) {
			reg, err := registry.Open(context.Background(), stubFetcher{schema: body}, registry.WithWireFormatter(fs.NewWireFormatter()))
			if err != nil {
				t.Fatal(err)
			}
			_, _, _, err = reg.GetSchema(context.Background(), "aaaaaaaaaaaaaaaa")
			if !errors.Is(err, registry.ErrInvalidSchema) {
				t.Fatalf("expect ErrInvalidSchema, got %v", err)
			}
		})
	}
}

func TestLatestReader(t *testing.T) {
	ctx := context.Background()
	a := fs.NewDirAdapter(t.TempDir())
	// v2 renames Owner to Client (alias) and adds At with a default.
	v1 := parse(t, `{"type":"record","name":"events","namespace":"bank","fields":[{"name":"ID","type":"string"},{"name":"Owner","type":"string"}]}`)
	v2 := parse(t, `{"type":"record","name":"events","namespace":"bank","fields":[{"name":"ID","type":"string"},{"name":"Client","type":"string","aliases":["Owner"]},{"name":"At","type":"long","default":7}]}`)
	other := parse(t, `{"type":"record","name":"events","namespace":"shop","fields":[{"name":"ID","type":"string"}]}`)
	for _, s := range []*avro.RecordSchema{v1, v2, other} {
		if _, err := a.Persist(ctx, s); err != nil {
			t.Fatal(err)
		}
	}

	type old struct {
		ID    string `avro:"ID"`
		Owner string `avro:"Owner"`
	}
	type latest struct {
		ID     string `avro:"ID"`
		Client string `avro:"Client"`
		At     int64  `avro:"At"`
	}
	w, err := registry.Open(ctx, a, registry.WithCurrent(v1))
	if err != nil {
		t.Fatal(err)
	}
	b, err := w.Encode(old{ID: "1", Owner: "ada"})
	if err != nil {
		t.Fatal(err)
	}

	r, err := registry.Open(ctx, a, registry.WithLatestReader())
	if err != nil {
		t.Fatal(err)
	}
	var got latest
	if _, err := r.Decode(ctx, b, &got); err != nil {
		t.Fatal(err)
	}
	if got != (latest{ID: "1", Client: "ada", At: 7}) {
		t.Fatalf("decode into the latest schema: %+v", got)
	}

	// Data of another namespace decodes into that namespace's latest schema.
	ws, err := registry.Open(ctx, a, registry.WithCurrent(other))
	if err != nil {
		t.Fatal(err)
	}
	bs, err := ws.Encode(v1Record{ID: "s"})
	if err != nil {
		t.Fatal(err)
	}
	var gotShop v1Record
	if _, err := r.Decode(ctx, bs, &gotShop); err != nil || gotShop.ID != "s" {
		t.Fatalf("other namespace: %+v %v", gotShop, err)
	}
}

type v1Record struct {
	ID string `avro:"ID"`
}

func TestLatestReaderNeedsWalkerAndNoCurrent(t *testing.T) {
	ctx := context.Background()
	if _, err := registry.Open(ctx, stubFetcher{}, registry.WithWireFormatter(fs.NewWireFormatter()), registry.WithLatestReader()); !errors.Is(err, registry.ErrUnableToSetupRegistry) {
		t.Fatalf("store without Walker: %v", err)
	}
	a, _, _ := store(t)
	if _, err := registry.Open(ctx, a, registry.WithCurrent(parse(t, recV1)), registry.WithLatestReader()); !errors.Is(err, registry.ErrUnableToSetupRegistry) {
		t.Fatalf("with a current schema: %v", err)
	}
}
