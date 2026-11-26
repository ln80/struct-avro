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
const recV2 = `{"type":"record","name":"events","namespace":"bank","fields":[{"name":"ID","type":"string"},{"name":"At","type":"long","default":0}]}`

type idGetter string

func (g idGetter) AVROSchemaID() string { return string(g) }

func newReg(t *testing.T) (*registry.Registry, *fs.Adapter, string) {
	t.Helper()
	dir := t.TempDir()
	a := fs.NewDirAdapter(dir)
	reg := registry.New(a, a, fs.NewWireFormatter())
	return reg, a, dir
}

// TestRegistrySetupAndResolve persists v1, sets v2 as current, and verifies that a
// payload tagged with v1's id is resolved (reader=v2, writer=v1) and cached.
func TestRegistrySetupAndResolve(t *testing.T) {
	ctx := context.Background()
	reg, a, _ := newReg(t)

	s1 := avro.MustParse(recV1).(*avro.RecordSchema)
	s2 := avro.MustParse(recV2).(*avro.RecordSchema)

	id1, err := a.Persist(ctx, s1)
	if err != nil {
		t.Fatalf("persist v1: %v", err)
	}
	id2, err := a.Persist(ctx, s2)
	if err != nil {
		t.Fatalf("persist v2: %v", err)
	}

	if err := reg.Setup(ctx, s2, func(c *registry.RegistryConfig) { c.PersistCurrent = true }); err != nil {
		t.Fatalf("setup: %v", err)
	}

	// GetCurrent returns the current (v2) schema once Setup completed.
	gotID, cur, batch, err := reg.GetCurrent(ctx, idGetter("ignored-when-current-set"))
	if err != nil {
		t.Fatalf("get current: %v", err)
	}
	if gotID != id2 {
		t.Fatalf("current id: want %q, got %q", id2, gotID)
	}
	if cur == nil || batch == nil {
		t.Fatal("current/batch schema is nil")
	}

	// Resolving the older v1 id must succeed against the v2 reader schema.
	rid, rsch, _, err := reg.GetSchema(ctx, id1)
	if err != nil {
		t.Fatalf("get schema v1: %v", err)
	}
	if rid != id1 || rsch == nil {
		t.Fatalf("resolved id: want %q, got %q (sch nil=%v)", id1, rid, rsch == nil)
	}
}

// TestRegistryWireRoundTrip exercises the embedded WireFormatter via the registry.
func TestRegistryWireRoundTrip(t *testing.T) {
	reg, _, _ := newReg(t)
	id := "0123456789abcdef" // 16 chars
	payload := []byte("encoded-bytes")

	framed, err := reg.AppendSchemaID(payload, id)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	gotID, data, err := reg.ExtractSchemaID(framed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if gotID != id || string(data) != string(payload) {
		t.Fatalf("round trip: got (%q,%q), want (%q,%q)", gotID, data, id, payload)
	}
}

// TestRegistryConcurrentGet hammers GetSchema from many goroutines to surface data
// races in the cache (run with -race).
func TestRegistryConcurrentGet(t *testing.T) {
	ctx := context.Background()
	reg, a, _ := newReg(t)

	s1 := avro.MustParse(recV1).(*avro.RecordSchema)
	s2 := avro.MustParse(recV2).(*avro.RecordSchema)
	id1, err := a.Persist(ctx, s1)
	if err != nil {
		t.Fatalf("persist v1: %v", err)
	}
	if _, err := a.Persist(ctx, s2); err != nil {
		t.Fatalf("persist v2: %v", err)
	}
	if err := reg.Setup(ctx, s2, func(c *registry.RegistryConfig) { c.PersistCurrent = true }); err != nil {
		t.Fatalf("setup: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, _, err := reg.GetSchema(ctx, id1); err != nil {
				t.Errorf("concurrent get: %v", err)
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
			reg := registry.New(stubFetcher{schema: body}, nil, fs.NewWireFormatter())
			_, _, _, err := reg.GetSchema(context.Background(), "aaaaaaaaaaaaaaaa")
			if !errors.Is(err, registry.ErrInvalidSchema) {
				t.Fatalf("expect ErrInvalidSchema, got %v", err)
			}
		})
	}
}
