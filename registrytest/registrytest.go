// Package registrytest checks that a schema store behaves as the registry
// package expects. Run it from the store's own tests against real storage:
//
//	func TestStore(t *testing.T) {
//		registrytest.Run(t, func(t *testing.T) registrytest.Store {
//			return newStoreForTest(t) // empty, isolated per call
//		})
//	}
//
// The suite does not assume an ID format or how versions are numbered beyond
// what the registry and the tool package rely on.
package registrytest

import (
	"context"
	"errors"
	"sync"
	"testing"

	avro "github.com/ln80/avro/v2"
	"github.com/ln80/struct-avro/registry"
)

// Store is a complete schema store: it fetches, persists, walks and frames IDs.
type Store interface {
	registry.Fetcher
	registry.Persister
	registry.Walker
	registry.WireFormatter
}

// NewStore returns an empty store, isolated from the stores of other calls.
type NewStore func(t *testing.T) Store

const (
	nsA = "conformance_a"
	nsB = "conformance_b"
)

func schemaV1(ns string) *avro.RecordSchema {
	return avro.MustParse(`{"type":"record","name":"events","namespace":"` + ns + `","fields":[` +
		`{"name":"ID","type":"string"}]}`).(*avro.RecordSchema)
}

func schemaV2(ns string) *avro.RecordSchema {
	return avro.MustParse(`{"type":"record","name":"events","namespace":"` + ns + `","fields":[` +
		`{"name":"ID","type":"string"},{"name":"At","type":"long","default":0}]}`).(*avro.RecordSchema)
}

type record1 struct {
	ID string `avro:"ID"`
}

type record2 struct {
	ID string `avro:"ID"`
	At int64  `avro:"At"`
}

// Run runs every check against stores from newStore.
func Run(t *testing.T, newStore NewStore) {
	t.Helper()
	t.Run("GetUnknown", func(t *testing.T) { testGetUnknown(t, newStore(t)) })
	t.Run("PersistAndFetch", func(t *testing.T) { testPersistAndFetch(t, newStore(t)) })
	t.Run("PersistIsIdempotent", func(t *testing.T) { testPersistIdempotent(t, newStore(t)) })
	t.Run("Walk", func(t *testing.T) { testWalk(t, newStore(t)) })
	t.Run("WireFormat", func(t *testing.T) { testWireFormat(t, newStore(t)) })
	t.Run("Registry", func(t *testing.T) { testRegistry(t, newStore(t)) })
	t.Run("Concurrent", func(t *testing.T) { testConcurrent(t, newStore(t)) })
}

func persist(t *testing.T, s Store, schema *avro.RecordSchema) string {
	t.Helper()
	id, err := s.Persist(context.Background(), schema)
	if err != nil {
		t.Fatalf("persist %s: %v", schema.FullName(), err)
	}
	if id == "" {
		t.Fatalf("persist %s returned an empty ID", schema.FullName())
	}
	return id
}

func testGetUnknown(t *testing.T, s Store) {
	ctx := context.Background()
	if _, err := s.GetByDefinition(ctx, schemaV1(nsA)); !errors.Is(err, registry.ErrSchemaNotFound) {
		t.Fatalf("GetByDefinition on an empty store: want ErrSchemaNotFound, got %v", err)
	}
	id := persist(t, s, schemaV1(nsA))
	// An ID that differs from a stored one in its last character.
	other := []byte(id)
	if other[len(other)-1] == '0' {
		other[len(other)-1] = '1'
	} else {
		other[len(other)-1] = '0'
	}
	if _, err := s.Get(ctx, string(other)); err == nil {
		t.Fatal("Get of an unknown ID must fail")
	}
}

func testPersistAndFetch(t *testing.T, s Store) {
	ctx := context.Background()
	v1 := schemaV1(nsA)
	id := persist(t, s, v1)

	got, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	parsed, err := avro.Parse(got)
	if err != nil {
		t.Fatalf("Get returned a schema that does not parse: %v", err)
	}
	if parsed.Fingerprint() != v1.Fingerprint() {
		t.Fatal("Get returned another schema")
	}

	byDef, err := s.GetByDefinition(ctx, v1)
	if err != nil {
		t.Fatalf("GetByDefinition: %v", err)
	}
	if byDef != id {
		t.Fatalf("GetByDefinition: want %s, got %s", id, byDef)
	}

	// Another version gets another ID.
	id2 := persist(t, s, schemaV2(nsA))
	if id2 == id {
		t.Fatal("two schemas got the same ID")
	}
}

func testPersistIdempotent(t *testing.T, s Store) {
	ctx := context.Background()
	id := persist(t, s, schemaV1(nsA))
	again := persist(t, s, schemaV1(nsA))
	if again != id {
		t.Fatalf("persisting a stored schema again: want ID %s, got %s", id, again)
	}
	n, err := s.Walk(ctx, func(string, int64, bool, *avro.RecordSchema) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("persisting a stored schema again must not add a version: %d versions", n)
	}
}

type visit struct {
	id      string
	version int64
	latest  bool
	ns      string
}

func walkAll(t *testing.T, s Store, opts ...func(*registry.WalkConfig)) []visit {
	t.Helper()
	var got []visit
	n, err := s.Walk(context.Background(), func(id string, version int64, latest bool, schema *avro.RecordSchema) error {
		if schema == nil {
			t.Fatal("Walk passed a nil schema")
		}
		got = append(got, visit{id, version, latest, schema.Namespace()})
		return nil
	}, opts...)
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if n != len(got) {
		t.Fatalf("Walk returned %d, visited %d", n, len(got))
	}
	return got
}

func testWalk(t *testing.T, s Store) {
	idA1 := persist(t, s, schemaV1(nsA))
	idA2 := persist(t, s, schemaV2(nsA))
	idB1 := persist(t, s, schemaV1(nsB))

	got := walkAll(t, s)
	if len(got) != 3 {
		t.Fatalf("Walk: want 3 versions, got %d", len(got))
	}
	byID := map[string]visit{}
	for _, v := range got {
		byID[v.id] = v
	}
	a1, a2, b1 := byID[idA1], byID[idA2], byID[idB1]
	if a1.ns != nsA || a2.ns != nsA || b1.ns != nsB {
		t.Fatalf("Walk: wrong namespaces: %+v", got)
	}
	if a2.version <= a1.version {
		t.Fatalf("Walk: a later version must have a higher number: %d then %d", a1.version, a2.version)
	}
	if a1.latest || !a2.latest || !b1.latest {
		t.Fatalf("Walk: latest flags: %+v", got)
	}
	// Versions of a namespace are visited in ascending order.
	var last int64
	for _, v := range got {
		if v.ns != nsA {
			continue
		}
		if v.version <= last {
			t.Fatalf("Walk: versions of %s not ascending: %+v", nsA, got)
		}
		last = v.version
	}

	onlyB := walkAll(t, s, func(c *registry.WalkConfig) { c.Namespaces = []string{nsB} })
	if len(onlyB) != 1 || onlyB[0].id != idB1 {
		t.Fatalf("Walk with a namespace filter: %+v", onlyB)
	}

	stop := errors.New("stop")
	if _, err := s.Walk(context.Background(), func(string, int64, bool, *avro.RecordSchema) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("Walk must return the callback's error, got %v", err)
	}
}

func testWireFormat(t *testing.T, s Store) {
	id := persist(t, s, schemaV1(nsA))
	payload := []byte("payload")
	framed, err := s.AppendSchemaID(payload, id)
	if err != nil {
		t.Fatalf("AppendSchemaID: %v", err)
	}
	gotID, gotPayload, err := s.ExtractSchemaID(framed)
	if err != nil {
		t.Fatalf("ExtractSchemaID: %v", err)
	}
	if gotID != id || string(gotPayload) != string(payload) {
		t.Fatalf("round trip: got (%s, %q)", gotID, gotPayload)
	}
	for _, bad := range [][]byte{nil, {}, []byte("x")} {
		if _, _, err := s.ExtractSchemaID(bad); err == nil {
			t.Fatalf("ExtractSchemaID(%q) must fail", bad)
		}
	}
	if _, err := s.AppendSchemaID(payload, ""); err == nil {
		t.Fatal("AppendSchemaID with an empty ID must fail")
	}
}

func testRegistry(t *testing.T, s Store) {
	ctx := context.Background()
	persist(t, s, schemaV1(nsA))
	persist(t, s, schemaV2(nsA))

	old, err := registry.Open(ctx, s, registry.WithCurrent(schemaV1(nsA)))
	if err != nil {
		t.Fatalf("Open with v1: %v", err)
	}
	b, err := old.Encode(record1{ID: "a"})
	if err != nil {
		t.Fatal(err)
	}

	cur, err := registry.Open(ctx, s, registry.WithCurrent(schemaV2(nsA)))
	if err != nil {
		t.Fatalf("Open with v2: %v", err)
	}
	var got record2
	if _, err := cur.Decode(ctx, b, &got); err != nil {
		t.Fatalf("decode v1 data with v2: %v", err)
	}
	if got.ID != "a" {
		t.Fatalf("decode: %+v", got)
	}
}

func testConcurrent(t *testing.T, s Store) {
	ctx := context.Background()
	id := persist(t, s, schemaV1(nsA))
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Get(ctx, id); err != nil {
				t.Errorf("concurrent Get: %v", err)
			}
			if _, err := s.GetByDefinition(ctx, schemaV1(nsA)); err != nil {
				t.Errorf("concurrent GetByDefinition: %v", err)
			}
		}()
	}
	wg.Wait()
}
