package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	avro "github.com/ln80/avro/v2"
	"github.com/ln80/struct-avro/registry"
)

func TestWireFormatterRoundTrip(t *testing.T) {
	wf := NewWireFormatter()
	id := strings.Repeat("a", schemaIDLen) // 16 chars
	payload := []byte("hello-avro-payload")

	framed, err := wf.AppendSchemaID(payload, id)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if len(framed) != schemaIDLen+len(payload) {
		t.Fatalf("framed length: want %d, got %d", schemaIDLen+len(payload), len(framed))
	}

	gotID, gotData, err := wf.ExtractSchemaID(framed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if gotID != id {
		t.Fatalf("id: want %q, got %q", id, gotID)
	}
	if string(gotData) != string(payload) {
		t.Fatalf("payload: want %q, got %q", payload, gotData)
	}
}

func TestWireFormatterRejectsBadLength(t *testing.T) {
	wf := NewWireFormatter()

	// Short ID would previously corrupt payloads silently (Extract always read 16).
	if _, err := wf.AppendSchemaID([]byte("x"), "short"); !errors.Is(err, registry.ErrInvalidSchemaID) {
		t.Fatalf("append short id: want ErrInvalidSchemaID, got %v", err)
	}
	if _, err := wf.AppendSchemaID([]byte("x"), strings.Repeat("a", schemaIDLen+1)); !errors.Is(err, registry.ErrInvalidSchemaID) {
		t.Fatalf("append long id: want ErrInvalidSchemaID, got %v", err)
	}
	if _, _, err := wf.ExtractSchemaID([]byte("tooshort")); !errors.Is(err, registry.ErrInvalidDataWireFormat) {
		t.Fatalf("extract short data: want ErrInvalidDataWireFormat, got %v", err)
	}
}

func TestParseSchemaPath(t *testing.T) {
	tcs := []struct {
		path    string
		wantV   int64
		wantNS  string
		wantID  string
		wantErr bool
	}{
		{"bank/1@abcdef0123456789.json", 1, "bank", "abcdef0123456789", false},
		{"root/bank/12@deadbeefdeadbeef.json", 12, "bank", "deadbeefdeadbeef", false},
		{"bank/notanumber@id.json", 0, "", "", true},  // bad version
		{"bank/1-no-separator.json", 0, "", "", true}, // missing '@'
		{"noslash.json", 0, "", "", true},             // no namespace segment
	}
	for _, tc := range tcs {
		v, ns, id, err := parseSchemaPath(tc.path)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("%s: expected error, got nil", tc.path)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: unexpected error %v", tc.path, err)
		}
		if v != tc.wantV || ns != tc.wantNS || id != tc.wantID {
			t.Fatalf("%s: got (%d,%q,%q), want (%d,%q,%q)", tc.path, v, ns, id, tc.wantV, tc.wantNS, tc.wantID)
		}
	}
}

const recV1 = `{"type":"record","name":"events","namespace":"bank","fields":[{"name":"ID","type":"string"}]}`
const recV2 = `{"type":"record","name":"events","namespace":"bank","fields":[{"name":"ID","type":"string"},{"name":"At","type":"long","default":0}]}`

func TestAdapterPersistGetWalk(t *testing.T) {
	dir := t.TempDir()
	a := NewDirAdapter(dir)
	ctx := context.Background()

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
	if id1 == id2 {
		t.Fatalf("expected distinct ids, both %q", id1)
	}

	// Get by id returns the persisted definition.
	def, err := a.Get(ctx, id1)
	if err != nil {
		t.Fatalf("get id1: %v", err)
	}
	if !strings.Contains(def, "\"name\": \"ID\"") && !strings.Contains(def, "\"name\":\"ID\"") {
		t.Fatalf("get id1: unexpected definition %s", def)
	}

	// GetByDefinition resolves the same id via fingerprint.
	gotID, err := a.GetByDefinition(ctx, s2)
	if err != nil {
		t.Fatalf("get by def v2: %v", err)
	}
	if gotID != id2 {
		t.Fatalf("get by def v2: want %q, got %q", id2, gotID)
	}

	// Walk visits both versions in ascending order, marking the last as latest.
	type visit struct {
		id      string
		version int64
		latest  bool
	}
	var visits []visit
	n, err := a.Walk(ctx, func(id string, version int64, latest bool, _ *avro.RecordSchema) error {
		visits = append(visits, visit{id, version, latest})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if n != 2 || len(visits) != 2 {
		t.Fatalf("walk count: want 2, got %d (%v)", n, visits)
	}
	if visits[0].version != 1 || visits[0].latest {
		t.Fatalf("walk[0]: want version 1 not-latest, got %+v", visits[0])
	}
	if visits[1].version != 2 || !visits[1].latest {
		t.Fatalf("walk[1]: want version 2 latest, got %+v", visits[1])
	}
}

// minimalRecord is a valid empty Avro record schema used as Walk input.
const minimalRecord = `{"type":"record","name":"events","namespace":"ns","fields":[]}`

func writeSchema(t *testing.T, dir, namespace, filename, body string) {
	t.Helper()
	nsDir := filepath.Join(dir, namespace)
	if err := os.MkdirAll(nsDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nsDir, filename), []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
}

func TestWalk_LatestIsNumericMax_CrossingTen(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "ns", "9@aaaaaaaaaaaaaaaa.json", minimalRecord)
	writeSchema(t, dir, "ns", "10@bbbbbbbbbbbbbbbb.json", minimalRecord)
	writeSchema(t, dir, "ns", "12@cccccccccccccccc.json", minimalRecord)

	adapter := NewDirAdapter(dir)
	seen := map[int64]bool{}
	var order []int64
	n, err := adapter.Walk(context.Background(), func(id string, version int64, latest bool, schema *avro.RecordSchema) error {
		seen[version] = latest
		order = append(order, version)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("expected 3 schemas, got %d", n)
	}
	if !seen[12] {
		t.Fatal("expected version 12 to be marked latest")
	}
	if seen[9] {
		t.Fatal("version 9 must not be latest when 12 exists (lexicographic sort trap)")
	}
	if seen[10] {
		t.Fatal("version 10 must not be latest when 12 exists")
	}
	if want := []int64{9, 10, 12}; !slicesEqual(order, want) {
		t.Fatalf("walk order: want %v, got %v", want, order)
	}
}

func TestWalk_LatestAmongSingleDigitVersions(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"1@aaaaaaaaaaaaaaaa.json", "5@bbbbbbbbbbbbbbbb.json", "9@cccccccccccccccc.json"} {
		writeSchema(t, dir, "ns", name, minimalRecord)
	}

	adapter := NewDirAdapter(dir)
	var latestVersion int64
	_, err := adapter.Walk(context.Background(), func(id string, version int64, latest bool, schema *avro.RecordSchema) error {
		if latest {
			latestVersion = version
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if latestVersion != 9 {
		t.Fatalf("expected latest version 9, got %d", latestVersion)
	}
}

func TestPersist_NextVersionIsNumericMaxPlusOne(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "bank", "9@aaaaaaaaaaaaaaaa.json", recV1)
	writeSchema(t, dir, "bank", "10@bbbbbbbbbbbbbbbb.json", recV1)

	a := NewDirAdapter(dir)
	id, err := a.Persist(context.Background(), avro.MustParse(recV2).(*avro.RecordSchema))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "bank", "11@"+id+".json")); err != nil {
		t.Fatalf("expected version 11 to be persisted: %v", err)
	}
}

func TestMaxVersionAmong(t *testing.T) {
	max, err := maxVersionAmong([]string{
		"ns/9@aaaaaaaaaaaaaaaa.json",
		"ns/10@bbbbbbbbbbbbbbbb.json",
		"ns/12@cccccccccccccccc.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if max != 12 {
		t.Fatalf("expected max 12, got %d", max)
	}
}

func slicesEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestGet_RejectsInvalidID(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "ns", "1@aaaaaaaaaaaaaaaa.json", minimalRecord)
	adapter := NewDirAdapter(dir)

	// '*' and '?' would match another schema's file if used in the glob pattern.
	for _, id := range []string{"****************", "????????????????", "[a]aaaaaaaaaaaaa", "AAAAAAAAAAAAAAAA", "short", ""} {
		if _, err := adapter.Get(context.Background(), id); !errors.Is(err, registry.ErrInvalidSchemaID) {
			t.Fatalf("id %q: expect ErrInvalidSchemaID, got %v", id, err)
		}
	}
	if _, err := adapter.Get(context.Background(), "aaaaaaaaaaaaaaaa"); err != nil {
		t.Fatal(err)
	}
}

func TestExtractSchemaID_RejectsNonHexPrefix(t *testing.T) {
	wf := NewWireFormatter()
	if _, _, err := wf.ExtractSchemaID([]byte("****************payload")); !errors.Is(err, registry.ErrInvalidDataWireFormat) {
		t.Fatalf("expect ErrInvalidDataWireFormat, got %v", err)
	}
}

func TestWalk_RejectsInvalidSchemaFiles(t *testing.T) {
	cases := map[string]struct{ name, body string }{
		"not a record": {"1@aaaaaaaaaaaaaaaa.json", `"string"`},
		"not json":     {"1@aaaaaaaaaaaaaaaa.json", `{`},
		"bad id":       {"1@zzzzzzzzzzzzzzzz.json", minimalRecord},
		"zero version": {"0@aaaaaaaaaaaaaaaa.json", minimalRecord},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeSchema(t, dir, "ns", tc.name, tc.body)
			_, err := NewDirAdapter(dir).Walk(context.Background(), func(string, int64, bool, *avro.RecordSchema) error { return nil })
			if err == nil {
				t.Fatal("expect error")
			}
		})
	}
}

func TestPersist_SameVersionTwiceIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	a := NewDirAdapter(dir)
	s := avro.MustParse(recV1).(*avro.RecordSchema)
	fixed := func(pc *registry.PersistConfig) {
		pc.Resolver = func(*avro.RecordSchema) (string, int, error) { return "aaaaaaaaaaaaaaaa", 3, nil }
	}
	for i := 0; i < 2; i++ {
		if _, err := a.Persist(context.Background(), s, fixed); err != nil {
			t.Fatalf("persist %d: %v", i, err)
		}
	}

	// Same version and ID, different schema: refused, file kept.
	if _, err := a.Persist(context.Background(), avro.MustParse(recV2).(*avro.RecordSchema), fixed); !errors.Is(err, registry.ErrCreateOrUpdateSchemaFailed) {
		t.Fatalf("expect ErrCreateOrUpdateSchemaFailed, got %v", err)
	}
	// Same version, other ID: refused.
	other := func(pc *registry.PersistConfig) {
		pc.Resolver = func(*avro.RecordSchema) (string, int, error) { return "bbbbbbbbbbbbbbbb", 3, nil }
	}
	if _, err := a.Persist(context.Background(), s, other); !errors.Is(err, registry.ErrCreateOrUpdateSchemaFailed) {
		t.Fatalf("expect ErrCreateOrUpdateSchemaFailed, got %v", err)
	}
	got, err := a.Get(context.Background(), "aaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `"ID"`) || strings.Contains(got, `"At"`) {
		t.Fatalf("stored schema was overwritten: %s", got)
	}
}
