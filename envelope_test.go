package structavro

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	avro "github.com/ln80/avro/v2"
	bankv1 "github.com/ln80/struct-avro/testdata/v1"
	bankv2 "github.com/ln80/struct-avro/testdata/v2"
)

// envelope mirrors event-store's avroEvent exported fields, tags and order, so that
// the generated schema is byte-identical to what event-store produces.
type envelope struct {
	FStreamID         string        `avro:"StmID"`
	FRawGlobalVersion string        `avro:"GVer"`
	FRawVersion       string        `avro:"Ver"`
	FID               string        `avro:"ID"`
	FType             string        `avro:"Type"`
	FAt               int64         `avro:"At"`
	FUser             string        `avro:"User"`
	FIPAddr           string        `avro:"IPAddr"`
	FDests            []string      `avro:"Dests"`
	FTTL              time.Duration `avro:"TTL"`
	FRawEvent         any           `avro:"Data" ev:",inject=union"`
}

func newAPI() avro.API {
	return avro.Config{PartialUnionTypeResolution: true, UnionResolutionError: false}.Freeze()
}

func bankV1Entries() []RecordEntry {
	return []RecordEntry{
		{Name: "bank.AccountOpened", Type: reflect.TypeOf(bankv1.AccountOpened{}), Sample: bankv1.AccountOpened{}},
		{Name: "bank.MoneyDeposited", Type: reflect.TypeOf(bankv1.MoneyDeposited{}), Sample: bankv1.MoneyDeposited{}},
		{Name: "bank.MoneyWithdrawn", Type: reflect.TypeOf(bankv1.MoneyWithdrawn{}), Sample: bankv1.MoneyWithdrawn{}},
	}
}

func bankV2Entries() []RecordEntry {
	return []RecordEntry{
		{Name: "bank.AccountOpened", Type: reflect.TypeOf(bankv2.AccountOpened{}), Sample: bankv2.AccountOpened{Client: bankv2.User{BirthDay: bankv2.DEFAULT_BIRTH_DAY}}},
		{Name: "bank.MoneyDeposited", Type: reflect.TypeOf(bankv2.MoneyDeposited{}), Sample: bankv2.MoneyDeposited{}},
		{Name: "bank.MoneyWithdrawn", Type: reflect.TypeOf(bankv2.MoneyWithdrawn{}), Sample: bankv2.MoneyWithdrawn{}},
	}
}

func buildBank(t *testing.T, a avro.API, entries []RecordEntry) *avro.RecordSchema {
	t.Helper()
	sch, err := BuildEnvelopeSchema(a, EnvelopeConfig{
		Namespace: "bank",
		RootType:  reflect.TypeOf(envelope{}),
		RootName:  "events",
		InjectKey: "union",
		Entries:   entries,
		Validate:  true,
	})
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	return sch
}

// registerUnion registers each entry's value + pointer under its full name so the
// union resolver can map records to Go types on unmarshal (mirrors event-store's
// registerEventTypes).
func registerUnion(a avro.API, samples map[string]any) {
	for name, v := range samples {
		a.Register(name, v)
		rv := reflect.New(reflect.TypeOf(v))
		rv.Elem().Set(reflect.ValueOf(v))
		a.Register(name, rv.Interface())
	}
}

func TestBuildEnvelopeSchema_Stable(t *testing.T) {
	a := newAPI()
	s1 := buildBank(t, a, bankV1Entries())
	s2 := buildBank(t, a, bankV1Entries())

	// The avro CRC64 fingerprint (Parsing Canonical Form) is the schema-ID
	// determinant the registry actually uses. It must be stable: it depends on the
	// generated field names — including the defxyz/sensitivexyz fingerprint fields —
	// but NOT on inline "default" values, which the canonical form drops.
	if fp1, fp2 := s1.Fingerprint(), s2.Fingerprint(); fp1 != fp2 {
		t.Fatalf("schema fingerprint is not deterministic:\n%x\n---\n%x", fp1, fp2)
	}

	b1, err := s1.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	b2, err := s2.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	// MarshalJSON is only canonically stable: the embedded record-field "default"
	// maps serialise in non-deterministic Go map order (true of event-store too), so
	// compare the canonicalised forms rather than raw bytes.
	if c1, c2 := canonicalJSON(t, b1), canonicalJSON(t, b2); !bytes.Equal(c1, c2) {
		t.Fatalf("schema generation is not deterministic:\n%s\n---\n%s", c1, c2)
	}

	// the generated JSON must be a valid, parseable AVRO schema
	if _, err := avro.ParseBytes(b1); err != nil {
		t.Fatalf("generated invalid AVRO schema: %v\n%s", err, b1)
	}

	records, err := UnpackUnionRecords(s1, "Data")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("expect 3 union records, got %d", len(records))
	}
	if want, got := "bank.AccountOpened", records[0].FullName(); want != got {
		t.Fatalf("want %q, got %q", want, got)
	}
}

// canonicalJSON parses an AVRO schema JSON and re-marshals it so that all object
// keys are sorted. This neutralises the non-deterministic Go map iteration order
// that event-store's avro engine emits for inline record-field "default" values
// (e.g. {"ID":"","Email":""} vs {"Email":"","ID":""}). It does NOT alter the
// fingerprint field names (defxyz.../sensitivexyz...) which are the schema-ID
// determinants, so canonical equality still proves byte-parity of the schema ID.
func canonicalJSON(t *testing.T, b []byte) []byte {
	t.Helper()
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("canonicalize: %v\n%s", err, b)
	}
	// json.Marshal sorts map keys, giving a deterministic canonical form.
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("canonicalize marshal: %v", err)
	}
	return out
}

// TestEnvelopeSchemaParity is the cross-module parity gate: the schema generated by
// struct-avro's BuildEnvelopeSchema must be structurally identical (and produce the
// same fingerprint field names) as the schema produced by the current event-store
// avro engine. Golden files are captured by event-store/tool/paritygen from the live
// engine. Comparison is canonicalised because the embedded default maps serialise in
// non-deterministic order on BOTH sides.
func TestEnvelopeSchemaParity(t *testing.T) {
	cases := []struct {
		name    string
		golden  string
		entries []RecordEntry
	}{
		{"v1", "bank_v1.json", bankV1Entries()},
		{"v2", "bank_v2.json", bankV2Entries()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newAPI()
			sch := buildBank(t, a, tc.entries)
			got, err := sch.MarshalJSON()
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			want, err := os.ReadFile(filepath.Join("testdata", "golden", tc.golden))
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			cgot, cwant := canonicalJSON(t, got), canonicalJSON(t, want)
			if !bytes.Equal(cgot, cwant) {
				t.Fatalf("schema parity mismatch with event-store golden:\n got: %s\nwant: %s", cgot, cwant)
			}
		})
	}
}

// TestEnvelopeSchemaEvolution reproduces event-store's TestInteg: encode an event
// with the v1 schema, then resolve+decode it with the v2 schema and verify the
// field rename (Owner -> Client via alias) and the new defaulted field (BirthDay).
func TestEnvelopeSchemaEvolution(t *testing.T) {
	aEnc := newAPI()
	registerUnion(aEnc, map[string]any{
		"bank.AccountOpened":  bankv1.AccountOpened{},
		"bank.MoneyDeposited": bankv1.MoneyDeposited{},
		"bank.MoneyWithdrawn": bankv1.MoneyWithdrawn{},
	})
	schV1 := buildBank(t, aEnc, bankV1Entries())

	opened := bankv1.AccountOpened{
		ID:    "acc-1",
		Owner: bankv1.User{ID: "abc", Email: "tom.bill@example.com"},
		At:    42,
	}
	env := envelope{
		FStreamID: "bank/tenant",
		FID:       "evt-1",
		FType:     "bank.AccountOpened",
		FAt:       time.Now().UnixNano(),
		FRawEvent: opened,
	}
	encoded, err := aEnc.Marshal(schV1, env)
	if err != nil {
		t.Fatalf("marshal v1: %v", err)
	}

	aDec := newAPI()
	registerUnion(aDec, map[string]any{
		"bank.AccountOpened":  bankv2.AccountOpened{},
		"bank.MoneyDeposited": bankv2.MoneyDeposited{},
		"bank.MoneyWithdrawn": bankv2.MoneyWithdrawn{},
	})
	schV2 := buildBank(t, aDec, bankV2Entries())

	compat := avro.NewSchemaCompatibility()
	resolved, err := compat.Resolve(schV2, schV1)
	if err != nil {
		t.Fatalf("resolve v2<-v1: %v", err)
	}

	var out envelope
	if err := aDec.Unmarshal(resolved, encoded, &out); err != nil {
		t.Fatalf("unmarshal v2: %v", err)
	}

	got, ok := out.FRawEvent.(*bankv2.AccountOpened)
	if !ok {
		t.Fatalf("expect *bank.AccountOpened (v2), got %T", out.FRawEvent)
	}
	if want := opened.Owner.Email; got.Client.Email != want {
		t.Fatalf("email: want %q, got %q", want, got.Client.Email)
	}
	if got.Client.BirthDay != bankv2.DEFAULT_BIRTH_DAY {
		t.Fatalf("birthday default: want %q, got %q", bankv2.DEFAULT_BIRTH_DAY, got.Client.BirthDay)
	}
	if got.ID != opened.ID {
		t.Fatalf("id: want %q, got %q", opened.ID, got.ID)
	}
}
