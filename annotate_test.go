package structavro

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	avro "github.com/ln80/avro/v2"
)

func bankConfig(entries []RecordEntry) EnvelopeConfig {
	return EnvelopeConfig{
		Namespace: "bank",
		RootType:  reflect.TypeOf(envelope{}),
		RootName:  "events",
		InjectKey: "union",
		Entries:   entries,
		Validate:  true,
	}
}

func fieldNames(s *avro.RecordSchema) []string {
	var names []string
	for _, f := range s.Fields() {
		names = append(names, f.Name())
	}
	return names
}

func hasPrefix(names []string, prefix string) bool {
	for _, n := range names {
		if strings.HasPrefix(n, prefix) {
			return true
		}
	}
	return false
}

func TestFingerprintFieldNames(t *testing.T) {
	legacy, err := BuildEnvelopeSchema(newAPI(), bankConfig(bankV1Entries()))
	if err != nil {
		t.Fatal(err)
	}
	if names := fieldNames(legacy); !hasPrefix(names, "defxyz") || !hasPrefix(names, "sensitivexyz") {
		t.Fatalf("zero value must keep the legacy names, got %v", names)
	}

	cfg := bankConfig(bankV1Entries())
	cfg.Fingerprint = FingerprintFields{Defaults: "_fp_defaults_", Annotations: "_fp_annotations_"}
	custom, err := BuildEnvelopeSchema(newAPI(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	names := fieldNames(custom)
	if !hasPrefix(names, "_fp_defaults_") || !hasPrefix(names, "_fp_annotations_") || hasPrefix(names, "defxyz") {
		t.Fatalf("custom prefixes not used: %v", names)
	}
	// The hashes do not depend on the prefix.
	suffix := func(names []string, prefix string) string {
		for _, n := range names {
			if strings.HasPrefix(n, prefix) {
				return strings.TrimPrefix(n, prefix)
			}
		}
		return ""
	}
	if suffix(names, "_fp_defaults_") != suffix(fieldNames(legacy), "defxyz") {
		t.Fatal("defaults hash changed with the prefix")
	}

	// Old data stays readable: the custom schema can read the legacy one.
	if err := NewCompatibilityAPI().Compatible(custom, legacy); err != nil {
		t.Fatalf("renamed fingerprint fields must stay backward compatible: %v", err)
	}

	for _, bad := range []FingerprintFields{
		{Defaults: "same", Annotations: "same"},
		{Defaults: "1abc"},
		{Annotations: "a-b"},
	} {
		cfg := bankConfig(bankV1Entries())
		cfg.Fingerprint = bad
		if _, err := BuildEnvelopeSchema(newAPI(), cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%+v: expect ErrInvalidConfig, got %v", bad, err)
		}
	}
}

func TestFieldAnnotator(t *testing.T) {
	type Ev struct {
		Email string `privacy:"secret"`
		Name  string
	}
	custom := func(f reflect.StructField) (*Annotation, error) {
		v, ok := f.Tag.Lookup("privacy")
		if !ok {
			return nil, nil
		}
		if v != "secret" {
			return nil, errors.New("unknown privacy class")
		}
		return &Annotation{Property: "privacy", Value: v, Token: f.Name + "." + v}, nil
	}
	entries := []RecordEntry{{Name: "bank.Ev", Type: reflect.TypeOf(Ev{}), Sample: Ev{}}}

	cfg := bankConfig(entries)
	cfg.Annotator = custom
	sch, err := BuildEnvelopeSchema(newAPI(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	recs, err := UnpackUnionRecords(sch, "Data")
	if err != nil {
		t.Fatal(err)
	}
	var email *avro.Field
	for _, f := range recs[0].Fields() {
		if f.Name() == "Email" {
			email = f
		}
	}
	if email == nil || email.Prop("privacy") != "secret" {
		t.Fatalf("custom property not written: %v", email)
	}
	if !hasPrefix(fieldNames(sch), "sensitivexyz") {
		t.Fatal("custom annotations must be fingerprinted")
	}

	cfg.Annotator = NoAnnotations
	plain, err := BuildEnvelopeSchema(newAPI(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if hasPrefix(fieldNames(plain), "sensitivexyz") {
		t.Fatal("NoAnnotations must not add an annotations fingerprint")
	}

	type Bad struct {
		Email string `privacy:"other"`
	}
	cfg = bankConfig([]RecordEntry{{Name: "bank.Bad", Type: reflect.TypeOf(Bad{}), Sample: Bad{}}})
	cfg.Annotator = custom
	if _, err := BuildEnvelopeSchema(newAPI(), cfg); !errors.Is(err, ErrInvalidTag) {
		t.Fatalf("expect ErrInvalidTag, got %v", err)
	}
}

func TestSensitiveTagsToken(t *testing.T) {
	type Ev struct {
		Email string `pii:"data,replace=x,kind=email"`
	}
	f, _ := reflect.TypeOf(Ev{}).FieldByName("Email")
	ann, err := SensitiveTags(f)
	if err != nil {
		t.Fatal(err)
	}
	// Part of the schema format: options are sorted, the field type name and
	// field name come first.
	if want := `string.Email.pii:"data,kind=email,replace=x"`; ann.Token != want {
		t.Fatalf("token: want %s, got %s", want, ann.Token)
	}
}
