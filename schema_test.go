package structavro

import (
	"errors"
	"reflect"
	"testing"

	avro "github.com/ln80/avro/v2"
)

// TestNestedRecordFieldZeroDefaults ensures nested records under *T and []T get
// AVRO zero-value defaults, so additive nested fields stay backward-compatible.
func TestNestedRecordFieldZeroDefaults(t *testing.T) {
	a := NewAPI()

	build := func(evType reflect.Type) *avro.RecordSchema {
		t.Helper()
		sch, err := BuildEnvelopeSchema(a, EnvelopeConfig{
			Namespace: "nest",
			RootType:  reflect.TypeOf(envelope{}),
			RootName:  "events",
			InjectKey: "union",
			Entries:   []RecordEntry{{Name: "nest.Ev", Type: evType, Sample: reflect.Zero(evType).Interface()}},
			Validate:  true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return sch
	}

	// Define v1/v2 in separate scopes so both use the same AVRO record names
	// (Nested / Item), matching real domain evolution that keeps type names.
	sch1 := func() *avro.RecordSchema {
		type Nested struct {
			Name string
		}
		type Item struct {
			Key string
		}
		type Ev struct {
			Payload *Nested
			Items   []Item
		}
		return build(reflect.TypeOf(Ev{}))
	}()

	events, err := UnpackUnionRecords(sch1, "Data")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expect 1 event schema, got %d", len(events))
	}
	assertFieldHasDefault(t, findNamedRecordField(t, events[0], "Payload"), "Name")
	assertFieldHasDefault(t, findArrayItemRecord(t, events[0], "Items"), "Key")

	sch2 := func() *avro.RecordSchema {
		type Nested struct {
			Name  string
			Extra string
		}
		type Item struct {
			Key  string
			Tags []string
		}
		type Ev struct {
			Payload *Nested
			Items   []Item
		}
		return build(reflect.TypeOf(Ev{}))
	}()

	if err := NewCompatibilityAPI().Compatible(sch2, sch1); err != nil {
		t.Fatalf("evolved schema must be backward-compatible with v1: %v", err)
	}

	events2, err := UnpackUnionRecords(sch2, "Data")
	if err != nil {
		t.Fatal(err)
	}
	assertFieldHasDefault(t, findNamedRecordField(t, events2[0], "Payload"), "Extra")
	assertFieldHasDefault(t, findArrayItemRecord(t, events2[0], "Items"), "Tags")
}

// TestBuildEnvelopeSchemaNilSample ensures an entry without a Sample (documented as
// optional) does not panic while deriving defaults.
func TestBuildEnvelopeSchemaNilSample(t *testing.T) {
	type Ev struct{ ID string }
	if _, err := BuildEnvelopeSchema(NewAPI(), EnvelopeConfig{
		Namespace: "nilsample",
		RootType:  reflect.TypeOf(envelope{}),
		RootName:  "events",
		InjectKey: "union",
		Entries:   []RecordEntry{{Name: "nilsample.Ev", Type: reflect.TypeOf(Ev{})}},
	}); err != nil {
		t.Fatal(err)
	}
}

func findNamedRecordField(t *testing.T, parent *avro.RecordSchema, fieldName string) *avro.RecordSchema {
	t.Helper()
	for _, f := range parent.Fields() {
		if f.Name() != fieldName {
			continue
		}
		typ := f.Type()
		if typ.Type() == avro.Union {
			for _, branch := range typ.(*avro.UnionSchema).Types() {
				if branch.Type() == avro.Record {
					return branch.(*avro.RecordSchema)
				}
				if branch.Type() == avro.Ref {
					return branch.(*avro.RefSchema).Schema().(*avro.RecordSchema)
				}
			}
		}
		if typ.Type() == avro.Record {
			return typ.(*avro.RecordSchema)
		}
		if typ.Type() == avro.Ref {
			return typ.(*avro.RefSchema).Schema().(*avro.RecordSchema)
		}
		t.Fatalf("field %s: expected record (possibly in union), got %s", fieldName, typ.Type())
	}
	t.Fatalf("field %s not found on %s", fieldName, parent.FullName())
	return nil
}

func findArrayItemRecord(t *testing.T, parent *avro.RecordSchema, fieldName string) *avro.RecordSchema {
	t.Helper()
	for _, f := range parent.Fields() {
		if f.Name() != fieldName {
			continue
		}
		arr, ok := f.Type().(*avro.ArraySchema)
		if !ok {
			t.Fatalf("field %s: expected array, got %s", fieldName, f.Type().Type())
		}
		items := arr.Items()
		if items.Type() == avro.Ref {
			return items.(*avro.RefSchema).Schema().(*avro.RecordSchema)
		}
		if items.Type() == avro.Record {
			return items.(*avro.RecordSchema)
		}
		t.Fatalf("field %s: expected array of records, got items %s", fieldName, items.Type())
	}
	t.Fatalf("field %s not found on %s", fieldName, parent.FullName())
	return nil
}

func assertFieldHasDefault(t *testing.T, rec *avro.RecordSchema, fieldName string) {
	t.Helper()
	for _, f := range rec.Fields() {
		if f.Name() == fieldName {
			if !f.HasDefault() {
				t.Fatalf("%s.%s: expected AVRO default, got none", rec.FullName(), fieldName)
			}
			return
		}
	}
	t.Fatalf("field %s not found on %s", fieldName, rec.FullName())
}

func TestSchemaOfSensitiveTags(t *testing.T) {
	t.Run("valid data tag with replace", func(t *testing.T) {
		type Ev struct {
			Email string `pii:"data,replace=redacted"`
		}
		if _, err := SchemaOf(reflect.TypeOf(Ev{}), WithNamespace("ns")); err != nil {
			t.Fatalf("expect valid tag, got %v", err)
		}
	})

	invalid := map[string]reflect.Type{
		"unknown name": reflect.TypeOf(struct {
			Email string `pii:"datum"`
		}{}),
		"unknown option": reflect.TypeOf(struct {
			Email string `pii:"data,knd=email"`
		}{}),
		"repeated option": reflect.TypeOf(struct {
			Email string `pii:"data,kind=a,kind=b"`
		}{}),
		"two tag ids": reflect.TypeOf(struct {
			Email string `pii:"data" sensitive:"data"`
		}{}),
	}
	for name, typ := range invalid {
		t.Run(name, func(t *testing.T) {
			_, err := SchemaOf(typ, WithName("Ev"), WithNamespace("ns"))
			if !errors.Is(err, ErrInvalidTag) {
				t.Fatalf("expect ErrInvalidTag, got %v", err)
			}
		})
	}
}
