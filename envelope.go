package structavro

import (
	"fmt"
	"reflect"

	avro "github.com/ln80/avro/v2"
)

// RecordEntry describes one record (e.g. an event type) to embed in an envelope union.
type RecordEntry struct {
	// Name is the record's full (dotted) schema name, e.g. "bank.AccountOpened".
	Name string
	// Type is the Go struct type of the record.
	Type reflect.Type
	// Sample is a value of Type used to derive AVRO defaults (typically the zero
	// value or a registry-provided default). May be nil.
	Sample any
	// Aliases are record-name aliases declared at the registry level (struct-level
	// aliases via the blank "_" field are resolved automatically).
	Aliases []string
}

// EnvelopeConfig configures BuildEnvelopeSchema.
type EnvelopeConfig struct {
	// Namespace of the generated schemas. If empty, RootNamespace defaults to "_".
	Namespace string
	// RootType is the Go struct type of the envelope (e.g. the event envelope).
	RootType reflect.Type
	// RootName forces the envelope record name (e.g. "events").
	RootName string
	// InjectKey is the `ev:",inject=<key>"` key on the envelope field that receives
	// the union of record schemas.
	InjectKey string
	// Entries are the records to include in the union.
	Entries []RecordEntry
	// TagKey overrides the schema-option tag key (default "ev").
	TagKey string
	// Validate, when true, round-trips each record's defaults through the API to
	// catch invalid generated defaults (mirrors event-store's eventSchema checks).
	Validate bool
	// Annotator returns the annotation of each struct field. nil means
	// SensitiveTags; NoAnnotations turns annotations off.
	Annotator FieldAnnotator
	// Fingerprint names the fields that make a change to defaults or
	// annotations change the schema ID. The zero value keeps the legacy names.
	Fingerprint FingerprintFields
}

// BuildEnvelopeSchema assembles an envelope record schema: it generates one record
// schema per entry (sharing a dedup cache and a fingerprint accumulator), wraps them
// in a [null, ...records] union, and injects that union into the envelope RootType.
//
// It also registers each entry on a (see RegisterEntries), so that decoding the
// union returns the entry's Go type.
func BuildEnvelopeSchema(a avro.API, cfg EnvelopeConfig) (*avro.RecordSchema, error) {
	if len(cfg.Entries) == 0 {
		return nil, fmt.Errorf("%w: no record entries provided", ErrInvalidConfig)
	}
	if a == nil {
		return nil, fmt.Errorf("%w: no API", ErrInvalidConfig)
	}
	if err := RegisterEntries(a, cfg.Entries); err != nil {
		return nil, err
	}
	tagKey := cfg.TagKey
	if tagKey == "" {
		tagKey = DefaultTagKey
	}

	names, err := cfg.Fingerprint.withDefaults()
	if err != nil {
		return nil, err
	}
	cache := newCache()
	acc := newAccumulator()
	acc.names = names

	schemas := make([]avro.Schema, 0, len(cfg.Entries)+1)
	for _, entry := range cfg.Entries {
		def, err := defaultOf(entry.Sample)
		if err != nil {
			return nil, err
		}

		// aliases can be defined at the registry level as well as at the struct level
		// (via the blank "_" field tag).
		aliases := append([]string{}, entry.Aliases...)
		aliases = structTypeAliases(tagKey, entry.Type, aliases)
		aliases = unique(aliases)

		schema, err := schemaOf(entry.Type, func(sc *schemaConfig) {
			sc.name = entry.Name
			sc.namespace = cfg.Namespace
			sc.def = def
			sc.aliases = aliases
			sc.cache = cache
			sc.isEvent = true
			sc.acc = acc
			sc.tagKey = tagKey
			sc.annotate = cfg.Annotator
		})
		if err != nil {
			return nil, fmt.Errorf("record '%s': %w", entry.Name, err)
		}

		if cfg.Validate {
			data, err := a.Marshal(schema, map[string]any{})
			if err != nil {
				return nil, fmt.Errorf("record '%s' invalid generated defaults: %w", entry.Name, err)
			}
			data2, err := a.Marshal(schema, entry.Sample)
			if err != nil {
				return nil, fmt.Errorf("record '%s' invalid generated schema: %w", entry.Name, err)
			}
			if !reflect.DeepEqual(data, data2) {
				return nil, fmt.Errorf("record '%s': invalid generated defaults", entry.Name)
			}
		}

		schemas = append(schemas, schema)
	}

	schemas = append([]avro.Schema{avro.NewPrimitiveSchema(avro.Null, nil)}, schemas...)

	unionSch, err := avro.NewUnionSchema(schemas)
	if err != nil {
		return nil, err
	}

	rootNS := cfg.Namespace
	if rootNS == "" {
		rootNS = "_"
	}

	sc, err := schemaOf(cfg.RootType, func(c *schemaConfig) {
		c.inject = map[string]avro.Schema{cfg.InjectKey: unionSch}
		c.namespace = rootNS
		c.name = cfg.RootName
		c.root = true
		c.acc = acc
		c.tagKey = tagKey
		c.annotate = cfg.Annotator
	})
	if err != nil {
		return nil, err
	}

	root, ok := sc.(*avro.RecordSchema)
	if !ok {
		return nil, fmt.Errorf("envelope schema type must be a record, got %T", sc)
	}

	return root, nil
}

// UnpackUnionRecords returns the record sub-schemas wrapped in the named union field
// of an envelope record (skipping the null branch). It is the inverse of the union
// embedding done by BuildEnvelopeSchema.
func UnpackUnionRecords(schema *avro.RecordSchema, fieldName string) ([]*avro.RecordSchema, error) {
	schemas := make([]*avro.RecordSchema, 0)
	for _, f := range schema.Fields() {
		if f.Name() != fieldName {
			continue
		}
		sc, ok := f.Type().(*avro.UnionSchema)
		if !ok {
			return nil, fmt.Errorf("field '%s' must be a union, got %T", fieldName, f.Type())
		}
		for _, t := range sc.Types() {
			if t.Type() == avro.Null {
				continue
			}
			tt, ok := t.(*avro.RecordSchema)
			if !ok {
				return nil, fmt.Errorf("union member must be a record, got %T", t)
			}
			schemas = append(schemas, tt)
		}
	}

	return schemas, nil
}

// RegisterEntries registers each entry's Go type on a under the entry name, as a
// value and as a pointer. Without it, a decoded union holds a map instead of the
// Go type. BuildEnvelopeSchema calls it.
func RegisterEntries(a avro.API, entries []RecordEntry) error {
	for _, e := range entries {
		if e.Name == "" || e.Type == nil {
			return fmt.Errorf("%w: record entry needs a name and a type", ErrInvalidConfig)
		}
		if e.Type.Kind() != reflect.Struct {
			return fmt.Errorf("%w: record entry %s: type %s is not a struct", ErrInvalidConfig, e.Name, e.Type)
		}
		if e.Sample != nil && reflect.TypeOf(e.Sample) != e.Type {
			return fmt.Errorf("%w: record entry %s: sample is a %T, not a %s", ErrInvalidConfig, e.Name, e.Sample, e.Type)
		}
		a.Register(e.Name, reflect.Zero(e.Type).Interface())
		a.Register(e.Name, reflect.New(e.Type).Interface())
	}
	return nil
}
