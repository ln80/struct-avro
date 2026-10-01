package avro

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	avro "github.com/ln80/avro/v2"
	sensitive "github.com/ln80/struct-sensitive"
)

// Cache deduplicates named (record) schemas while walking a struct tree, so that a
// type referenced more than once is emitted once and referenced via a RefSchema.
type Cache map[string]avro.NamedSchema

// NewCache returns an empty schema dedup cache, shareable across several SchemaOf
// calls that belong to the same logical schema (e.g. events in one envelope).
func NewCache() Cache { return make(Cache) }

// Config controls a single SchemaOf invocation. Use the With* options to set it.
type Config struct {
	// name forces the record schema name (used for the root envelope).
	name string
	// namespace given to the record schema.
	namespace string
	// inject substitutes a precomputed schema for fields tagged `ev:",inject=<key>"`.
	// Only honored on the root (non-event) record.
	inject map[string]avro.Schema
	// tagKey is the struct tag key carrying schema options (default "ev").
	tagKey string
	// def is the default value to attach to the resolved schema.
	def any
	// aliases to add to the resolved schema.
	aliases []string
	// cache deduplicates generated named schemas.
	cache Cache
	// isEvent marks record schemas that represent an event (emitted as a custom prop).
	isEvent bool
	// root marks the top-level (envelope) schema vs nested ones.
	root bool
	// acc accumulates defaults/sensitive metadata while walking, to emit the
	// envelope-level fingerprint fields.
	acc *Accumulator
}

// Option configures a SchemaOf call.
type Option func(*Config)

func WithName(name string) Option           { return func(c *Config) { c.name = name } }
func WithNamespace(ns string) Option        { return func(c *Config) { c.namespace = ns } }
func WithDefault(def any) Option            { return func(c *Config) { c.def = def } }
func WithAliases(aliases []string) Option   { return func(c *Config) { c.aliases = aliases } }
func WithCache(cache Cache) Option          { return func(c *Config) { c.cache = cache } }
func WithAccumulator(a *Accumulator) Option { return func(c *Config) { c.acc = a } }
func WithEvent(isEvent bool) Option         { return func(c *Config) { c.isEvent = isEvent } }
func WithRoot(root bool) Option             { return func(c *Config) { c.root = root } }
func WithTagKey(key string) Option          { return func(c *Config) { c.tagKey = key } }

// WithInject substitutes the given schema for fields tagged `ev:",inject=<key>"`.
func WithInject(key string, schema avro.Schema) Option {
	return func(c *Config) {
		if c.inject == nil {
			c.inject = make(map[string]avro.Schema)
		}
		c.inject[key] = schema
	}
}

// SchemaOf generates the AVRO schema for a Go type.
func SchemaOf(t reflect.Type, opts ...Option) (avro.Schema, error) {
	cfgOpts := make([]func(*Config), 0, len(opts))
	for _, o := range opts {
		cfgOpts = append(cfgOpts, o)
	}
	return schemaOf(t, cfgOpts...)
}

func schemaOf(t reflect.Type, opts ...func(*Config)) (avro.Schema, error) {
	cfg := &Config{
		inject: make(map[string]avro.Schema),
		tagKey: DefaultTagKey,
		def:    nil,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(cfg)
	}
	if cfg.tagKey == "" {
		cfg.tagKey = DefaultTagKey
	}

	// the basic Config to pass down to complex child fields
	childOpt := func(sc *Config) {
		sc.tagKey = cfg.tagKey
		sc.inject = cfg.inject
		sc.namespace = cfg.namespace
		sc.cache = cfg.cache
		sc.root = false
		sc.acc = cfg.acc
	}

	avroOpts := make([]avro.SchemaOption, 0)

	props := map[string]any{}
	if cfg.isEvent {
		props["isEvent"] = true
	}
	if len(props) > 0 {
		avroOpts = append(avroOpts, avro.WithProps(props))
	}

	if len(cfg.aliases) > 0 {
		avroOpts = append(avroOpts, avro.WithAliases(cfg.aliases))
	}

	primitiveSchema, err := schemaOfPrimitives(t, avroOpts)
	if err != nil {
		return nil, err
	}
	if primitiveSchema != nil {
		return primitiveSchema, nil
	}

	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		es, err := schemaOf(t.Elem(), childOpt)
		if err != nil {
			return nil, err
		}
		return avro.NewArraySchema(es, avroOpts...), nil

	case reflect.Map:
		es, err := schemaOf(t.Elem(), childOpt)
		if err != nil {
			return nil, err
		}
		return avro.NewMapSchema(es, avroOpts...), nil

	case reflect.Pointer:
		n := avro.NewPrimitiveSchema(avro.Null, nil)
		es, err := schemaOf(t.Elem(), childOpt)
		if err != nil {
			return nil, err
		}
		return avro.NewUnionSchema([]avro.Schema{n, es})

	case reflect.Struct:
		return schemaOfStruct(t, avroOpts, cfg, childOpt)

	default:
		return nil, fmt.Errorf("event schema: unknown type %s %v", t.Kind().String(), t)
	}
}

var byteType = reflect.TypeFor[[]byte]()

// schemaOfPrimitives generates a schema for primitive Go types. It returns a nil
// schema (no error) if the type is not primitive.
//
// NOTE: the integer/duration/uint mappings below are intentionally preserved
// byte-for-byte from the original event-store implementation. Changing them would
// alter generated schema bytes and therefore the schema IDs used to decode already
// persisted data. Revisit only behind an explicit schema-version migration.
func schemaOfPrimitives(t reflect.Type, avroOpts []avro.SchemaOption) (avro.Schema, error) {
	if t == byteType {
		return avro.NewPrimitiveSchema(avro.Bytes, nil, avroOpts...), nil
	}

	switch t.Kind() {
	case reflect.String:
		return avro.NewPrimitiveSchema(avro.String, nil, avroOpts...), nil

	case reflect.Bool:
		return avro.NewPrimitiveSchema(avro.Boolean, nil, avroOpts...), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16:
		return avro.NewPrimitiveSchema(avro.Int, nil, avroOpts...), nil

	case reflect.Int64, reflect.Uint32:
		if strings.Contains(strings.ToLower(t.String()), "time.duration") {
			return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimeMicros)), nil
		}
		return avro.NewPrimitiveSchema(avro.Long, nil, avroOpts...), nil

	case reflect.Uint64:
		return avro.NewPrimitiveSchema(avro.Fixed, nil, avroOpts...), nil

	case reflect.Float32:
		return avro.NewPrimitiveSchema(avro.Float, nil, avroOpts...), nil

	case reflect.Float64:
		return avro.NewPrimitiveSchema(avro.Double, nil, avroOpts...), nil

	case reflect.Struct:
		if t.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMillis), avroOpts...), nil
		}
	}

	return nil, nil
}

// schemaOfStruct generates the schema of the given struct type.
func schemaOfStruct(t reflect.Type, avroOpts []avro.SchemaOption, cfg *Config, childOpt func(*Config)) (avro.Schema, error) {
	name := cfg.name
	// If the name is not provided as config then extract it from the struct type name.
	if name == "" {
		// In the case of a generic type, ignore the type-param part.
		name = strings.Split(t.String(), "[")[0]

		// In the case of a provided namespace, ensure the fullName validity by
		// dropping the package qualifier (the namespace replaces it).
		if cfg.namespace != "" {
			parts := strings.Split(name, ".")
			name = parts[len(parts)-1]
		}
	}

	fullName := name
	if cfg.namespace != "" {
		fullName = cfg.namespace + "." + fullName
	}

	fields := make([]*avro.Field, 0)

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}

		// extract schema options from the tag (default key "ev")
		_, evOpts := parseTag(cfg.tagKey, f.Tag)

		if f.Anonymous {
			ft := f.Type
			// TBD: in the case of an anonymous struct pointer field, do not follow
			// the nullable-union logic.
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				// resolve the schema of the nested struct using the same default as the parent
				anonymousSchema, err := schemaOf(ft, childOpt, func(sc *Config) {
					sc.def = cfg.def
					sc.acc = cfg.acc
					sc.cache = cfg.cache
				})
				if err != nil {
					return nil, err
				}

				switch s := anonymousSchema.(type) {

				// For a record, append its fields to the parent schema.
				case *avro.RecordSchema:
					fields = append(fields, s.Fields()...)

				// For a RefSchema, append fields to the parent while adjusting defaults.
				//
				// Reminder: a RefSchema means the schema was resolved from a previous
				// event and cached. Consequently its defaults belong to another event.
				case *avro.RefSchema:
					for _, rf := range s.Schema().(*avro.RecordSchema).Fields() {
						refFieldOpts := append(make([]avro.SchemaOption, 0), avro.WithAliases(rf.Aliases()), avro.WithOrder(rf.Order()), avro.WithProps(rf.Props()))

						// Override the field default extracted from the ref if the parent has one.
						var fieldDef any
						if rf.HasDefault() {
							fieldDef = rf.Default()
						}
						def, found, err := fieldDefault(cfg.def, rf.Name())
						if err != nil {
							return nil, err
						}
						if found {
							fieldDef = def
						}
						refFieldOpts = append(refFieldOpts, avro.WithDefault(fieldDef))

						field, err := avro.NewField(rf.Name(), deduplicateType(cfg.cache, rf.Type()), refFieldOpts...)
						if err != nil {
							return nil, err
						}
						fields = append(fields, field)
					}
				default:
					return nil, errors.New("invalid anonymous field AVRO schema type")
				}
				continue
			}

			return nil, errors.New("invalid anonymous field type. only struct or struct pointer are supported")
		}

		fieldName := f.Name

		// the avro tag name is only considered for the (non-event) root envelope
		if cfg.root && !cfg.isEvent {
			if tag, ok := f.Tag.Lookup("avro"); ok {
				fieldName = tag
			}
		}

		var fieldSchema avro.Schema

		// limit 'inject' to the root (envelope) as the drawbacks in the general case
		// are unclear.
		if cfg.root && !cfg.isEvent {
			if inject, ok := evOpts["inject"]; ok && len(inject) > 0 && inject[0] != "" {
				if sch, ok := cfg.inject[inject[0]]; ok {
					fieldSchema = sch
				}
			}
		}

		// Extract the field default from the parent. Required when a field is itself a
		// struct (resolved as a nested record, which also needs a default).
		fieldDef, fieldDefFound, err := fieldDefault(cfg.def, fieldName)
		if err != nil {
			return nil, err
		}
		// Nested records under *T / []T are often resolved with cfg.def == nil, so their
		// fields would otherwise get no AVRO default and break backward-compatible
		// evolution. Fall back to the Go zero value so additive nested fields (string,
		// []string, ...) emit defaults ("" / [] / 0 / false / null for pointers).
		if !fieldDefFound {
			zd, err := defaultOf(reflect.Zero(f.Type).Interface())
			if err != nil {
				return nil, err
			}
			fieldDef, fieldDefFound = zd, true
		}

		// resolve the field schema
		if fieldSchema == nil {
			fieldSchema, err = schemaOf(f.Type, childOpt, func(sc *Config) {
				// resolve field-type aliases from a blank field defined at the field
				// struct level (distinct from direct field-name aliases).
				if typeAliases := structTypeAliases(cfg.tagKey, f.Type, nil); len(typeAliases) > 0 {
					sc.aliases = typeAliases
				}
				if fieldDefFound {
					sc.def = fieldDef
				}
			})
			if err != nil {
				return nil, err
			}
		}

		avroFieldOpts := make([]avro.SchemaOption, 0)

		if fieldDef != nil {
			avroFieldOpts = append(avroFieldOpts, avro.WithDefault(fieldDef))
			if cfg.acc != nil && !cfg.root {
				cfg.acc.appendDefault(fieldDef)
			}
		}

		// only accept a nil default for a nullable union type
		if fieldDef == nil {
			if fieldSchema.Type() == avro.Union && fieldSchema.(*avro.UnionSchema).Types()[0].Type() == avro.Null {
				avroFieldOpts = append(avroFieldOpts, avro.WithDefault(fieldDef))
				if cfg.acc != nil && !cfg.root {
					cfg.acc.appendDefault(fieldDef)
				}
			}
		}

		// field-name aliases
		if aliases, ok := evOpts["aliases"]; ok && len(aliases) > 0 {
			avroFieldOpts = append(avroFieldOpts, avro.WithAliases(aliases))
		}

		tagPayload := sensitive.ParseTag(f.Tag)
		if tagPayload != nil {
			avroFieldOpts = append(avroFieldOpts, avro.WithProps(map[string]any{
				"sensitive": tagPayload,
			}))
			if cfg.acc != nil {
				cfg.acc.appendSensitive(f.Type.Name() + "." + f.Name + "." + tagPayload.Marshal())
			}
		}
		field, err := avro.NewField(fieldName, fieldSchema, avroFieldOpts...)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}

	if cfg.root && cfg.acc != nil {
		if sensF := cfg.acc.sensitiveField(); sensF != nil {
			fields = append(fields, sensF)
		}
		if defF := cfg.acc.defaultField(); defF != nil {
			fields = append(fields, defF)
		}
	}

	if !cfg.root && cfg.cache != nil {
		if s, ok := cfg.cache[fullName]; ok {
			return avro.NewRefSchema(s), nil
		}
	}

	sh, err := avro.NewRecordSchema(name, cfg.namespace, fields, avroOpts...)
	if err != nil {
		return nil, err
	}

	if cfg.cache != nil {
		cfg.cache[sh.FullName()] = sh
	}

	return sh, nil
}

// deduplicateType replaces a type (or subtype) with a ref if it already exists in the cache.
func deduplicateType(cache Cache, schema avro.Schema) avro.Schema {
	if schema.Type() == avro.Ref {
		return schema
	}

	if schema.Type() == avro.Record {
		sch := schema.(*avro.RecordSchema)
		if s, ok := cache[sch.FullName()]; ok {
			return avro.NewRefSchema(s)
		}
		return sch
	}

	if schema.Type() == avro.Array {
		return avro.NewArraySchema(deduplicateType(cache, schema.(*avro.ArraySchema).Items()))
	}
	if schema.Type() == avro.Map {
		return avro.NewMapSchema(deduplicateType(cache, schema.(*avro.MapSchema).Values()))
	}

	if schema.Type() == avro.Union {
		schemas := make([]avro.Schema, 0)
		for _, s := range schema.(*avro.UnionSchema).Types() {
			schemas = append(schemas, deduplicateType(cache, s))
		}
		s, _ := avro.NewUnionSchema(schemas)
		return s
	}

	return schema
}

// structTypeAliases resolves type-name aliases declared on a struct's blank "_" field tag.
func structTypeAliases(tagKey string, t reflect.Type, defAliases []string) []string {
	aliases := append([]string{}, defAliases...)
	if t.Kind() != reflect.Struct {
		return aliases
	}

	blankF, ok := t.FieldByName("_")
	if !ok {
		return aliases
	}
	_, evOpts := parseTag(tagKey, blankF.Tag)
	evAls, ok := evOpts["aliases"]
	if !ok {
		return aliases
	}

	for _, a := range evAls {
		found := false
		for _, aa := range aliases {
			if aa == a {
				found = true
				break
			}
		}
		if !found {
			aliases = append(aliases, a)
		}
	}

	return aliases
}
