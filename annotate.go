package structavro

import (
	"fmt"
	"reflect"

	sensitive "github.com/ln80/struct-sensitive"
)

// SensitiveProperty is the Avro field property SensitiveTags writes. The default
// code generation template reads it back to rebuild the struct tag.
const SensitiveProperty = "sensitive"

// Annotation is metadata a FieldAnnotator attaches to an Avro field.
type Annotation struct {
	// Property is the name of the Avro field property that holds Value.
	Property string
	// Value is written as the property value. It must marshal to JSON.
	Value any
	// Token is folded into the schema fingerprint, so that a change to the
	// annotation changes the schema and its ID. Two different annotations must
	// have different tokens.
	Token string
}

// FieldAnnotator returns the annotation of a struct field, or nil when the field
// has none. An error stops schema generation; it should name the problem, not
// repeat the field's value.
type FieldAnnotator func(f reflect.StructField) (*Annotation, error)

// SensitiveTags is the default FieldAnnotator. It reads the struct-sensitive
// tags (sensitive, pii or sens) and writes them to the "sensitive" property, as
// the struct-sensitive TagPayload. The fingerprint token is
// "<field type name>.<field name>.<tag>", which is part of the schema format:
// changing it changes the ID of every schema with a sensitive field.
func SensitiveTags(f reflect.StructField) (*Annotation, error) {
	payload, err := sensitive.ParseTag(f.Tag)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	return &Annotation{
		Property: SensitiveProperty,
		Value:    payload,
		Token:    f.Type.Name() + "." + f.Name + "." + payload.Marshal(),
	}, nil
}

// NoAnnotations is a FieldAnnotator that annotates no field.
func NoAnnotations(reflect.StructField) (*Annotation, error) { return nil, nil }

// FingerprintFields names the two string fields BuildEnvelopeSchema adds to the
// envelope record. Avro's canonical form, from which schema IDs are computed,
// drops field defaults and custom properties. Each added field's name ends with
// a hash of the record defaults or of the field annotations, so that a change to
// either changes the schema ID and is persisted as a new version.
//
// The names are part of the stored schemas. Changing a prefix adds a new schema
// version: the new fields have defaults, so old data stays readable.
type FingerprintFields struct {
	// Defaults prefixes the field carrying the hash of the record defaults.
	// Empty means LegacyDefaultsPrefix.
	Defaults string
	// Annotations prefixes the field carrying the hash of the field
	// annotations. Empty means LegacyAnnotationsPrefix.
	Annotations string
}

const (
	// LegacyDefaultsPrefix is the default FingerprintFields.Defaults.
	LegacyDefaultsPrefix = "defxyz"
	// LegacyAnnotationsPrefix is the default FingerprintFields.Annotations.
	LegacyAnnotationsPrefix = "sensitivexyz"
)

func (f FingerprintFields) withDefaults() (FingerprintFields, error) {
	if f.Defaults == "" {
		f.Defaults = LegacyDefaultsPrefix
	}
	if f.Annotations == "" {
		f.Annotations = LegacyAnnotationsPrefix
	}
	if f.Defaults == f.Annotations {
		return f, fmt.Errorf("%w: fingerprint field prefixes must differ", ErrInvalidConfig)
	}
	for _, p := range []string{f.Defaults, f.Annotations} {
		if !validAvroName(p) {
			return f, fmt.Errorf("%w: fingerprint field prefix %q is not an Avro name", ErrInvalidConfig, p)
		}
	}
	return f, nil
}

// validAvroName reports whether s matches [A-Za-z_][A-Za-z0-9_]*.
func validAvroName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '_', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
