package avro

import (
	avro "github.com/ln80/avro/v2"
)

// NewAPI returns the AVRO API configuration used to encode/decode envelopes:
// partial union type resolution enabled and union resolution errors disabled, so
// that unknown union branches decode to nil rather than failing.
func NewAPI() avro.API {
	return avro.Config{PartialUnionTypeResolution: true, UnionResolutionError: false}.Freeze()
}

// NewCompatibilityAPI returns a fresh AVRO schema-compatibility checker/resolver.
func NewCompatibilityAPI() *avro.SchemaCompatibility {
	return avro.NewSchemaCompatibility()
}
