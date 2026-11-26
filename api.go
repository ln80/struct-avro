package avro

import (
	avro "github.com/ln80/avro/v2"
)

const (
	// DefaultMaxByteSliceSize is the largest bytes or string value the decoder
	// reads, in bytes.
	DefaultMaxByteSliceSize = 1 << 20
	// DefaultMaxSliceAllocSize is the largest number of array elements the
	// decoder allocates. Without a limit, an array length read from corrupt or
	// hostile bytes makes the decoder allocate that many elements up front.
	DefaultMaxSliceAllocSize = 1 << 20
)

// APIOption configures NewAPI.
type APIOption func(*avro.Config)

// WithMaxByteSliceSize sets the largest bytes or string value the decoder reads.
// A value below 1 keeps the default.
func WithMaxByteSliceSize(n int) APIOption {
	return func(c *avro.Config) {
		if n > 0 {
			c.MaxByteSliceSize = n
		}
	}
}

// WithMaxSliceAllocSize sets the largest number of array elements the decoder
// allocates. A value below 1 keeps the default.
func WithMaxSliceAllocSize(n int) APIOption {
	return func(c *avro.Config) {
		if n > 0 {
			c.MaxSliceAllocSize = n
		}
	}
}

// NewAPI returns the AVRO API configuration used to encode/decode envelopes:
// partial union type resolution enabled and union resolution errors disabled, so
// that unknown union branches decode to nil rather than failing. Decoding is
// bounded by DefaultMaxByteSliceSize and DefaultMaxSliceAllocSize unless an
// option changes them.
func NewAPI(opts ...APIOption) avro.API {
	cfg := avro.Config{
		PartialUnionTypeResolution: true,
		UnionResolutionError:       false,
		MaxByteSliceSize:           DefaultMaxByteSliceSize,
		MaxSliceAllocSize:          DefaultMaxSliceAllocSize,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg.Freeze()
}

// NewCompatibilityAPI returns a fresh AVRO schema-compatibility checker/resolver.
func NewCompatibilityAPI() *avro.SchemaCompatibility {
	return avro.NewSchemaCompatibility()
}
