package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ln80/avro/v2"
)

var (
	ErrInvalidSchemaID             = errors.New("invalid schema ID")
	ErrInvalidDataWireFormat       = errors.New("invalid data wire format")
	ErrMarshalOperationUnsupported = errors.New("marshal operation unsupported")
	ErrUnableToResolveSchema       = errors.New("unable to resolve schema")
	ErrUnableToSetupRegistry       = errors.New("unable to setup registry")
	ErrCreateOrUpdateSchemaFailed  = errors.New("failed to create or update schema")
	ErrSchemaNotFound              = errors.New("schema not found")
)

// WireFormatter frames/unframes the schema ID into/out of an encoded payload.
type WireFormatter interface {
	ExtractSchemaID(data []byte) (string, []byte, error)
	AppendSchemaID(data []byte, id string) ([]byte, error)
}

// Fetcher resolves schemas from a backing store.
type Fetcher interface {
	Get(ctx context.Context, id string) (schema string, err error)
	GetByDefinition(ctx context.Context, schema avro.Schema) (id string, err error)
}

type PersistConfig struct {
	Resolver func(schema *avro.RecordSchema) (id string, version int, err error)
}

// Persister stores schemas in a backing store.
type Persister interface {
	Persist(ctx context.Context, schema *avro.RecordSchema, opts ...func(*PersistConfig)) (id string, err error)
}

type WalkConfig struct {
	Namespaces []string
}

// Walker iterates over the persisted schema versions.
type Walker interface {
	Walk(ctx context.Context, fn func(id string, version int64, latest bool, schema *avro.RecordSchema) error, opts ...func(*WalkConfig)) (int, error)
}

// SchemaIDGetter is implemented by values that already carry a resolved schema ID
// (e.g. a decoded record that should be re-encoded with the same schema).
type SchemaIDGetter interface{ AVROSchemaID() string }

type schemaEntry struct {
	schema      *avro.RecordSchema
	batchSchema *avro.ArraySchema
	schemaID    string
}

type RegistryConfig struct {
	Namespace      bool
	PersistCurrent bool
	ReadOnly       bool
}

// Registry resolves and caches AVRO schemas, framing schema IDs into the wire format.
//
// Concurrency contract: Setup must complete (once) before any concurrent call to
// GetCurrent/GetSchema. After Setup, `current` and `cfg` are read-only; the schema
// cache is guarded by mu.
type Registry struct {
	wireFormatter WireFormatter

	fetcher   Fetcher
	persister Persister

	cfg *RegistryConfig

	api           avro.API
	compatibility *avro.SchemaCompatibility

	current *schemaEntry
	cache   map[string]schemaEntry
	// cacheByFingerprint indexes cached schema IDs by their string fingerprint
	// to avoid a linear scan on every definition lookup.
	cacheByFingerprint map[string]string
	mu                 sync.RWMutex
}

func New(fetcher Fetcher, persister Persister, wf WireFormatter) *Registry {
	reg := &Registry{
		wireFormatter:      wf,
		fetcher:            fetcher,
		persister:          persister,
		compatibility:      avro.NewSchemaCompatibility(),
		api:                avro.Config{PartialUnionTypeResolution: true, UnionResolutionError: false}.Freeze(),
		cache:              make(map[string]schemaEntry),
		cacheByFingerprint: make(map[string]string),
		current:            nil,
	}

	return reg
}

// ExtractSchemaID implements WireFormatter.
func (r *Registry) ExtractSchemaID(data []byte) (string, []byte, error) {
	return r.wireFormatter.ExtractSchemaID(data)
}

// AppendSchemaID implements WireFormatter.
func (r *Registry) AppendSchemaID(data []byte, id string) ([]byte, error) {
	return r.wireFormatter.AppendSchemaID(data, id)
}

// getSchemaByDefinition resolves the schema ID for a given schema definition,
// caching the result. Caller must NOT hold r.mu.
func (r *Registry) getSchemaByDefinition(ctx context.Context, schema avro.Schema) (string, error) {
	fp := schema.Fingerprint()
	fpKey := string(fp[:])

	r.mu.RLock()
	if id, ok := r.cacheByFingerprint[fpKey]; ok {
		r.mu.RUnlock()
		return id, nil
	}
	r.mu.RUnlock()

	id, err := r.fetcher.GetByDefinition(ctx, schema)
	if err != nil {
		return "", err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.current != nil && r.current.schemaID != id {
		schema, err = r.compatibility.Resolve(r.current.schema, schema)
		if err != nil {
			return "", err
		}
	}
	r.storeLocked(id, schema)

	return id, nil
}

// storeLocked records a resolved schema in the cache. Caller must hold r.mu.
func (r *Registry) storeLocked(id string, schema avro.Schema) {
	rec := schema.(*avro.RecordSchema)
	r.cache[id] = schemaEntry{
		schema:      rec,
		batchSchema: avro.NewArraySchema(schema),
		schemaID:    id,
	}
	fp := rec.Fingerprint()
	r.cacheByFingerprint[string(fp[:])] = id
}

// API returns the underlying AVRO API.
func (r *Registry) API() avro.API {
	return r.api
}

// Setup resolves (and optionally persists) the current schema. It must run once
// before any concurrent Get* call.
func (r *Registry) Setup(ctx context.Context, schema avro.Schema, opts ...func(*RegistryConfig)) error {
	cfg := &RegistryConfig{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(cfg)
	}
	r.cfg = cfg

	if schema == nil {
		return nil
	}

	if r.current != nil {
		return nil
	}

	if r.cfg.ReadOnly {
		r.current = &schemaEntry{
			schema:      schema.(*avro.RecordSchema),
			batchSchema: avro.NewArraySchema(schema),
		}
		return nil
	}

	id, err := r.getSchemaByDefinition(ctx, schema)
	if err != nil {
		if errors.Is(err, ErrSchemaNotFound) && r.cfg.PersistCurrent {
			id, err = r.persister.Persist(ctx, schema.(*avro.RecordSchema))
			if err != nil {
				return fmt.Errorf("%w: %v", ErrCreateOrUpdateSchemaFailed, err)
			}
		} else {
			return fmt.Errorf("%w: %v", ErrUnableToSetupRegistry, err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.current = &schemaEntry{
		schemaID:    id,
		schema:      schema.(*avro.RecordSchema),
		batchSchema: avro.NewArraySchema(schema),
	}

	return nil
}

// GetCurrent returns the current schema, or the schema referenced by v's schema ID.
func (r *Registry) GetCurrent(ctx context.Context, v SchemaIDGetter) (schemaID string, schema *avro.RecordSchema, batchSchema *avro.ArraySchema, err error) {
	if r.current != nil {
		return r.current.schemaID, r.current.schema, r.current.batchSchema, nil
	}

	id := v.AVROSchemaID()
	if id == "" {
		err = ErrSchemaNotFound
		return
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.cache[id]
	if !ok {
		err = ErrSchemaNotFound
		return
	}

	return id, entry.schema, entry.batchSchema, nil
}

// GetSchema resolves a schema by ID, fetching and caching it if needed.
func (r *Registry) GetSchema(ctx context.Context, id string) (string, *avro.RecordSchema, *avro.ArraySchema, error) {
	if r.current != nil && r.current.schemaID == id {
		return id, r.current.schema, r.current.batchSchema, nil
	}

	r.mu.RLock()
	entry, ok := r.cache[id]
	r.mu.RUnlock()
	if ok {
		return id, entry.schema, entry.batchSchema, nil
	}

	out, err := r.fetcher.Get(ctx, id)
	if err != nil {
		return "", nil, nil, err
	}

	schema := avro.MustParse(out)
	if r.current != nil {
		schema, err = r.compatibility.Resolve(r.current.schema, schema)
		if err != nil {
			return "", nil, nil, err
		}
	}

	r.mu.Lock()
	r.storeLocked(id, schema)
	res := r.cache[id]
	r.mu.Unlock()

	return id, res.schema, res.batchSchema, nil
}
