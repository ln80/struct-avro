// Package registry resolves Avro schemas by ID, frames schema IDs into encoded
// payloads, and resolves data written with an older schema against the current
// one.
//
// A store is anything that implements Fetcher. It may also implement Persister,
// Walker and WireFormatter; Open detects them. The fs package provides a file
// store; other stores (a managed schema registry, for example) implement the
// same interfaces.
package registry

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/ln80/avro/v2"
	"github.com/ln80/struct-avro"
)

var (
	ErrInvalidSchemaID             = errors.New("invalid schema ID")
	ErrInvalidDataWireFormat       = errors.New("invalid data wire format")
	ErrMarshalOperationUnsupported = errors.New("marshal operation unsupported")
	ErrUnableToResolveSchema       = errors.New("unable to resolve schema")
	ErrUnableToSetupRegistry       = errors.New("unable to setup registry")
	ErrCreateOrUpdateSchemaFailed  = errors.New("failed to create or update schema")
	ErrSchemaNotFound              = errors.New("schema not found")
	ErrInvalidSchema               = errors.New("invalid schema")
	// ErrNoCurrentSchema is returned when encoding without a current schema
	// registered with WithCurrent.
	ErrNoCurrentSchema = errors.New("no current schema")
)

// WireFormatter frames/unframes the schema ID into/out of an encoded payload.
type WireFormatter interface {
	ExtractSchemaID(data []byte) (string, []byte, error)
	AppendSchemaID(data []byte, id string) ([]byte, error)
}

// Fetcher resolves schemas from a backing store.
type Fetcher interface {
	// Get returns the schema stored under id, as JSON. It returns an error
	// wrapping ErrSchemaNotFound when there is none.
	Get(ctx context.Context, id string) (schema string, err error)
	// GetByDefinition returns the ID of the stored schema with the same
	// canonical form as schema, or an error wrapping ErrSchemaNotFound.
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

// Option configures Open.
type Option func(*options)

type options struct {
	api            avro.API
	wireFormatter  WireFormatter
	current        *avro.RecordSchema
	readerOnly     bool
	persistCurrent bool
	latestReader   bool
}

// WithAPI sets the Avro API used to encode and decode. The default is
// structavro.NewAPI(). Types decoded from a union must be registered on it.
func WithAPI(api avro.API) Option { return func(o *options) { o.api = api } }

// WithWireFormatter sets how schema IDs are framed into payloads. By default the
// store frames them if it implements WireFormatter.
func WithWireFormatter(wf WireFormatter) Option {
	return func(o *options) { o.wireFormatter = wf }
}

// WithCurrent sets the schema this process writes with. Open looks it up in the
// store and fails if it is not there. Data written with another schema is
// resolved against it when decoded.
func WithCurrent(schema *avro.RecordSchema) Option {
	return func(o *options) { o.current, o.readerOnly = schema, false }
}

// WithReaderSchema sets the schema data is decoded into, without looking it up
// in the store. The registry then decodes only: Encode returns
// ErrNoCurrentSchema. Use it in readers that run ahead of the schema store.
func WithReaderSchema(schema *avro.RecordSchema) Option {
	return func(o *options) { o.current, o.readerOnly = schema, true }
}

// WithLatestReader decodes data written with any schema into the latest stored
// schema of the same namespace, when the registry has no current schema. Fields
// added later get their defaults and renamed fields are matched by alias. The
// store must implement Walker. Use it in readers that handle every namespace,
// such as a forwarder, without a current schema of their own.
func WithLatestReader() Option { return func(o *options) { o.latestReader = true } }

// WithPersistCurrent persists the current schema when the store does not have
// it. The store must implement Persister. Use it in development only: in
// production, schemas are persisted ahead of deployment by the tool package.
func WithPersistCurrent() Option { return func(o *options) { o.persistCurrent = true } }

type schemaEntry struct {
	id string
	// writer is the schema as stored. It encodes data under this ID.
	writer *avro.RecordSchema
	// reader decodes data written with writer: writer resolved against the
	// current schema, or writer itself when there is none.
	reader      *avro.RecordSchema
	readerBatch *avro.ArraySchema
	writerBatch *avro.ArraySchema
}

// Registry resolves and caches AVRO schemas, framing schema IDs into the wire
// format. It is safe for concurrent use. Its configuration does not change
// after Open.
type Registry struct {
	wireFormatter WireFormatter
	fetcher       Fetcher
	api           avro.API
	compatibility *avro.SchemaCompatibility

	// current is nil when the registry has no current schema. Its id is empty
	// when it was set with WithReaderSchema.
	current *schemaEntry

	// walker is set by WithLatestReader when there is no current schema.
	walker   Walker
	latestMu sync.Mutex
	latest   map[string]*avro.RecordSchema

	mu    sync.RWMutex
	cache map[string]*schemaEntry
}

// Open returns a registry reading schemas from store.
func Open(ctx context.Context, store Fetcher, opts ...Option) (*Registry, error) {
	if store == nil || (reflect.ValueOf(store).Kind() == reflect.Pointer && reflect.ValueOf(store).IsNil()) {
		return nil, fmt.Errorf("%w: no store", ErrUnableToSetupRegistry)
	}
	o := options{}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.api == nil {
		o.api = structavro.NewAPI()
	}
	if o.wireFormatter == nil {
		wf, ok := store.(WireFormatter)
		if !ok {
			return nil, fmt.Errorf("%w: the store does not frame schema IDs: use WithWireFormatter", ErrUnableToSetupRegistry)
		}
		o.wireFormatter = wf
	}

	r := &Registry{
		wireFormatter: o.wireFormatter,
		fetcher:       store,
		api:           o.api,
		compatibility: avro.NewSchemaCompatibility(),
		cache:         make(map[string]*schemaEntry),
	}

	if o.current == nil {
		if o.persistCurrent {
			return nil, fmt.Errorf("%w: WithPersistCurrent needs WithCurrent", ErrUnableToSetupRegistry)
		}
		if o.latestReader {
			w, ok := store.(Walker)
			if !ok {
				return nil, fmt.Errorf("%w: WithLatestReader needs a store that implements Walker", ErrUnableToSetupRegistry)
			}
			r.walker = w
			r.latest = make(map[string]*avro.RecordSchema)
		}
		return r, nil
	}
	if o.latestReader {
		return nil, fmt.Errorf("%w: WithLatestReader cannot be used with a current or reader schema", ErrUnableToSetupRegistry)
	}

	if o.readerOnly {
		r.current = newEntry("", o.current, o.current)
		return r, nil
	}

	id, err := store.GetByDefinition(ctx, o.current)
	if err != nil {
		if !errors.Is(err, ErrSchemaNotFound) || !o.persistCurrent {
			return nil, fmt.Errorf("%w: current schema %s: %w", ErrUnableToSetupRegistry, o.current.FullName(), err)
		}
		p, ok := store.(Persister)
		if !ok {
			return nil, fmt.Errorf("%w: the store cannot persist schemas", ErrCreateOrUpdateSchemaFailed)
		}
		if id, err = p.Persist(ctx, o.current); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrCreateOrUpdateSchemaFailed, err)
		}
	}
	if id == "" {
		return nil, fmt.Errorf("%w: the store returned an empty ID for the current schema", ErrUnableToSetupRegistry)
	}
	r.current = newEntry(id, o.current, o.current)
	r.cache[id] = r.current
	return r, nil
}

func newEntry(id string, writer, reader *avro.RecordSchema) *schemaEntry {
	return &schemaEntry{
		id:          id,
		writer:      writer,
		reader:      reader,
		writerBatch: avro.NewArraySchema(writer),
		readerBatch: avro.NewArraySchema(reader),
	}
}

// API returns the underlying AVRO API.
func (r *Registry) API() avro.API {
	return r.api
}

// ExtractSchemaID splits a framed payload into its schema ID and data.
func (r *Registry) ExtractSchemaID(data []byte) (string, []byte, error) {
	return r.wireFormatter.ExtractSchemaID(data)
}

// AppendSchemaID frames data with the schema ID.
func (r *Registry) AppendSchemaID(data []byte, id string) ([]byte, error) {
	return r.wireFormatter.AppendSchemaID(data, id)
}

// Current returns the ID and schema this registry writes with. ok is false when
// the registry has no current schema or was opened with WithReaderSchema.
func (r *Registry) Current() (id string, schema *avro.RecordSchema, ok bool) {
	if r.current == nil || r.current.id == "" {
		return "", nil, false
	}
	return r.current.id, r.current.writer, true
}

// entry returns the cached entry for id, fetching and resolving it if needed.
func (r *Registry) entry(ctx context.Context, id string) (*schemaEntry, error) {
	r.mu.RLock()
	e, ok := r.cache[id]
	r.mu.RUnlock()
	if ok {
		return e, nil
	}

	out, err := r.fetcher.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	parsed, err := avro.Parse(out)
	if err != nil {
		return nil, fmt.Errorf("%w: schema %s: %w", ErrInvalidSchema, id, err)
	}
	writer, ok := parsed.(*avro.RecordSchema)
	if !ok {
		return nil, fmt.Errorf("%w: schema %s is not a record", ErrInvalidSchema, id)
	}

	reader := writer
	if r.walker != nil {
		latest, err := r.latestOf(ctx, writer.Namespace())
		if err != nil {
			return nil, err
		}
		if latest != nil && latest.Fingerprint() != writer.Fingerprint() {
			resolved, err := r.compatibility.Resolve(latest, writer)
			if err != nil {
				return nil, fmt.Errorf("%w: schema %s: %w", ErrUnableToResolveSchema, id, err)
			}
			if reader, ok = resolved.(*avro.RecordSchema); !ok {
				return nil, fmt.Errorf("%w: schema %s did not resolve to a record", ErrUnableToResolveSchema, id)
			}
		}
	}
	if r.current != nil {
		resolved, err := r.compatibility.Resolve(r.current.reader, writer)
		if err != nil {
			return nil, fmt.Errorf("%w: schema %s: %w", ErrUnableToResolveSchema, id, err)
		}
		if reader, ok = resolved.(*avro.RecordSchema); !ok {
			return nil, fmt.Errorf("%w: schema %s did not resolve to a record", ErrUnableToResolveSchema, id)
		}
	}

	e = newEntry(id, writer, reader)
	r.mu.Lock()
	defer r.mu.Unlock()
	if cached, ok := r.cache[id]; ok {
		return cached, nil
	}
	r.cache[id] = e
	return e, nil
}

// latestOf returns the latest stored schema of namespace, or nil if it has none.
// It walks the store once per namespace.
func (r *Registry) latestOf(ctx context.Context, namespace string) (*avro.RecordSchema, error) {
	r.latestMu.Lock()
	defer r.latestMu.Unlock()
	if s, ok := r.latest[namespace]; ok {
		return s, nil
	}
	var latest *avro.RecordSchema
	if _, err := r.walker.Walk(ctx, func(_ string, _ int64, isLatest bool, s *avro.RecordSchema) error {
		if isLatest && s.Namespace() == namespace {
			latest = s
		}
		return nil
	}, func(c *WalkConfig) { c.Namespaces = []string{namespace} }); err != nil {
		return nil, fmt.Errorf("%w: latest schema of %s: %w", ErrUnableToResolveSchema, namespace, err)
	}
	r.latest[namespace] = latest
	return latest, nil
}

// GetSchema returns the schema to decode data written under id: the stored
// schema resolved against the current one, or the stored schema when the
// registry has no current schema. The second schema decodes a batch.
func (r *Registry) GetSchema(ctx context.Context, id string) (string, *avro.RecordSchema, *avro.ArraySchema, error) {
	e, err := r.entry(ctx, id)
	if err != nil {
		return "", nil, nil, err
	}
	return id, e.reader, e.readerBatch, nil
}

// Encode encodes v with the current schema and frames the schema ID.
func (r *Registry) Encode(v any) ([]byte, error) {
	if r.current == nil || r.current.id == "" {
		return nil, ErrNoCurrentSchema
	}
	return r.encode(r.current.id, r.current.writer, v)
}

// EncodeBatch encodes vs, a slice, as an array of the current schema and frames
// the schema ID once.
func (r *Registry) EncodeBatch(vs any) ([]byte, error) {
	if r.current == nil || r.current.id == "" {
		return nil, ErrNoCurrentSchema
	}
	return r.encode(r.current.id, r.current.writerBatch, vs)
}

// EncodeWithID encodes v with the stored schema id, for example to write a
// decoded record again unchanged.
func (r *Registry) EncodeWithID(ctx context.Context, id string, v any) ([]byte, error) {
	e, err := r.entry(ctx, id)
	if err != nil {
		return nil, err
	}
	return r.encode(id, e.writer, v)
}

// EncodeBatchWithID encodes vs, a slice, as an array of the stored schema id.
func (r *Registry) EncodeBatchWithID(ctx context.Context, id string, vs any) ([]byte, error) {
	e, err := r.entry(ctx, id)
	if err != nil {
		return nil, err
	}
	return r.encode(id, e.writerBatch, vs)
}

func (r *Registry) encode(id string, schema avro.Schema, v any) ([]byte, error) {
	b, err := r.api.Marshal(schema, v)
	if err != nil {
		return nil, err
	}
	return r.wireFormatter.AppendSchemaID(b, id)
}

// Decode reads the schema ID from data, resolves its schema and decodes the
// payload into v. It returns the schema ID.
func (r *Registry) Decode(ctx context.Context, data []byte, v any) (string, error) {
	return r.decode(ctx, data, v, false)
}

// DecodeBatch decodes a payload written by EncodeBatch into v, a pointer to a
// slice. It returns the schema ID.
func (r *Registry) DecodeBatch(ctx context.Context, data []byte, v any) (string, error) {
	return r.decode(ctx, data, v, true)
}

func (r *Registry) decode(ctx context.Context, data []byte, v any, batch bool) (string, error) {
	id, payload, err := r.wireFormatter.ExtractSchemaID(data)
	if err != nil {
		return "", err
	}
	e, err := r.entry(ctx, id)
	if err != nil {
		return "", err
	}
	var schema avro.Schema = e.reader
	if batch {
		schema = e.readerBatch
	}
	if err := r.api.Unmarshal(schema, payload, v); err != nil {
		return "", err
	}
	return id, nil
}
