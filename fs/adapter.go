package fs

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	avro "github.com/ln80/avro/v2"
	"github.com/ln80/struct-avro/registry"
)

// schemaIDLen is the fixed wire-format schema ID length in bytes. IDs are the hex
// encoding of an 8-byte CRC64 fingerprint (16 hex chars).
const schemaIDLen = 16

// Adapter is a filesystem-backed registry store.
//
// Reads go through the provided fs.FS (which may be an embed.FS or in-memory FS).
// Writes (Persist) target the real OS filesystem rooted at dir; dir must therefore
// be a real directory path consistent with fs when persistence is used.
type Adapter struct {
	fs  fs.FS
	dir string
}

func NewAdapter(f fs.FS, dir string) *Adapter {
	return &Adapter{fs: f, dir: dir}
}

func NewDirAdapter(dir string) *Adapter {
	return NewAdapter(os.DirFS(dir), dir)
}

// validID reports whether id is a schema ID this adapter writes: the lowercase
// hex encoding of an 8-byte fingerprint. It is checked before an ID is used in a
// glob pattern, because an ID read from stored bytes may hold '*', '?' or '['.
func validID(id string) bool {
	if len(id) != schemaIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// parseSchemaPath extracts (version, namespace, id) from a "<ns>/<version>@<id>.json" path.
func parseSchemaPath(path string) (version int64, namespace, id string, err error) {
	splits := strings.Split(path, "/")
	if len(splits) < 2 {
		err = fmt.Errorf("invalid schema path: '%s'", path)
		return
	}
	namespace = strings.TrimSpace(splits[len(splits)-2])

	name := splits[len(splits)-1]
	at := strings.SplitN(name, "@", 2)
	if len(at) != 2 {
		err = fmt.Errorf("invalid schema path: '%s' missing version separator", path)
		return
	}
	version, err = strconv.ParseInt(strings.TrimSpace(at[0]), 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid schema path: '%s' bad version: %w", path, err)
		return
	}
	if version < 1 {
		err = fmt.Errorf("invalid schema path: '%s' version must be positive", path)
		return
	}
	id = strings.TrimSuffix(at[1], ".json")
	if !validID(id) {
		err = fmt.Errorf("%w: invalid schema path: '%s'", registry.ErrInvalidSchemaID, path)
		return
	}
	return
}

// maxVersionAmong returns the highest numeric schema version found in matches
// (paths of the form "<namespace>/<version>@<id>.json"). Used by Persist and
// Walk so "latest" is never inferred from lexicographic filename order —
// "9@…" sorts after "12@…" and would otherwise win incorrectly.
func maxVersionAmong(matches []string) (int64, error) {
	var max int64
	for _, path := range matches {
		v, _, _, err := parseSchemaPath(path)
		if err != nil {
			return 0, err
		}
		if v > max {
			max = v
		}
	}
	return max, nil
}

// Persist implements registry.Persister.
func (p *Adapter) Persist(ctx context.Context, schema *avro.RecordSchema, opts ...func(*registry.PersistConfig)) (string, error) {
	if _, err := os.Stat(p.dir); err != nil {
		return "", fmt.Errorf("%w: %w", registry.ErrCreateOrUpdateSchemaFailed, err)
	}

	namespace := schema.Namespace()
	dir := filepath.Join(p.dir, namespace)

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", fmt.Errorf("%w: %w", registry.ErrCreateOrUpdateSchemaFailed, err)
	}

	cfg := &registry.PersistConfig{
		Resolver: func(schema *avro.RecordSchema) (id string, version int, err error) {
			var fingerprint []byte
			fingerprint, err = schema.FingerprintUsing(avro.CRC64Avro)
			if err != nil {
				return
			}
			id = hex.EncodeToString(fingerprint)
			version = 1

			var matches []string
			matches, err = fs.Glob(p.fs, schema.Namespace()+"/*@*.json")
			if err != nil {
				return
			}
			if len(matches) > 0 {
				var prev int64
				prev, err = maxVersionAmong(matches)
				if err != nil {
					return
				}
				version = int(prev) + 1
			}

			return
		},
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(cfg)
	}

	id, version, err := cfg.Resolver(schema)
	if err != nil {
		return "", err
	}

	if !validID(id) {
		return "", fmt.Errorf("%w: resolver returned an invalid ID", registry.ErrInvalidSchemaID)
	}
	if version < 1 {
		return "", fmt.Errorf("%w: resolver returned version %d", registry.ErrCreateOrUpdateSchemaFailed, version)
	}

	b, err := schema.MarshalJSON()
	if err != nil {
		return "", err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, b, "", "\t"); err != nil {
		return "", err
	}

	name := strconv.Itoa(version) + "@" + id + ".json"
	taken, err := fs.Glob(os.DirFS(dir), strconv.Itoa(version)+"@*.json")
	if err != nil {
		return "", err
	}
	for _, other := range taken {
		if other != name {
			return "", fmt.Errorf("%w: %s/%d is already stored as %s", registry.ErrCreateOrUpdateSchemaFailed, namespace, version, other)
		}
	}

	path := filepath.Clean(filepath.Join(dir, name))
	if err := writeNewFile(path, pretty.Bytes()); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return "", fmt.Errorf("%w: %w", registry.ErrCreateOrUpdateSchemaFailed, err)
		}
		// The same version and ID is stored already. Persisting it again is a
		// no-op when the schema is the same, and an error otherwise.
		same, err := sameSchemaFile(path, b)
		if err != nil {
			return "", fmt.Errorf("%w: %w", registry.ErrCreateOrUpdateSchemaFailed, err)
		}
		if !same {
			return "", fmt.Errorf("%w: %s holds a different schema", registry.ErrCreateOrUpdateSchemaFailed, name)
		}
	}

	return id, nil
}

var _ registry.Persister = &Adapter{}

// sameSchemaFile reports whether the schema stored at path has the same
// canonical form and properties as the JSON schema b.
func sameSchemaFile(path string, b []byte) (bool, error) {
	stored, err := os.ReadFile(path) // #nosec G304 -- path is built from a validated version and ID under the adapter dir
	if err != nil {
		return false, err
	}
	var x, y any
	if err := json.Unmarshal(stored, &x); err != nil {
		return false, fmt.Errorf("%w: %s: %w", registry.ErrInvalidSchema, filepath.Base(path), err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		return false, err
	}
	xb, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return bytes.Equal(xb, yb), nil
}

// writeNewFile writes b to a file that must not exist yet. A version is written
// once: an existing file means another writer took that version, and is never
// overwritten. On a failed write the partial file is removed.
func writeNewFile(path string, b []byte) (err error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644) // #nosec G302 G304 -- schemas are committed source, not secrets; path is built from a validated version and ID under the adapter dir
	if err != nil {
		return err
	}
	defer func() {
		if cerr := file.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			_ = os.Remove(path)
		}
	}()
	_, err = file.Write(b)
	return err
}

// Get implements registry.Fetcher.
func (f *Adapter) Get(ctx context.Context, id string) (string, error) {
	if !validID(id) {
		return "", registry.ErrInvalidSchemaID
	}
	matches, err := fs.Glob(f.fs, "*/*@"+id+".json")
	if err != nil {
		return "", err
	}

	if len(matches) == 0 {
		return "", fmt.Errorf("%w: %s", registry.ErrSchemaNotFound, id)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("%w: schema %s is stored more than once", registry.ErrInvalidSchema, id)
	}

	file, err := f.fs.Open(matches[0])
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()

	b, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// GetByDefinition implements registry.Fetcher.
func (f *Adapter) GetByDefinition(ctx context.Context, schema avro.Schema) (string, error) {
	matches, err := fs.Glob(f.fs, "*/*@*.json")
	if err != nil {
		return "", err
	}

	if len(matches) == 0 {
		return "", registry.ErrSchemaNotFound
	}

	for _, path := range matches {
		id, sch, err := f.readSchema(path)
		if err != nil {
			return "", err
		}
		if id == "" {
			continue
		}
		if sch.Fingerprint() == schema.Fingerprint() {
			return id, nil
		}
	}

	return "", registry.ErrSchemaNotFound
}

func (f *Adapter) readSchema(path string) (id string, schema avro.Schema, err error) {
	file, err := f.fs.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer func() { _ = file.Close() }()

	b, err := io.ReadAll(file)
	if err != nil {
		return "", nil, err
	}
	schema, err = avro.ParseBytes(b)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %s: %w", registry.ErrInvalidSchema, path, err)
	}
	_, _, id, err = parseSchemaPath(path)
	return id, schema, err
}

// Walk implements registry.Walker.
func (f *Adapter) Walk(ctx context.Context, fn func(id string, version int64, latest bool, schema *avro.RecordSchema) error, opts ...func(*registry.WalkConfig)) (int, error) {
	matches, err := fs.Glob(f.fs, "*/*@*.json")
	if err != nil {
		return 0, err
	}

	cfg := &registry.WalkConfig{
		Namespaces: make([]string, 0),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(cfg)
	}

	foundNamespaces := len(cfg.Namespaces) > 0
	filter := func(n string) bool {
		if !foundNamespaces {
			return true
		}
		for _, nn := range cfg.Namespaces {
			if nn == n {
				return true
			}
		}
		return false
	}
	// First pass: parse every path and find the numeric max version per namespace,
	// so "latest" is correct even when filenames sort as 10@ < 11@ < 12@ < 9@.
	type entry struct {
		path      string
		version   int64
		namespace string
		id        string
	}
	maxByNS := make(map[string]int64)
	entries := make([]entry, 0, len(matches))
	for _, path := range matches {
		version, namespace, id, err := parseSchemaPath(path)
		if err != nil {
			return 0, err
		}
		if !filter(namespace) {
			continue
		}
		entries = append(entries, entry{path: path, version: version, namespace: namespace, id: id})
		if version > maxByNS[namespace] {
			maxByNS[namespace] = version
		}
	}
	// Visit versions in ascending numeric order within each namespace.
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].namespace != entries[j].namespace {
			return entries[i].namespace < entries[j].namespace
		}
		return entries[i].version < entries[j].version
	})

	n := 0
	for _, e := range entries {
		_, sch, err := f.readSchema(e.path)
		if err != nil {
			return 0, err
		}

		rec, ok := sch.(*avro.RecordSchema)
		if !ok {
			return 0, fmt.Errorf("%w: %s is not a record", registry.ErrInvalidSchema, e.path)
		}
		latest := e.version == maxByNS[e.namespace]
		if err := fn(e.id, e.version, latest, rec); err != nil {
			return 0, err
		}

		n++
	}

	return n, nil
}

var _ registry.Walker = &Adapter{}

// WireFormatter frames a fixed-width schema ID as a prefix of the encoded payload.
type WireFormatter struct{}

// AppendSchemaID implements registry.WireFormatter.
func (*WireFormatter) AppendSchemaID(data []byte, id string) ([]byte, error) {
	if !validID(id) {
		return nil, fmt.Errorf("%w: expected %d lowercase hex characters", registry.ErrInvalidSchemaID, schemaIDLen)
	}
	bid := []byte(id)

	out := make([]byte, 0, schemaIDLen+len(data))
	out = append(out, bid...)
	out = append(out, data...)
	return out, nil
}

// ExtractSchemaID implements registry.WireFormatter.
func (*WireFormatter) ExtractSchemaID(data []byte) (string, []byte, error) {
	if len(data) < schemaIDLen {
		return "", nil, fmt.Errorf("%w: data too short", registry.ErrInvalidDataWireFormat)
	}

	id := string(data[0:schemaIDLen])
	if !validID(id) {
		return "", nil, fmt.Errorf("%w: invalid schema ID prefix", registry.ErrInvalidDataWireFormat)
	}
	return id, data[schemaIDLen:], nil
}

func NewWireFormatter() *WireFormatter {
	return &WireFormatter{}
}

var _ registry.WireFormatter = &WireFormatter{}
