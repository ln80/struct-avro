package fs

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
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
	id = strings.TrimSpace(strings.TrimSuffix(at[1], ".json"))
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
	if _, err := os.Stat(p.dir); os.IsNotExist(err) {
		return "", err
	}

	namespace := schema.Namespace()
	dir := filepath.Join(p.dir, namespace)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return "", err
		}
	}

	cfg := &registry.PersistConfig{
		Resolver: func(schema *avro.RecordSchema) (id string, version int, err error) {
			fingerprint, _ := schema.FingerprintUsing(avro.CRC64Avro)
			id = hex.EncodeToString(fingerprint[:])
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

	path := filepath.Clean(filepath.Join(dir, strconv.Itoa(version)+"@"+id+".json"))
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	b, err := schema.MarshalJSON()
	if err != nil {
		return "", err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, b, "", "\t"); err != nil {
		return "", err
	}

	if _, err := file.Write(pretty.Bytes()); err != nil {
		return "", err
	}

	return id, nil
}

var _ registry.Persister = &Adapter{}

// Get implements registry.Fetcher.
func (f *Adapter) Get(ctx context.Context, id string) (string, error) {
	matches, err := fs.Glob(f.fs, "*/*@"+id+".json")
	if err != nil {
		return "", err
	}

	if len(matches) == 0 {
		return "", fmt.Errorf("%w: %s", registry.ErrSchemaNotFound, id)
	}

	file, err := f.fs.Open(matches[0])
	if err != nil {
		return "", err
	}
	defer file.Close()

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
	defer file.Close()

	b, err := io.ReadAll(file)
	if err != nil {
		return "", nil, err
	}
	schema, err = avro.ParseBytes(b)
	if err != nil {
		return "", nil, err
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

		latest := e.version == maxByNS[e.namespace]
		if err := fn(e.id, e.version, latest, sch.(*avro.RecordSchema)); err != nil {
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
	bid := []byte(id)
	if len(bid) != schemaIDLen {
		return nil, fmt.Errorf("%w: expected length %d, got %d", registry.ErrInvalidSchemaID, schemaIDLen, len(bid))
	}

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

	return string(data[0:schemaIDLen]), data[schemaIDLen:], nil
}

func NewWireFormatter() *WireFormatter {
	return &WireFormatter{}
}

var _ registry.WireFormatter = &WireFormatter{}
