package fs

import (
	"testing"

	"github.com/ln80/struct-avro/registry"
)

// FuzzExtractSchemaID checks that framed payloads from storage never panic the
// parser and that an accepted ID frames back to the same bytes.
func FuzzExtractSchemaID(f *testing.F) {
	f.Add([]byte("0123456789abcdefpayload"))
	f.Add([]byte("****************"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		wf := NewWireFormatter()
		id, payload, err := wf.ExtractSchemaID(data)
		if err != nil {
			return
		}
		if !validID(id) {
			t.Fatalf("accepted an invalid ID")
		}
		again, err := wf.AppendSchemaID(payload, id)
		if err != nil || string(again) != string(data) {
			t.Fatalf("round trip failed: %v", err)
		}
	})
}

// FuzzParseSchemaPath checks the parser of stored file names.
func FuzzParseSchemaPath(f *testing.F) {
	for _, s := range []string{"ns/1@0123456789abcdef.json", "ns/0001@0123456789abcdef.json", "ns/+1@0123456789abcdef.json", "a/b/12@ffffffffffffffff.json", "ns/0@x.json", "@", "ns/-1@0123456789abcdef.json", "ns/1@*.json"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, path string) {
		version, _, id, err := parseSchemaPath(path)
		if err != nil {
			return
		}
		if version < 1 || !validID(id) {
			t.Fatalf("accepted version %d, id valid=%v", version, validID(id))
		}
	})
}

var _ registry.WireFormatter = NewWireFormatter()
