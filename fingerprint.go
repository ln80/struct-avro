package avro

import (
	"bytes"
	"crypto/sha3"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"

	"github.com/ln80/avro/v2"
)

// Accumulator collects field defaults and sensitive-field descriptors encountered
// while walking the event records of an envelope. It emits two synthetic root fields
// whose names embed a fingerprint of that metadata, so that any change to defaults or
// sensitive annotations forces a schema change (and thus a new schema version).
//
// IMPORTANT: the byte layout fed to the hash is preserved exactly from the original
// event-store implementation. Altering it would change the generated field names,
// hence the schema bytes and the schema IDs used to decode persisted data.
type Accumulator struct {
	def       []byte
	sensitive []string
}

// NewAccumulator returns an empty accumulator, shared across the SchemaOf calls of one envelope.
func NewAccumulator() *Accumulator { return &Accumulator{} }

func (a *Accumulator) appendDefault(def any) {
	if m, ok := def.(map[string]any); ok {
		arr := make([][2]any, 0, len(m))
		for k, v := range m {
			arr = append(arr, [2]any{k, v})
		}
		slices.SortStableFunc(arr, func(x, y [2]any) int {
			return bytes.Compare([]byte(x[0].(string)), []byte(y[0].(string)))
		})
		def = arr
	}

	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(def)
	a.def = append(a.def, buf.Bytes()...)
}

func (a *Accumulator) appendSensitive(data string) {
	a.sensitive = append(a.sensitive, data)
}

func (a *Accumulator) defaultField() *avro.Field {
	if a.def == nil {
		return nil
	}
	vv := append([]byte(nil), a.def...)
	slices.Sort(vv)

	hash := sha3.Sum256(vv)
	fingerprint := hex.EncodeToString(hash[:])
	f, _ := avro.NewField("defxyz"+fingerprint, avro.NewPrimitiveSchema(avro.String, nil), avro.WithDefault(""))
	return f
}

func (a *Accumulator) sensitiveField() *avro.Field {
	if a.sensitive == nil {
		return nil
	}
	vv := append([]string(nil), a.sensitive...)
	slices.Sort(vv)

	hash := sha3.Sum256([]byte(strings.Join(vv, ".")))
	fingerprint := hex.EncodeToString(hash[:])
	f, _ := avro.NewField("sensitivexyz"+fingerprint, avro.NewPrimitiveSchema(avro.String, nil), avro.WithDefault(""))
	return f
}
