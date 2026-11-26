package structavro

import (
	"reflect"
	"strings"
	"testing"

	avro "github.com/ln80/avro/v2"
)

// FuzzParseTag checks that the ev tag parser never panics and that option names
// and values carry no separators.
func FuzzParseTag(f *testing.F) {
	for _, s := range []string{"", "Name", ",aliases=A B", ",inject=union", "x,a=1,b,=c,d= e  f ", ",,,", "a=b=c"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, tag string) {
		if strings.ContainsAny(tag, "`\"\\") {
			return // not representable inside a struct tag literal
		}
		_, opts := parseTag(DefaultTagKey, reflect.StructTag(`ev:"`+tag+`"`))
		for k, vs := range opts {
			if k == "" || strings.ContainsAny(k, ",=") {
				t.Fatalf("option name %q", k)
			}
			for _, v := range vs {
				if v == "" || strings.ContainsAny(v, ", ") {
					t.Fatalf("option %s value %q", k, v)
				}
			}
		}
	})
}

// FuzzSchemaOfTags builds struct types whose fields carry arbitrary tags and
// checks that SchemaOf either fails or returns a schema that parses back.
func FuzzSchemaOfTags(f *testing.F) {
	for _, s := range []string{
		`pii:"data"`, `pii:"data,kind=email,replace=x"`, `sensitive:"subjectID,prefix=u_"`,
		`pii:"dive"`, `pii:"datum"`, `pii:"data" sens:"data"`, `ev:",aliases=Old"`,
		`avro:"Other"`, `pii:""`, `pii:"data,=x"`,
	} {
		f.Add(s, s)
	}
	f.Fuzz(func(t *testing.T, tag1, tag2 string) {
		typ := reflect.StructOf([]reflect.StructField{
			{Name: "A", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(tag1)},
			{Name: "B", Type: reflect.TypeFor[*struct{ C []int }](), Tag: reflect.StructTag(tag2)},
		})
		sch, err := SchemaOf(typ, WithName("Fuzz"), WithNamespace("fuzz"))
		if err != nil {
			return
		}
		if _, err := avro.Parse(sch.String()); err != nil {
			t.Fatalf("generated schema does not parse: %v", err)
		}
	})
}
