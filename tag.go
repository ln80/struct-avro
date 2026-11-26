package structavro

import (
	"reflect"
	"slices"
	"strings"
)

// DefaultTagKey is the default struct-tag key used to carry schema options
// (field name, aliases, inject, ...). It mirrors the event-store "ev" tag so
// that structs annotated for event-store keep working unchanged.
const DefaultTagKey = "ev"

// tagOptions is a map of a field's tag options. Each option may hold multiple values.
type tagOptions map[string][]string

// parseTag returns the field name override and the option map encoded in the
// struct tag under key. Format: `key:"name,opt1=a b,opt2"`.
//
// This is a self-contained copy of event-store's event.ParseTag (with a
// configurable key) so the library does not depend on event-store.
func parseTag(key string, tag reflect.StructTag) (name string, opts tagOptions) {
	opts = make(tagOptions)
	tagStr := tag.Get(key)
	if tagStr == "" {
		return
	}
	raw := strings.Split(tagStr, ",")
	name = strings.TrimSpace(raw[0])
	for _, o := range raw[1:] {
		splits := strings.Split(o, "=")

		optName := strings.TrimSpace(splits[0])
		if optName == "" {
			continue
		}
		optValues := []string{}
		if len(splits) > 1 {
			for _, vv := range strings.Split(strings.TrimSpace(splits[1]), " ") {
				vv := strings.TrimSpace(vv)
				if vv == "" {
					continue
				}
				optValues = append(optValues, vv)
			}
		}
		opts[optName] = optValues
	}

	return
}

// unique returns the slice with duplicates removed, preserving first-seen order.
func unique[T comparable](slice []T) []T {
	seen := make(map[T]struct{})
	return slices.DeleteFunc(slice, func(elem T) bool {
		if _, exists := seen[elem]; exists {
			return true
		}
		seen[elem] = struct{}{}
		return false
	})
}
