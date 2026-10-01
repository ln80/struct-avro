package avro

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

var (
	timeType   = reflect.TypeFor[time.Time]()
	stringType = reflect.TypeFor[string]()
)

// DefaultOf derives the AVRO default value for a Go value, recursively. It mirrors
// the type mapping used by SchemaOf (e.g. time.Time -> unix millis, integers -> int/long).
func DefaultOf(v any) (any, error) {
	return defaultOf(v)
}

func defaultOf(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	rv := reflect.ValueOf(v)
	rt := reflect.TypeOf(v)

	if rt.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, nil
		}
		// a non-nil pointer is dereferenced so a sample like &Nested{...} can seed
		// nested defaults.
		return defaultOf(rv.Elem().Interface())
	}

	switch rt.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16:
		return rv.Convert(reflect.TypeFor[int]()).Interface(), nil

	case reflect.Int64, reflect.Uint32:
		return rv.Convert(reflect.TypeFor[int64]()).Interface(), nil
	}
	if rt.ConvertibleTo(stringType) {
		return rv.Convert(stringType).String(), nil
	}

	if rt.ConvertibleTo(timeType) {
		return rv.Convert(timeType).Interface().(time.Time).UnixMilli(), nil
	}

	if rt.Kind() == reflect.Struct {
		result := make(map[string]any)
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			if !f.IsExported() {
				continue
			}

			if f.Anonymous {
				fv := rv.Field(i)
				if fv.Type().Kind() == reflect.Pointer {
					fv = fv.Elem()
				}
				if fv.Type().Kind() != reflect.Struct {
					return nil, fmt.Errorf("unsupported anonymous field '%v'", fv.Type())
				}
				nested, err := defaultOf(fv.Interface())
				if err != nil {
					return nil, err
				}
				for k, v := range nested.(map[string]any) {
					result[k] = v
				}
				continue
			}

			r, err := defaultOf(rv.Field(i).Interface())
			if err != nil {
				return nil, err
			}
			result[f.Name] = r
		}

		return result, nil
	}

	if rt.Kind() == reflect.Map {
		result := make(map[string]any)
		iter := rv.MapRange()
		for iter.Next() {
			k := iter.Key()
			v := iter.Value()
			result[k.String()] = v.Interface()
		}
		return result, nil
	}

	if rt.Kind() == reflect.Slice {
		result := make([]any, 0)
		if rv.IsNil() {
			return result, nil
		}
		for i := 0; i < rv.Len(); i++ {
			r, err := defaultOf(rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			result = append(result, r)
		}
		return result, nil
	}

	return v, nil
}

// fieldDefault extracts a named field's default from a parent struct's default map.
//
// The AVRO library may mutate default values in place; the JSON round-trip returns a
// fresh copy that is safe to reuse as an initial value.
func fieldDefault(parentDef any, name string) (any, bool, error) {
	def, ok := parentDef.(map[string]any)
	if !ok {
		return nil, false, nil
	}
	d, ok := def[name]
	if !ok {
		return nil, false, nil
	}
	if d == nil {
		return d, true, nil
	}

	switch d.(type) {
	case []any:
		fDef := []any{}
		b, err := json.Marshal(d)
		if err != nil {
			return nil, false, err
		}
		if err := json.Unmarshal(b, &fDef); err != nil {
			return nil, false, err
		}
		return fDef, true, nil
	case map[string]any:
		fDef := map[string]any{}
		b, err := json.Marshal(d)
		if err != nil {
			return nil, false, err
		}
		if err := json.Unmarshal(b, &fDef); err != nil {
			return nil, false, err
		}
		return fDef, true, nil

	default:
		return d, true, nil
	}
}
