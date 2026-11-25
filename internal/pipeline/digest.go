package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// CanonicalEncode renders a value into deterministic JSON independent of map
// iteration order and of the host float formatting. Floats use the shortest
// round-trippable base-10 representation, which is identical on every platform
// that implements IEEE-754 binary64 (all Go targets). NaN/Inf are rejected so
// a fingerprint can never silently collapse.
func CanonicalEncode(v any) ([]byte, error) {
	var b strings.Builder
	if err := encodeCanonical(&b, reflect.ValueOf(v)); err != nil {
		return nil, err
	}
	return []byte(b.String()), nil
}

func encodeCanonical(b *strings.Builder, rv reflect.Value) error {
	for rv.Kind() == reflect.Interface || rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			b.WriteString("null")
			return nil
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Struct:
		t := rv.Type()
		b.WriteByte('{')
		first := true
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			name := f.Name
			if tag := f.Tag.Get("json"); tag != "" {
				parts := strings.Split(tag, ",")
				if parts[0] == "-" {
					continue
				}
				if parts[0] != "" {
					name = parts[0]
				}
			}
			if !first {
				b.WriteByte(',')
			}
			first = false
			key, _ := json.Marshal(name)
			b.Write(key)
			b.WriteByte(':')
			if err := encodeCanonical(b, rv.Field(i)); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	case reflect.Map:
		keys := make([]string, 0, rv.Len())
		byKey := map[string]reflect.Value{}
		for _, k := range rv.MapKeys() {
			kb, err := CanonicalEncode(k.Interface())
			if err != nil {
				return err
			}
			ks := string(kb)
			keys = append(keys, ks)
			byKey[ks] = rv.MapIndex(k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, ks := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(ks)
			b.WriteByte(':')
			if err := encodeCanonical(b, byKey[ks]); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	case reflect.Slice, reflect.Array:
		b.WriteByte('[')
		for i := 0; i < rv.Len(); i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := encodeCanonical(b, rv.Index(i)); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case reflect.String:
		kb, _ := json.Marshal(rv.String())
		b.Write(kb)
	case reflect.Bool:
		if rv.Bool() {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.WriteString(strconv.FormatInt(rv.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		b.WriteString(strconv.FormatUint(rv.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return bizError(CodeNaNInput, "non-finite float encountered while computing fingerprint")
		}
		b.WriteString(strconv.FormatFloat(f, 'g', -1, 64))
	default:
		return fmt.Errorf("canonical encode: unsupported kind %s", rv.Kind())
	}
	return nil
}

// Digest returns the hex SHA-256 of the canonical encoding.
func Digest(v any) (string, error) {
	b, err := CanonicalEncode(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// MustDigest panics only on programmer error (non-finite data must be caught
// earlier in the pipeline and surfaced as a business error).
func MustDigest(v any) string {
	d, err := Digest(v)
	if err != nil {
		panic(err)
	}
	return d
}

// ConfigVersion is the fingerprint of every reconstruction-affecting config
// field. It participates in every cache key and checkpoint record.
func ConfigVersion(c PipelineConfig) string {
	return MustDigest(c)
}
