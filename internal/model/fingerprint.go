package model

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
)

// CanonicalJSON deterministically serializes any value using sorted object keys.
// Non-finite floats (NaN/Inf) are mapped to JSON null so fingerprinting never
// fails; the quantization stage independently rejects such values with
// E_QUANT_PARAM. All fingerprinting goes through this function so the same
// logical content always yields byte-identical digests across processes.
func CanonicalJSON(v any) ([]byte, error) {
	return json.Marshal(normalize(toGeneric(reflect.ValueOf(v))))
}

// Digest returns the hex SHA-256 of the canonical JSON encoding of v.
func Digest(v any) (string, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func normalize(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = normalize(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i := range t {
			out[i] = normalize(t[i])
		}
		return out
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return nil
		}
		return t
	default:
		return v
	}
}

// toGeneric converts arbitrary typed values (including structs with NaN float
// fields) into generic map/slice/scalar values using reflection, replacing
// non-finite floats with nil so the result is always JSON-safe.
func toGeneric(rv reflect.Value) any {
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Struct:
		rt := rv.Type()
		out := map[string]any{}
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			if f.PkgPath != "" || !rv.Field(i).CanInterface() {
				continue
			}
			name := jsonName(f)
			out[name] = toGeneric(rv.Field(i))
		}
		return out
	case reflect.Slice, reflect.Array:
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = toGeneric(rv.Index(i))
		}
		return out
	case reflect.Map:
		out := map[string]any{}
		iter := rv.MapRange()
		for iter.Next() {
			out[mapKey(iter.Key())] = toGeneric(iter.Value())
		}
		return out
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return f
	case reflect.Bool:
		return rv.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint()
	case reflect.String:
		return rv.String()
	default:
		if !rv.IsValid() || !rv.CanInterface() {
			return nil
		}
		return rv.Interface()
	}
}

func jsonName(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	if tag == "" || tag == "-" {
		return f.Name
	}
	name := tag
	for i := 0; i < len(tag); i++ {
		if tag[i] == ',' {
			name = tag[:i]
			break
		}
	}
	if name == "" {
		return f.Name
	}
	return name
}

// DigestBytes returns the hex SHA-256 of raw bytes (used for feature blobs).
func DigestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// FeatureBlob is the deterministic byte layout of a flattened feature matrix:
// little-endian float64 values laid out sample-major, preceded by dimensions.
func FeatureBlob(features [][]float64) []byte {
	n := 0
	if len(features) > 0 {
		n = len(features[0])
	}
	buf := make([]byte, 16+len(features)*n*8)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(len(features)))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(n))
	off := 16
	for _, row := range features {
		for _, x := range row {
			binary.LittleEndian.PutUint64(buf[off:off+8], math.Float64bits(x))
			off += 8
		}
	}
	return buf
}

// mapKey renders a reflect map key as a stable string regardless of key kind.
func mapKey(kv reflect.Value) string {
	switch kv.Kind() {
	case reflect.String:
		return kv.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(kv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(kv.Uint(), 10)
	case reflect.Bool:
		return strconv.FormatBool(kv.Bool())
	default:
		return fmt.Sprintf("%v", kv.Interface())
	}
}
