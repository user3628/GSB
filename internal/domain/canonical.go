package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// CanonicalJSON consistently marshals values for hashing and persistence.
// encoding/json sorts map keys and emits struct fields in declaration order.
func CanonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func CanonicalString(v any) (string, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func Digest(v any) (string, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	return DigestBytes(b), nil
}

func DigestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func PrettyJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func CloneRange(r ValueRange) ValueRange {
	out := r
	out.Enum = append([]string(nil), r.Enum...)
	if r.Min != nil {
		v := *r.Min
		out.Min = &v
	}
	if r.Max != nil {
		v := *r.Max
		out.Max = &v
	}
	return out
}

func Int64Ptr(v int64) *int64 {
	return &v
}

func IntPtr(v int) *int {
	return &v
}
