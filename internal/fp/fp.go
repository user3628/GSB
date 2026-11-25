// Package fp provides deterministic content fingerprints (SHA-256 over
// domain-separated, canonically serialized inputs) used as stage version
// fingerprints throughout the material pipeline.
package fp

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// Hash accumulates fingerprint bytes with an explicit domain scope so the same
// payload used by different stages never collides.
type Hash struct {
	buf bytes.Buffer
}

// New starts a fingerprint for the named scope (e.g. "stage:control").
func New(scope string) *Hash {
	h := &Hash{}
	h.AddString("scope")
	h.AddString(scope)
	return h
}

// AddString writes a length-prefixed string to the hash input.
func (h *Hash) AddString(s string) *Hash {
	var lb [8]byte
	n := len(s)
	for i := 0; i < 8; i++ {
		lb[i] = byte(n >> (8 * i))
	}
	h.buf.Write(lb[:])
	h.buf.WriteString(s)
	return h
}

// AddInt writes a varint-style fixed width integer.
func (h *Hash) AddInt(i int64) *Hash {
	var b [8]byte
	u := uint64(i)
	for k := 0; k < 8; k++ {
		b[k] = byte(u >> (8 * k))
	}
	h.buf.Write(b[:])
	return h
}

// AddBytes writes a length-prefixed byte slice.
func (h *Hash) AddBytes(b []byte) *Hash {
	h.AddInt(int64(len(b)))
	h.buf.Write(b)
	return h
}

// Sum returns the hex-encoded SHA-256 digest accumulated so far.
func (h *Hash) Sum() string {
	sum := sha256.Sum256(h.buf.Bytes())
	return hex.EncodeToString(sum[:])
}

// CanonicalJSON serializes v into deterministic compact JSON: object keys are
// sorted recursively and no insignificant whitespace is emitted.
func CanonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var decoded any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&decoded); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := writeCanonical(&out, decoded); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func writeCanonical(buf *bytes.Buffer, v any) error {
	switch t := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if t {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case string:
		b, err := json.Marshal(t)
		if err != nil {
			return err
		}
		buf.Write(b)
	case json.Number:
		buf.WriteString(t.String())
	case float64:
		buf.WriteString(fmt.Sprintf("%.17g", t))
	case []any:
		buf.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, e); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			kb, err := json.Marshal(k)
			if err != nil {
				return err
			}
			buf.Write(kb)
			buf.WriteByte(':')
			if err := writeCanonical(buf, t[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("fp: unsupported canonical type %T", v)
	}
	return nil
}

// Of returns the domain-scoped fingerprint of the canonical JSON form of v.
func Of(scope string, v any) (string, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	return New(scope).AddBytes(b).Sum(), nil
}
