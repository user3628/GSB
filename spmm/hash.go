package spmm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// CanonicalJSON marshals v without HTML escaping. All persisted JSON uses this
// helper so fingerprints do not depend on encoding/json's incidental choices.
func CanonicalJSON(v any) ([]byte, error) {
	return marshalCanonical(v)
}

func marshalCanonical(v any) ([]byte, error) {
	// json.Marshal sorts struct fields in declaration order and map keys
	// lexicographically, which is the canonical form used by this project.
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// HashJSON returns a hexadecimal SHA-256 of canonical JSON.
func HashJSON(v any) string {
	b, err := marshalCanonical(v)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func HashBytes(parts ...[]byte) string {
	h := sha256.New()
	for _, part := range parts {
		var length [8]byte
		n := uint64(len(part))
		for i := 0; i < 8; i++ {
			length[7-i] = byte(n >> (8 * i))
		}
		_, _ = h.Write(length[:])
		_, _ = h.Write(part)
	}
	return hex.EncodeToString(h.Sum(nil))
}
