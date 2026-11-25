package canonical

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Marshal returns a deterministic compact JSON representation for the value.
// Struct field order is part of the contract; maps are sorted by encoding/json.
func Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func Digest(v any) (string, error) {
	b, err := Marshal(v)
	if err != nil {
		return "", err
	}
	return DigestBytes(b), nil
}

func DigestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func ShortDigest(v any) (string, error) {
	d, err := Digest(v)
	if err != nil {
		return "", err
	}
	if len(d) > 16 {
		d = d[:16]
	}
	return d, nil
}
