package tm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func CanonicalJSON(v any) ([]byte, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func DigestCanonical(v any) (string, []byte, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), b, nil
}

func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func HashString(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}
