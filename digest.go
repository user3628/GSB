package speakerdiar

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// CanonicalJSON encodes values with struct field order and no HTML-sensitive spacing changes.
// All persisted and compared artifacts must pass through this function.
func CanonicalJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// PrettyJSON is used for human-facing files while retaining deterministic struct order.
func PrettyJSON(v any) ([]byte, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func mustCanonical(v any) []byte {
	b, err := CanonicalJSON(v)
	if err != nil {
		panic(err)
	}
	return b
}

// DigestBytes returns the hex SHA-256 digest of raw bytes.
func DigestBytes(p []byte) string {
	sum := sha256.Sum256(p)
	return hex.EncodeToString(sum[:])
}

// DigestValue returns a digest over canonical JSON.
func DigestValue(v any) (string, []byte, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return "", nil, err
	}
	return DigestBytes(b), b, nil
}

func mustDigestValue(v any) string {
	d, _, err := DigestValue(v)
	if err != nil {
		panic(err)
	}
	return d
}

func combineDigest(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
