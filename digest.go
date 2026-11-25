package feedbackstream

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func CanonicalJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}

func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func DigestValue(value any) (string, error) {
	data, err := CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	return SHA256Hex(data), nil
}
