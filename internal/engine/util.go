package engine

import (
	"crypto/rand"
	"encoding/hex"
)

func randomSuffix() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "0000000000000000"
	}
	return hex.EncodeToString(b)
}

func newRequestID() string {
	return "req-" + randomSuffix()
}

func int64Ptr(v int64) *int64 {
	return &v
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
