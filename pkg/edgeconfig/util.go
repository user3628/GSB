package edgeconfig

import (
	"crypto/rand"
	"encoding/hex"
)

func newRandomID(prefix string) string {
	buf := make([]byte, 8)
	_, _ = rand.Read(buf)
	return prefix + "_" + hex.EncodeToString(buf)
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
