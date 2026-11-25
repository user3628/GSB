package app

import (
	"crypto/sha256"
	"encoding/hex"
)

func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
