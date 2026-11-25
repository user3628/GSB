package sample

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"configaudit/internal/model"
)

// checksumSpec returns a stable sha256 over the canonical JSON of a spec.
func checksumSpec(s model.Spec) string {
	clone := s
	clone.Checksum = ""
	b, err := json.Marshal(clone)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
