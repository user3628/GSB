package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

const (
	// Explicit source priorities. Self subscriptions are declared first, but
	// effective order number still takes precedence before priority.
	sourcePrioritySelf     = 100
	sourcePriorityUpstream = 50
)

// viewDigest is a deterministic content digest of a component view.
func viewDigest(v *ComponentView) string {
	type plain struct {
		ComponentID string      `json:"component_id"`
		Entries     []ViewEntry `json:"entries"`
		Conflicts   []Conflict  `json:"conflicts"`
	}
	raw, err := json.Marshal(plain{v.ComponentID, v.Entries, v.Conflicts})
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
