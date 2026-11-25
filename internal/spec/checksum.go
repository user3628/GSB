package spec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// Checksum returns a deterministic SHA-256 digest over the canonical encoding
// of the given items and components. Ordering of the input slices does not
// affect the digest.
func Checksum(items []ConfigItem, components []Component) string {
	canonicalItems := make([]ConfigItem, len(items))
	copy(canonicalItems, items)
	sort.Slice(canonicalItems, func(i, j int) bool {
		return canonicalItems[i].KeyPath < canonicalItems[j].KeyPath
	})
	canonicalComponents := make([]Component, len(components))
	copy(canonicalComponents, components)
	for i := range canonicalComponents {
		canonicalComponents[i].SubPrefixes = sortedCopy(canonicalComponents[i].SubPrefixes)
		canonicalComponents[i].DependsOn = sortedCopy(canonicalComponents[i].DependsOn)
	}
	sort.Slice(canonicalComponents, func(i, j int) bool {
		return canonicalComponents[i].ID < canonicalComponents[j].ID
	})
	payload := struct {
		Items      []ConfigItem `json:"items"`
		Components []Component  `json:"components"`
	}{canonicalItems, canonicalComponents}
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
