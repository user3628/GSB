package arbiter

import "encoding/json"

func recyclePayload(floor int64, keepCount int, retire []int64) string {
	b, _ := json.Marshal(map[string]any{
		"protected_snapshot":  floor,
		"min_keep_versions":   keepCount,
		"retire_row_versions": retire,
	})
	return string(b)
}

// hashPayload is the deterministic SHA-256 over the canonical request body
// used for idempotency conflict detection.
func hashPayload(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return contentFingerprint(string(b), false)
}
