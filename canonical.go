package grayrouter

// CanonicalBytes exposes the deterministic JSON encoding used by signatures
// and idempotent hashes. Map keys are sorted by encoding/json.
func CanonicalBytes(value any) []byte { return canonicalJSON(value) }
