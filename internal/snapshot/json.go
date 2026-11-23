package snapshot

import "encoding/json"

// jsonUnmarshal is a thin indirection kept for symmetric testing.
func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
