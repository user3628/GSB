package pipeline

import "encoding/json"

// decodeCanonical decodes JSON produced by CanonicalEncode. Standard JSON
// decoding suffices because canonical output is valid JSON.
func decodeCanonical(b []byte, v any) error {
	if err := json.Unmarshal(b, v); err != nil {
		return bizError(CodeInternal, "decode artifact: %v", err)
	}
	return nil
}
