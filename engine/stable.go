package engine

import "encoding/json"

// StableJSON serializes a result without wall-clock-only fields. Two runs that
// reached the same persisted facts therefore produce byte-identical output.
func StableJSON(r *Result) ([]byte, error) {
	copyResult := *r
	copyResult.ElapsedMS = 0
	b, err := json.MarshalIndent(&copyResult, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
