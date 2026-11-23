package report

import "encoding/json"

// Decode parses a report previously produced by Marshal.
func Decode(raw []byte) (*Report, error) {
	var r Report
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	return &r, nil
}
