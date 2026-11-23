// Package policy defines versioned authorization policy snapshots.
package policy

import (
	"encoding/json"
	"sort"
)

// Snapshot is a versioned authorization policy: which fields carry sensitive
// marks, which target fields are inside the authorized boundary, and which
// marks remain sensitive after aggregation.
type Snapshot struct {
	Version           int                 `json:"version"`
	SensitiveFields   map[string][]string `json:"sensitive_fields"`     // field -> marks
	AuthorizedTargets map[string]bool     `json:"authorized_targets"`   // field -> inside boundary
	AggregateSafe     map[string]bool     `json:"aggregate_safe_marks"` // mark -> de-identified by aggregation
}

// MarksOf returns the declared marks of a sensitive source field.
func (s *Snapshot) MarksOf(field string) []string {
	return s.SensitiveFields[field]
}

// Authorized reports whether a target field is inside the boundary.
func (s *Snapshot) Authorized(field string) bool {
	return s.AuthorizedTargets[field]
}

// ParseSnapshot decodes a JSON policy snapshot.
func ParseSnapshot(data []byte) (*Snapshot, error) {
	var s Snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	if s.SensitiveFields == nil {
		s.SensitiveFields = map[string][]string{}
	}
	if s.AuthorizedTargets == nil {
		s.AuthorizedTargets = map[string]bool{}
	}
	if s.AggregateSafe == nil {
		s.AggregateSafe = map[string]bool{}
	}
	return &s, nil
}

// Marshal renders the snapshot deterministically (sorted keys).
func (s *Snapshot) Marshal() []byte {
	keys := make([]string, 0, len(s.SensitiveFields))
	for k := range s.SensitiveFields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	m := make(map[string][]string, len(keys))
	for _, k := range keys {
		marks := append([]string{}, s.SensitiveFields[k]...)
		sort.Strings(marks)
		m[k] = marks
	}
	out, _ := json.Marshal(struct {
		Version           int                 `json:"version"`
		SensitiveFields   map[string][]string `json:"sensitive_fields"`
		AuthorizedTargets map[string]bool     `json:"authorized_targets"`
		AggregateSafe     map[string]bool     `json:"aggregate_safe_marks"`
	}{s.Version, m, s.AuthorizedTargets, s.AggregateSafe})
	return out
}
