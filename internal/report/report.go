// Package report defines the deterministic JSON verification report.
package report

import (
	"encoding/json"

	"flowtrace/internal/adjudicate"
	"flowtrace/internal/latency"
	"flowtrace/internal/model"
)

// ParseFailure ties a parse error to its source window.
type ParseFailure struct {
	WindowID string `json:"window_id"`
	Code     string `json:"code"`
	Detail   string `json:"detail"`
}

// WindowReport is the per-window verification record.
type WindowReport struct {
	WindowID         string                `json:"window_id"`
	SessionID        string                `json:"session_id"`
	Code             string                `json:"code"`
	Incomplete       bool                  `json:"incomplete"`
	Events           []model.Event         `json:"events,omitempty"`
	Judgments        []model.Judgment      `json:"judgments,omitempty"`
	Corrections      []model.Correction    `json:"corrections,omitempty"`
	Consistent       bool                  `json:"consistent"`
	Mismatches       []adjudicate.Mismatch `json:"mismatches,omitempty"`
	SnapshotVersions []int                 `json:"snapshot_versions,omitempty"`
}

// ChainReport describes one window's snapshot version chain.
type ChainReport struct {
	WindowID      string `json:"window_id"`
	Versions      []int  `json:"versions"`
	PolicyVersion int    `json:"policy_version,omitempty"`
	Error         string `json:"error,omitempty"`
	ValidPrefix   int    `json:"valid_prefix,omitempty"`
}

// Report is the full verification output. All slices are pre-sorted by the
// builder so the marshaled bytes are identical across identical runs.
type Report struct {
	Windows           []WindowReport `json:"windows"`
	Latency           []latency.Stat `json:"latency"`
	Chains            []ChainReport  `json:"chains"`
	ParseFailures     []ParseFailure `json:"parse_failures,omitempty"`
	OverallConsistent bool           `json:"overall_consistent"`
}

// Marshal renders the report deterministically (stable field order, sorted
// slices, trailing newline).
func Marshal(r *Report) []byte {
	raw, _ := json.MarshalIndent(r, "", "  ")
	return append(raw, '\n')
}
