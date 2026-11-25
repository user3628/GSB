// Package report holds the audit result data structures shared between the
// audit engine and the persistence layer to avoid import cycles.
package report

import (
	"configaudit/internal/fixedpoint"
	"configaudit/internal/units"
)

// Contribution is one source channel's normalized contribution to a key.
type Contribution struct {
	Channel      string                  `json:"channel"`
	Generation   string                  `json:"generation"`
	RawText      string                  `json:"raw_text"`
	Record       units.Record            `json:"record"`
	Chain        *fixedpoint.ChainResult `json:"chain"`
	OrderIndex   int                     `json:"order_index"`
	Shadowed     bool                    `json:"shadowed"`
	ShadowReason string                  `json:"shadow_reason,omitempty"`
}

// CombinationResult is the audited result for one node/key/channel tuple.
type CombinationResult struct {
	ItemID       string                  `json:"item_id"`
	NodeID       string                  `json:"node_id"`
	KeyPath      string                  `json:"key_path"`
	Channel      string                  `json:"channel"`
	Normalized   fixedpoint.Value        `json:"normalized"`
	ChainFinal   fixedpoint.Value        `json:"chain_final"`
	ErrorBound   string                  `json:"error_bound"`
	ActualError  string                  `json:"actual_error"`
	Reference    string                  `json:"reference"`
	AbsDeviation string                  `json:"abs_deviation"`
	RelDeviation string                  `json:"rel_deviation"`
	WithinBound  bool                    `json:"within_bound"`
	Steps        []fixedpoint.StepResult `json:"steps"`
}

// KeyResult is the multi-source overlay outcome for one node/key.
type KeyResult struct {
	NodeID          string              `json:"node_id"`
	KeyPath         string              `json:"key_path"`
	WinnerChannel   string              `json:"winner_channel"`
	Contributions   []Contribution      `json:"contributions"`
	OverlayOrder    []string            `json:"overlay_order"`
	OverlayErrorULP int64               `json:"overlay_error_ulp"`
	Combinations    []CombinationResult `json:"combinations"`
	Effective       fixedpoint.Value    `json:"effective"`
	EffectiveBound  string              `json:"effective_bound"`
}

// Report is a full audit traversal: every node x key x channel combination.
type Report struct {
	NodeCount      int          `json:"node_count"`
	KeyCount       int          `json:"key_count"`
	CombinationCnt int          `json:"combination_count"`
	Keys           []*KeyResult `json:"keys"`
}
