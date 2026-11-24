// Package codes defines the business codes and the structured error type
// used across the adjudication engine.
package codes

import "fmt"

const (
	OK                 = "OK"
	ERuleInvalid       = "E_RULE_INVALID"
	EFixedOverflow     = "E_FIXED_OVERFLOW"
	ERuleDivergence    = "E_RULE_DIVERGENCE"
	ESnapshotVersion   = "E_SNAPSHOT_VERSION"
	ERuleBudget        = "E_RULE_BUDGET"
	ECompareContention = "E_COMPARE_CONTENTION"
)

// Error is a structured business error. RuleIndex and Offset are -1 when
// not applicable.
type Error struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	SnapshotID string `json:"snapshot_id,omitempty"`
	RuleIndex  int    `json:"rule_index"`
	Offset     int64  `json:"offset"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s (snapshot=%q rule=%d offset=%d)",
		e.Code, e.Message, e.SnapshotID, e.RuleIndex, e.Offset)
}

func New(code, msg string) *Error {
	return &Error{Code: code, Message: msg, RuleIndex: -1, Offset: -1}
}

func At(code, msg, snapshotID string, ruleIndex int, offset int64) *Error {
	return &Error{Code: code, Message: msg, SnapshotID: snapshotID, RuleIndex: ruleIndex, Offset: offset}
}
