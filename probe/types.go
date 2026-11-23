// Package probe executes minimal real file-system and process-start probes in
// an isolated temporary workspace.
package probe

import "sandboxpolicy/business"

type Options struct {
	TempParent string
	TimeoutMS  int
}

type RuleResult struct {
	RuleID   string        `json:"rule_id"`
	Action   string        `json:"action"`
	Ref      string        `json:"ref"`
	Expected string        `json:"expected"`
	Observed string        `json:"observed"`
	Status   string        `json:"status"`
	Match    bool          `json:"match"`
	Code     business.Code `json:"code,omitempty"`
	Detail   string        `json:"detail,omitempty"`
}

type Summary struct {
	Total      int `json:"total"`
	Matched    int `json:"matched"`
	Mismatched int `json:"mismatched"`
	Failed     int `json:"failed"`
	Skipped    int `json:"skipped"`
}

type Report struct {
	Namespace     string          `json:"namespace"`
	Version       int             `json:"version"`
	Results       []RuleResult    `json:"results"`
	Summary       Summary         `json:"summary"`
	BusinessCodes []business.Code `json:"business_codes"`
}
