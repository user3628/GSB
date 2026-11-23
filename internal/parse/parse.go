// Package parse normalizes binary event logs (v1/v2) and JSONL summaries
// into the unified internal event model.
package parse

import "flowtrace/internal/model"

// Error is a stable business-code parse failure. Error events never enter
// the normalized event set.
type Error struct {
	Code   string `json:"code"`
	Offset int    `json:"offset,omitempty"`
	Line   int    `json:"line,omitempty"`
	Detail string `json:"detail"`
}

// Result carries the normalized events plus any per-record errors.
type Result struct {
	Events []model.Event
	Errors []Error
}
