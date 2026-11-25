package workerr

import (
	"encoding/json"
	"fmt"
)

const (
	ELatencyBudget       = "E_LATENCY_BUDGET"
	EInvalidFold         = "E_INVALID_FOLD"
	EIncrementalMismatch = "E_INCREMENTAL_MISMATCH"
	EStaleCheckpoint     = "E_STALE_CHECKPOINT"
	EInvalidStage        = "E_INVALID_STAGE"
	ENumeric             = "E_NUMERIC"
	EConfig              = "E_CONFIG"
	ENotFound            = "E_NOT_FOUND"
	EConflict            = "E_CONFLICT"
)

type Error struct {
	Code          string  `json:"code"`
	Message       string  `json:"message"`
	QueryID       string  `json:"query_id,omitempty"`
	ElapsedMS     float64 `json:"elapsed_ms"`
	BudgetMS      float64 `json:"budget_ms"`
	Fold          *int    `json:"fold,omitempty"`
	Round         int     `json:"round,omitempty"`
	Reason        string  `json:"reason,omitempty"`
	FirstDiffFold *int    `json:"first_diff_fold,omitempty"`
	Field         string  `json:"field,omitempty"`
	ExpectedSeq   int64   `json:"expected_seq,omitempty"`
	ActualSeq     int64   `json:"actual_seq,omitempty"`
	Stage         string  `json:"stage,omitempty"`
	RequiredStage string  `json:"required_stage,omitempty"`
}

func (e *Error) Error() string {
	b, _ := json.Marshal(e)
	return string(b)
}

func New(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

func Wrap(code string, err error) *Error {
	if err == nil {
		return nil
	}
	if we, ok := err.(*Error); ok {
		return we
	}
	return &Error{Code: code, Message: err.Error()}
}

func InvalidFold(fold int, reason string) *Error {
	f := fold
	return &Error{Code: EInvalidFold, Message: fmt.Sprintf("invalid fold %d: %s", fold, reason), Fold: &f, Reason: reason}
}

func Numeric(fold, round int, field string) *Error {
	f := fold
	return &Error{Code: ENumeric, Message: fmt.Sprintf("non-finite %s at fold %d round %d", field, fold, round), Fold: &f, Round: round, Field: field}
}

func As(err error) (*Error, bool) {
	if err == nil {
		return nil, false
	}
	if we, ok := err.(*Error); ok {
		return we, true
	}
	return nil, false
}
