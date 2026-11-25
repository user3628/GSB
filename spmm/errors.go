package spmm

import (
	"errors"
	"fmt"
)

// Code is a stable, machine-readable business error code. API consumers must
// branch on Code rather than on human-readable messages.
type Code string

const (
	CodeInput           Code = "E_INPUT"
	CodeLatencyBudget   Code = "E_LATENCY_BUDGET"
	CodePathMismatch    Code = "E_PATH_MISMATCH"
	CodeNumericMismatch Code = "E_NUMERIC_MISMATCH"
	CodeMemoryBudget    Code = "E_MEMORY_BUDGET"
	CodeStalePublish    Code = "E_STALE_PUBLISH"
	CodeState           Code = "E_STATE"
	CodeInterrupted     Code = "E_INTERRUPTED"
)

// MemoryBreakdown makes a memory-budget refusal auditable without relying on
// an out-of-memory crash.
type MemoryBreakdown struct {
	PeakLimitBytes    int64 `json:"peak_limit_bytes"`
	OutputBufferBytes int64 `json:"output_buffer_bytes"`
	WorkspaceBytes    int64 `json:"workspace_bytes"`
	DenseSliceBytes   int64 `json:"dense_slice_bytes"`
	RequiredBytes     int64 `json:"required_bytes"`
	Rows              int   `json:"rows"`
	DenseColumns      int   `json:"dense_columns"`
	ColumnChunk       int   `json:"column_chunk"`
}

// Error is the structured error returned by all public business paths.
type Error struct {
	Code        Code             `json:"code"`
	Message     string           `json:"message"`
	QueryID     string           `json:"query_id,omitempty"`
	ElapsedMS   float64          `json:"elapsed_ms,omitempty"`
	BudgetMS    float64          `json:"budget_ms,omitempty"`
	RowStart    int              `json:"row_start,omitempty"`
	RowEnd      int              `json:"row_end,omitempty"`
	ColStart    int              `json:"col_start,omitempty"`
	ColEnd      int              `json:"col_end,omitempty"`
	MaxAbsDev   float64          `json:"max_abs_deviation,omitempty"`
	MaxRelDev   float64          `json:"max_relative_deviation,omitempty"`
	FirstDiff   *int             `json:"first_difference_row_block,omitempty"`
	Memory      *MemoryBreakdown `json:"memory,omitempty"`
	PlanKey     string           `json:"plan_key,omitempty"`
	PlanVersion int64            `json:"plan_version,omitempty"`
	Expectation string           `json:"expectation,omitempty"`
	Actual      string           `json:"actual,omitempty"`
	Detail      string           `json:"detail,omitempty"`
	Cause       error            `json:"-"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return string(e.Code)
}

func (e *Error) Unwrap() error { return e.Cause }

// AsError extracts a structured error from err.
func AsError(err error) (*Error, bool) {
	var target *Error
	if errors.As(err, &target) {
		return target, true
	}
	return nil, false
}

func IsCode(err error, code Code) bool {
	e, ok := AsError(err)
	return ok && e.Code == code
}

func NewError(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}
