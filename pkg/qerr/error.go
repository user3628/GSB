// Package qerr defines stable business error codes shared by the library and
// the command line tool.
package qerr

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Code is a stable, machine-readable business error code.
type Code string

const (
	CodeInvalidInput      Code = "E_INVALID_INPUT"
	CodeInvalidTransition Code = "E_INVALID_TRANSITION"
	CodeMissingArtifact   Code = "E_MISSING_ARTIFACT"
	CodeStaleCheckpoint   Code = "E_STALE_CHECKPOINT"
	CodeBudgetExceeded    Code = "E_BUDGET_EXCEEDED"
	CodeNumericMismatch   Code = "E_NUMERIC_MISMATCH"
	CodeInternal          Code = "E_INTERNAL"
)

// Error carries stable business context alongside the usual Go error text.
// Details is intentionally JSON-friendly so callers can persist diagnostics
// without depending on concrete Go types.
type Error struct {
	Code    Code            `json:"code"`
	ShardID string          `json:"shard_id,omitempty"`
	Stage   string          `json:"stage,omitempty"`
	Layer   string          `json:"layer,omitempty"`
	Message string          `json:"message"`
	Details json.RawMessage `json:"details,omitempty"`
	Err     error           `json:"-"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	msg := string(e.Code)
	if e.ShardID != "" {
		msg += " shard=" + e.ShardID
	}
	if e.Stage != "" {
		msg += " stage=" + e.Stage
	}
	if e.Layer != "" {
		msg += " layer=" + e.Layer
	}
	if e.Message != "" {
		msg += ": " + e.Message
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *Error) Unwrap() error { return e.Err }

// WithDetail marshals v into Details and returns the receiver.
func (e *Error) WithDetail(v any) *Error {
	b, err := json.Marshal(v)
	if err == nil {
		e.Details = b
	}
	return e
}

// New creates a business error.
func New(code Code, shard, stage, layer, message string, cause error) *Error {
	return &Error{Code: code, ShardID: shard, Stage: stage, Layer: layer, Message: message, Err: cause}
}

// ExitCode maps business errors onto deterministic CLI exit codes.
func ExitCode(err error) int {
	var be *Error
	if !errors.As(err, &be) {
		if err == nil {
			return 0
		}
		return 1
	}
	switch be.Code {
	case CodeInvalidInput:
		return 2
	case CodeNumericMismatch:
		return 3
	case CodeStaleCheckpoint:
		return 4
	case CodeBudgetExceeded:
		return 5
	default:
		return 1
	}
}

// IsCode reports whether err contains the given business code.
func IsCode(err error, code Code) bool {
	var be *Error
	return errors.As(err, &be) && be.Code == code
}

// Wrap turns an ordinary error into a business error while preserving an
// existing business error.
func Wrap(code Code, shard, stage, layer, message string, err error) error {
	if err == nil {
		return nil
	}
	var be *Error
	if errors.As(err, &be) {
		return err
	}
	return New(code, shard, stage, layer, message, err)
}

// Aggregate combines multiple business errors without hiding any context.
type Aggregate struct {
	Errors []*Error `json:"errors"`
}

func (a *Aggregate) Error() string {
	if a == nil || len(a.Errors) == 0 {
		return ""
	}
	if len(a.Errors) == 1 {
		return a.Errors[0].Error()
	}
	return fmt.Sprintf("%s (and %d more errors)", a.Errors[0].Error(), len(a.Errors)-1)
}

func (a *Aggregate) Unwrap() []error {
	out := make([]error, 0, len(a.Errors))
	for _, err := range a.Errors {
		out = append(out, err)
	}
	return out
}
