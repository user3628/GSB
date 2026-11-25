package errs

import (
	"errors"
	"fmt"
)

const (
	ENumeric             = "E_NUMERIC"
	EErrorBound          = "E_ERROR_BOUND"
	EIncrementalMismatch = "E_INCREMENTAL_MISMATCH"
	EStaleCheckpoint     = "E_STALE_CHECKPOINT"
	EMemoryBudget        = "E_MEMORY_BUDGET"
	EConfig              = "E_CONFIG"
	EState               = "E_STATE"
	EIO                  = "E_IO"
)

type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Fields  map[string]any `json:"fields,omitempty"`
}

func (e *Error) Error() string {
	if len(e.Fields) == 0 {
		return e.Code + ": " + e.Message
	}
	return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Fields)
}

func New(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Fields: map[string]any{}}
}

func (e *Error) With(key string, value any) *Error {
	if e.Fields == nil {
		e.Fields = map[string]any{}
	}
	e.Fields[key] = value
	return e
}

func (e *Error) WithFields(fields map[string]any) *Error {
	if e.Fields == nil {
		e.Fields = map[string]any{}
	}
	for k, v := range fields {
		e.Fields[k] = v
	}
	return e
}

func Code(err error) string {
	var ce *Error
	if errors.As(err, &ce) {
		return ce.Code
	}
	return ""
}

func As(err error) *Error {
	var ce *Error
	if errors.As(err, &ce) {
		return ce
	}
	return nil
}
