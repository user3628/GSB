package engine

import (
	"errors"
	"fmt"
)

const (
	CodeNumeric             = "E_NUMERIC"
	CodeIncrementalMismatch = "E_INCREMENTAL_MISMATCH"
	CodeStaleCheckpoint     = "E_STALE_CHECKPOINT"
	CodeFeatureDimInvalid   = "E_FEATURE_DIM_INVALID"
	CodeLabelInvalid        = "E_LABEL_INVALID"
	CodeLearningRateInvalid = "E_LEARNING_RATE_INVALID"
	CodeHashDimZero         = "E_HASH_DIM_ZERO"
	CodeHashDimInvalid      = "E_HASH_DIM_INVALID"
	CodeConfigInvalid       = "E_CONFIG_INVALID"
	CodeNotFound            = "E_NOT_FOUND"
	CodeConflict            = "E_CONFLICT"
	CodeLockUnavailable     = "E_LOCK_UNAVAILABLE"
	CodeReplacementRequired = "E_REPLACEMENT_REQUIRED"
)

// Error is the stable error representation used by both Go callers and the CLI.
type Error struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	RequestID       string `json:"request_id,omitempty"`
	Dim             *int   `json:"dim,omitempty"`
	UnitID          *int   `json:"unit_id,omitempty"`
	ExpectedVersion int64  `json:"expected_version,omitempty"`
	ActualVersion   int64  `json:"actual_version,omitempty"`
	FirstDiffByte   int64  `json:"first_diff_byte,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func newError(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func asError(err error) (*Error, bool) {
	if err == nil {
		return nil, false
	}
	var ee *Error
	if errors.As(err, &ee) {
		return ee, true
	}
	return nil, false
}

// ErrorCode returns the stable business code, or E_INTERNAL for ordinary errors.
func ErrorCode(err error) string {
	if ee, ok := asError(err); ok {
		return ee.Code
	}
	if err == nil {
		return ""
	}
	return "E_INTERNAL"
}
