package model

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrBaselineInvalid   ErrorCode = "E_BASELINE_INVALID"
	ErrValueTypeMismatch ErrorCode = "E_VALUE_TYPE_MISMATCH"
	ErrAuditQueueFull    ErrorCode = "E_AUDIT_QUEUE_FULL"
	ErrProbeTimeout      ErrorCode = "E_PROBE_TIMEOUT"
	ErrProbeRefused      ErrorCode = "E_PROBE_REFUSED"
	ErrStaleRead         ErrorCode = "E_STALE_READ"
	ErrStaleBase         ErrorCode = "E_STALE_BASE"
)

type CodedError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Details any       `json:"details,omitempty"`
}

func (e *CodedError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewCodedError(code ErrorCode, format string, args ...any) *CodedError {
	return &CodedError{Code: code, Message: fmt.Sprintf(format, args...)}
}

func (e *CodedError) WithDetails(details any) *CodedError {
	e.Details = details
	return e
}

func WithDetails(err error, details any) error {
	var coded *CodedError
	if errors.As(err, &coded) {
		coded.Details = details
		return coded
	}
	return err
}

func CodeOf(err error) ErrorCode {
	var coded *CodedError
	if errors.As(err, &coded) {
		return coded.Code
	}
	return ""
}

func AsCoded(err error) (*CodedError, bool) {
	var coded *CodedError
	if errors.As(err, &coded) {
		return coded, true
	}
	return nil, false
}
