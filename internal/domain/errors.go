package domain

import (
	"errors"
	"fmt"
)

type Code string

const (
	CodeSnapshotInvalid    Code = "E_SNAPSHOT_INVALID"
	CodeNumericOverflow    Code = "E_NUMERIC_OVERFLOW"
	CodeRecalcQueueFull    Code = "E_RECALC_QUEUE_FULL"
	CodeStaleVersion       Code = "E_STALE_VERSION"
	CodeInvalidTransition  Code = "E_INVALID_TRANSITION"
	CodeZeroDurationWindow Code = "E_WINDOW_ZERO_DURATION"
	CodeWindowRange        Code = "E_WINDOW_RANGE_OVERFLOW"
	CodeNotFound           Code = "E_NOT_FOUND"
	CodeBadRequest         Code = "E_BAD_REQUEST"
	CodeInternal           Code = "E_INTERNAL"
)

type Error struct {
	Code    Code           `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context"`
}

func (e *Error) Error() string {
	if len(e.Context) == 0 {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s (%v)", e.Code, e.Message, e.Context)
}

func NewError(code Code, message string, context map[string]any) *Error {
	if context == nil {
		context = map[string]any{}
	}
	return &Error{Code: code, Message: message, Context: context}
}

func WithContext(err error, values map[string]any) error {
	var businessErr *Error
	if !errors.As(err, &businessErr) {
		return err
	}
	merged := make(map[string]any, len(businessErr.Context)+len(values))
	for key, value := range businessErr.Context {
		merged[key] = value
	}
	for key, value := range values {
		merged[key] = value
	}
	return &Error{Code: businessErr.Code, Message: businessErr.Message, Context: merged}
}

func BusinessError(err error) (*Error, bool) {
	var businessErr *Error
	if errors.As(err, &businessErr) {
		return businessErr, true
	}
	return nil, false
}
