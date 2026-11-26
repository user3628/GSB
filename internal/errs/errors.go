// Package errs defines stable business error codes shared by the resolver,
// persistence layer and management API.
package errs

import (
	"errors"
	"fmt"
)

type Code string

const (
	EMessageMalformed     Code = "E_MESSAGE_MALFORMED"
	ETCPFrameInvalid      Code = "E_TCP_FRAME_INVALID"
	ENegativeTTLUndefined Code = "E_NEGATIVE_TTL_UNDEFINED"
	EStaleBase            Code = "E_STALE_BASE"
	EConflictingRequest   Code = "E_CONFLICTING_REQUEST"
	EUpstreamQueueFull    Code = "E_UPSTREAM_QUEUE_FULL"
	EUpstreamTimeout      Code = "E_UPSTREAM_TIMEOUT"
	EUpstreamRefused      Code = "E_UPSTREAM_REFUSED"
	EUpstreamBadResponse  Code = "E_UPSTREAM_BAD_RESPONSE"
	ERoundBudgetExceeded  Code = "E_ROUND_BUDGET_EXCEEDED"
	EUnsupportedType      Code = "E_UNSUPPORTED_TYPE"
	EZoneNotFound         Code = "E_ZONE_NOT_FOUND"
	EChecksumMismatch     Code = "E_CHECKSUM_MISMATCH"
	EChecksumRequired     Code = "E_CHECKSUM_REQUIRED"
	EInvalidRequest       Code = "E_INVALID_REQUEST"
	ENotFound             Code = "E_NOT_FOUND"
	EIntentConflict       Code = "E_INTENT_CONFLICT"
	EInternal             Code = "E_INTERNAL"
)

// Error is a business error that can cross package and HTTP boundaries without
// losing its stable machine-readable code.
type Error struct {
	Code    Code
	Message string
	Cause   error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

func New(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func Wrap(code Code, err error, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Cause: err}
}

func CodeOf(err error) Code {
	if err == nil {
		return ""
	}
	var be *Error
	if errors.As(err, &be) {
		return be.Code
	}
	return EInternal
}

func MessageOf(err error) string {
	if err == nil {
		return ""
	}
	var be *Error
	if errors.As(err, &be) {
		return be.Message
	}
	return err.Error()
}

func IsCode(err error, code Code) bool { return CodeOf(err) == code }
