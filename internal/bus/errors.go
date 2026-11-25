package bus

import (
	"errors"
	"fmt"
)

const (
	CodeInvalidArgument     = "E_INVALID_ARGUMENT"
	CodeUnknownTopic        = "E_UNKNOWN_TOPIC"
	CodeUnknownGroup        = "E_UNKNOWN_GROUP"
	CodeConflictingEvent    = "E_CONFLICTING_EVENT"
	CodeConflictingRequest  = "E_CONFLICTING_REQUEST"
	CodeInvalidTransition   = "E_INVALID_TRANSITION"
	CodeLeaseHeld           = "E_LEASE_HELD"
	CodeLeaseExpired        = "E_LEASE_EXPIRED"
	CodeOutOfOrder          = "E_OUT_OF_ORDER"
	CodeNoEvent             = "E_NO_EVENT"
	CodeOffsetExpired       = "E_OFFSET_EXPIRED"
	CodeOffsetInvalid       = "E_OFFSET_INVALID"
	CodeReplayActive        = "E_REPLAY_ACTIVE"
	CodeAlgorithmLocked     = "E_ALGORITHM_LOCKED"
	CodeNotFound            = "E_NOT_FOUND"
	CodeInternal            = "E_INTERNAL"
	CodeCrashInjected       = "E_SIMULATED_CRASH"
	CodeRequestIDRequired   = "E_REQUEST_ID_REQUIRED"
	CodePartitionIDsInvalid = "E_INVALID_PARTITION"
)

// BusError is a stable business error returned by both the Go API and HTTP API.
type BusError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	err     error
}

func (e *BusError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Code + ": " + e.Message
}

func (e *BusError) Unwrap() error { return e.err }

func NewError(code, message string) *BusError {
	return &BusError{Code: code, Message: message}
}

func WrapError(code, message string, err error) *BusError {
	return &BusError{Code: code, Message: message, err: err}
}

func ErrorCode(err error) string {
	var busErr *BusError
	if errors.As(err, &busErr) {
		return busErr.Code
	}
	if err != nil {
		return CodeInternal
	}
	return ""
}

func invalidArgument(format string, args ...any) *BusError {
	return NewError(CodeInvalidArgument, fmt.Sprintf(format, args...))
}

var ErrSimulatedCrash = NewError(CodeCrashInjected, "simulated process termination")
