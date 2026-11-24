package apperr

import (
	"errors"
	"fmt"
)

// Code is the stable machine-readable error contract used by the CLI and HTTP API.
type Code string

const (
	SpecInvalid           Code = "E_SPEC_INVALID"
	StaleSnapshot         Code = "E_STALE_SNAPSHOT"
	DependencyCycle       Code = "E_DEPENDENCY_CYCLE"
	PathDivergence        Code = "E_PATH_DIVERGENCE"
	WriteQueueFull        Code = "E_WRITE_QUEUE_FULL"
	WriteTimeout          Code = "E_WRITE_TIMEOUT"
	EndpointRefused       Code = "E_ENDPOINT_REFUSED"
	EndpointInvalid       Code = "E_ENDPOINT_INVALID"
	ReadbackFailed        Code = "E_READBACK_FAILED"
	WriteFailed           Code = "E_WRITE_FAILED"
	SnapshotCommitFailed  Code = "E_SNAPSHOT_COMMIT_FAILED"
	PersistenceFailed     Code = "E_PERSISTENCE_FAILED"
	RoundInProgress       Code = "E_ROUND_IN_PROGRESS"
	RecoveredInterruption Code = "E_RECOVERED_INTERRUPTION"
	ConvergenceIncomplete Code = "E_CONVERGENCE_INCOMPLETE"
	RoundBudgetExhausted  Code = "E_ROUND_BUDGET_EXHAUSTED"
	InvalidArgument       Code = "E_INVALID_ARGUMENT"
	Internal              Code = "E_INTERNAL"
)

// Error deliberately carries details that can be rendered without parsing prose.
type Error struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func New(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func WithDetails(code Code, details any, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Details: details}
}

func As(err error) (*Error, bool) {
	var target *Error
	if errors.As(err, &target) {
		return target, true
	}
	return nil, false
}

func CodeOf(err error) Code {
	if err == nil {
		return ""
	}
	if e, ok := As(err); ok {
		return e.Code
	}
	return Internal
}
