package convergence

import (
	"errors"
	"fmt"
)

const (
	CodeSpecInvalid        = "E_SPEC_INVALID"
	CodeDependencyCycle    = "E_DEPENDENCY_CYCLE"
	CodeStaleBase          = "E_STALE_BASE"
	CodeWriteTimeout       = "E_WRITE_TIMEOUT"
	CodeApplyMismatch      = "E_APPLY_MISMATCH"
	CodeWriteQueueFull     = "E_WRITE_QUEUE_FULL"
	CodeConflictingRequest = "E_CONFLICTING_REQUEST"
	CodeNotFound           = "E_NOT_FOUND"
	CodeVersionConflict    = "E_VERSION_CONFLICT"
	CodeRoundBudget        = "E_ROUND_BUDGET_EXCEEDED"
	CodePersistence        = "E_PERSISTENCE"
	CodeWriteFailed        = "E_WRITE_FAILED"
)

// Error is the stable business error returned by the embeddable component and CLI.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Details == nil {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s (%v)", e.Code, e.Message, e.Details)
}

func NewError(code, message string, details any) *Error {
	return &Error{Code: code, Message: message, Details: details}
}

func WrapError(code, message string, err error, details any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf("%s: %v", message, err), Details: details}
}

// AsError converts an error to the component's business Error when possible.
func AsError(err error) *Error {
	var target *Error
	if errors.As(err, &target) {
		return target
	}
	return nil
}

func HasCode(err error, code string) bool {
	business := AsError(err)
	return business != nil && business.Code == code
}
