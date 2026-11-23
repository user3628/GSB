package main

import (
	"errors"
	"fmt"
	"net/http"
)

const (
	CodeInvalidArgument     = "E_INVALID_ARGUMENT"
	CodeRouteInvalid        = "E_ROUTE_INVALID"
	CodeConflictingMessage  = "E_CONFLICTING_MESSAGE"
	CodeConflictingRequest  = "E_CONFLICTING_REQUEST"
	CodeInvalidTransition   = "E_INVALID_TRANSITION"
	CodeLeaseStale          = "E_LEASE_STALE"
	CodeLeaseHeld           = "E_LEASE_HELD"
	CodeTxnTimeout          = "E_TXN_TIMEOUT"
	CodeCoordRefused        = "E_COORD_REFUSED"
	CodeCoordInvalid        = "E_COORD_INVALID"
	CodeTxnConflict         = "E_TXN_CONFLICT"
	CodeTxnUnknown          = "E_TXN_UNKNOWN"
	CodeReplayExpired       = "E_REPLAY_EXPIRED"
	CodeReplayInvalid       = "E_REPLAY_INVALID"
	CodeQueueFull           = "E_QUEUE_FULL"
	CodeNotFound            = "E_NOT_FOUND"
	CodeIntentRecovered     = "E_INTENT_RECOVERED"
	CodeRequestInProgress   = "E_REQUEST_IN_PROGRESS"
	CodeDatabase            = "E_DATABASE"
	CodeInternal            = "E_INTERNAL"
	CodeMethodNotAllowed    = "E_METHOD_NOT_ALLOWED"
	CodeInjectionNotApplied = "E_INJECTION_NOT_APPLIED"
)

// WorkbenchError is a stable business error. Business codes are deliberately
// distinct from transport errors so callers can drive deterministic demos.
type WorkbenchError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	HTTP    int            `json:"-"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *WorkbenchError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func newErr(code, message string) *WorkbenchError {
	return &WorkbenchError{Code: code, Message: message, HTTP: statusForCode(code)}
}

func newErrDetails(code, message string, details map[string]any) *WorkbenchError {
	return &WorkbenchError{Code: code, Message: message, HTTP: statusForCode(code), Details: details}
}

func statusForCode(code string) int {
	switch code {
	case CodeInvalidArgument, CodeRouteInvalid:
		return http.StatusBadRequest
	case CodeConflictingMessage, CodeConflictingRequest, CodeInvalidTransition,
		CodeLeaseStale, CodeLeaseHeld, CodeReplayInvalid, CodeTxnConflict,
		CodeInjectionNotApplied:
		return http.StatusConflict
	case CodeTxnTimeout:
		return http.StatusGatewayTimeout
	case CodeCoordRefused, CodeCoordInvalid, CodeTxnUnknown:
		return http.StatusBadGateway
	case CodeReplayExpired:
		return http.StatusGone
	case CodeQueueFull:
		return http.StatusTooManyRequests
	case CodeNotFound:
		return http.StatusNotFound
	case CodeIntentRecovered:
		return http.StatusGone
	case CodeRequestInProgress:
		return http.StatusAccepted
	case CodeDatabase, CodeInternal:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

func asWorkbenchError(err error) *WorkbenchError {
	if err == nil {
		return nil
	}
	var we *WorkbenchError
	if errors.As(err, &we) {
		return we
	}
	return &WorkbenchError{
		Code:    CodeInternal,
		Message: err.Error(),
		HTTP:    http.StatusInternalServerError,
	}
}
