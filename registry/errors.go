package registry

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Business error codes are stable and returned verbatim by the HTTP API.
const (
	ErrValidation         = "E_VALIDATION"
	ErrNotFound           = "E_NOT_FOUND"
	ErrStaleSnapshot      = "E_STALE_SNAPSHOT"
	ErrStaleBase          = "E_STALE_BASE"
	ErrInvalidTransition  = "E_INVALID_TRANSITION"
	ErrProbeTimeout       = "E_PROBE_TIMEOUT"
	ErrProbeQueueFull     = "E_PROBE_QUEUE_FULL"
	ErrConflictingRequest = "E_CONFLICTING_REQUEST"
	ErrInternal           = "E_INTERNAL"
)

// Error is the uniform domain error with machine-readable context.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func newError(code, message string, kv ...any) *Error {
	ctx := make(map[string]any)
	for i := 0; i+1 < len(kv); i += 2 {
		ctx[fmt.Sprint(kv[i])] = kv[i+1]
	}
	return &Error{Code: code, Message: message, Context: ctx}
}

func validationError(err error) *Error {
	return newError(ErrValidation, err.Error(), "reason", err.Error())
}

// HTTPStatus maps stable business codes to transport status codes.
func (e *Error) HTTPStatus() int {
	switch e.Code {
	case ErrValidation:
		return http.StatusBadRequest
	case ErrNotFound:
		return http.StatusNotFound
	case ErrStaleSnapshot, ErrStaleBase, ErrConflictingRequest:
		return http.StatusConflict
	case ErrInvalidTransition:
		return http.StatusUnprocessableEntity
	case ErrProbeQueueFull:
		return http.StatusTooManyRequests
	case ErrProbeTimeout:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

// Envelope is the single JSON response shape used by the HTTP workbench.
type Envelope struct {
	OK        bool            `json:"ok"`
	Data      json.RawMessage `json:"data,omitempty"`
	Error     *Error          `json:"error,omitempty"`
	RequestID string          `json:"requestId,omitempty"`
}
