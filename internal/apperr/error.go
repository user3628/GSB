package apperr

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Code string

const (
	SpecInvalid         Code = "E_SPEC_INVALID"
	StaleBase           Code = "E_STALE_BASE"
	ApplyMismatch       Code = "E_APPLY_MISMATCH"
	StaleRead           Code = "E_STALE_READ"
	VerifyQueueFull     Code = "E_VERIFY_QUEUE_FULL"
	NotFound            Code = "E_NOT_FOUND"
	IdentityUnavailable Code = "E_IDENTITY_UNAVAILABLE"
	RecoveryRequired    Code = "E_RECOVERY_REQUIRED"
	Internal            Code = "E_INTERNAL"
)

type Error struct {
	Code    Code           `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *Error) Error() string {
	if len(e.Context) == 0 {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	b, _ := json.Marshal(e.Context)
	return fmt.Sprintf("%s: %s: %s", e.Code, e.Message, b)
}

func New(code Code, message string) *Error {
	return &Error{Code: code, Message: message, Context: map[string]any{}}
}

func With(code Code, message string, context map[string]any) *Error {
	if context == nil {
		context = map[string]any{}
	}
	return &Error{Code: code, Message: message, Context: context}
}

func Status(err error) int {
	e, ok := err.(*Error)
	if !ok {
		return http.StatusInternalServerError
	}
	switch e.Code {
	case SpecInvalid:
		return http.StatusBadRequest
	case StaleBase, StaleRead, ApplyMismatch:
		return http.StatusConflict
	case VerifyQueueFull:
		return http.StatusTooManyRequests
	case NotFound:
		return http.StatusNotFound
	case IdentityUnavailable, RecoveryRequired:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
