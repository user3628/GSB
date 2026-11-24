package domain

import (
	"fmt"
)

// Business codes are part of the public API contract. Handlers must never
// replace a typed failure with a generic transport error.
const (
	CodeAcquireTimeout     = "E_ACQUIRE_TIMEOUT"
	CodePoolExhausted      = "E_POOL_EXHAUSTED"
	CodeDialFailed         = "E_DIAL_FAILED"
	CodeQueryTimeout       = "E_QUERY_TIMEOUT"
	CodeConnReset          = "E_CONN_RESET"
	CodeConflictingRequest = "E_CONFLICTING_REQUEST"
	CodeStaleConfig        = "E_STALE_CONFIG"
	CodeBadRequest         = "E_BAD_REQUEST"
	CodeNotFound           = "E_NOT_FOUND"
	CodeInternal           = "E_INTERNAL"
	CodeQueryFailed        = "E_QUERY_FAILED"
	CodeConnNotLeased      = "E_CONN_NOT_LEASED"
	CodeRequestCancelled   = "E_REQUEST_CANCELLED"
	CodeConfigInvalid      = "E_CONFIG_INVALID"
	CodeShutdown           = "E_POOL_SHUTDOWN"
)

// BusinessError is propagated through the pool and HTTP layers. Context is
// deliberately JSON-safe so every failure remains auditable.
type BusinessError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
	Cause   error          `json:"-"`
}

func (e *BusinessError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *BusinessError) Unwrap() error { return e.Cause }

func NewError(code, message string, context map[string]any) *BusinessError {
	return &BusinessError{Code: code, Message: message, Context: context}
}

func WrapError(code, message string, context map[string]any, cause error) *BusinessError {
	if cause == nil {
		return NewError(code, message, context)
	}
	return &BusinessError{Code: code, Message: message, Context: context, Cause: cause}
}
