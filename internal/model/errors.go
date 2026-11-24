package model

import (
	"errors"
	"fmt"
	"net/http"
)

const (
	CodeBadRequest        = "E_BAD_REQUEST"
	CodeShardInvalid      = "E_SHARD_INVALID"
	CodeShardMismatch     = "E_SHARD_MISMATCH"
	CodePoolExhausted     = "E_POOL_EXHAUSTED"
	CodePrecisionDrift    = "E_PRECISION_DRIFT"
	CodeStaleSnapshot     = "E_STALE_SNAPSHOT"
	CodeStaleBase         = "E_STALE_BASE"
	CodeWriteQueueFull    = "E_WRITE_QUEUE_FULL"
	CodeConflictingSubmit = "E_CONFLICTING_SUBMIT"
	CodeSnapshotInvalid   = "E_SNAPSHOT_INVALID"
	CodeNotFound          = "E_NOT_FOUND"
	CodeNoPolicy          = "E_NO_POLICY"
	CodeWriteFailed       = "E_WRITE_FAILED"
	CodeInternal          = "E_INTERNAL"
)

// APIError is the only error shape exposed by the local HTTP API. The code is
// deliberately stable because training orchestrators branch on it.
type APIError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
	Status  int            `json:"-"`
	Cause   error          `json:"-"`
}

func (e *APIError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *APIError) Unwrap() error { return e.Cause }

func NewAPIError(code, message string) *APIError {
	return &APIError{Code: code, Message: message, Status: statusForCode(code)}
}

func WrapAPIError(code, message string, cause error) *APIError {
	e := NewAPIError(code, message)
	e.Cause = cause
	return e
}

func WithDetails(e *APIError, details map[string]any) *APIError {
	e.Details = details
	return e
}

func AsAPIError(err error) *APIError {
	if err == nil {
		return nil
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return WrapAPIError(CodeInternal, "internal engine failure", err)
}

func statusForCode(code string) int {
	switch code {
	case CodeBadRequest, CodeSnapshotInvalid:
		return http.StatusBadRequest
	case CodeShardInvalid, CodePrecisionDrift:
		return http.StatusUnprocessableEntity
	case CodeShardMismatch, CodeStaleSnapshot, CodeStaleBase, CodeConflictingSubmit:
		return http.StatusConflict
	case CodePoolExhausted, CodeWriteQueueFull:
		return http.StatusInsufficientStorage
	case CodeNotFound:
		return http.StatusNotFound
	case CodeNoPolicy:
		return http.StatusPreconditionFailed
	case CodeWriteFailed, CodeInternal:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

type ErrorEnvelope struct {
	Error *APIError `json:"error"`
}
