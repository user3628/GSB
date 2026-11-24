// Package apperr defines the business errors exposed by the replication lag
// engine. Every error that crosses the HTTP boundary carries a stable code and
// machine-readable context.
package apperr

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
)

const (
	CodeBadRequest          = "E_BAD_REQUEST"
	CodeNotFound            = "E_NOT_FOUND"
	CodeObservationInvalid  = "E_OBSERVATION_INVALID"
	CodeObservationConflict = "E_OBSERVATION_CONFLICT"
	CodeFormatCorrupt       = "E_FORMAT_CORRUPT"
	CodeFormatUnsupported   = "E_FORMAT_UNSUPPORTED"
	CodeInvalidTransition   = "E_INVALID_TRANSITION"
	CodeConflictingBatch    = "E_CONFLICTING_BATCH"
	CodeStaleBase           = "E_STALE_BASE"
	CodeBatchAborted        = "E_BATCH_ABORTED"
	CodeInternal            = "E_INTERNAL"
)

// Error is a deterministic, serializable business error.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context"`
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func New(code, message string, context map[string]any) *Error {
	if context == nil {
		context = map[string]any{}
	}
	return &Error{Code: code, Message: message, Context: context}
}

func Wrap(code, message string, context map[string]any, err error) *Error {
	e := New(code, message, context)
	if err != nil {
		e.Context["cause"] = err.Error()
	}
	return e
}

// As extracts a business error while retaining unknown errors as E_INTERNAL.
func As(err error) *Error {
	if err == nil {
		return nil
	}
	var appErr *Error
	if errors.As(err, &appErr) {
		if appErr.Context == nil {
			appErr.Context = map[string]any{}
		}
		return appErr
	}
	return New(CodeInternal, "internal error", map[string]any{"cause": err.Error()})
}

// HTTPStatus maps business errors to deterministic HTTP status codes.
func HTTPStatus(code string) int {
	switch code {
	case CodeBadRequest, CodeObservationInvalid, CodeFormatCorrupt:
		return http.StatusBadRequest
	case CodeNotFound:
		return http.StatusNotFound
	case CodeObservationConflict, CodeConflictingBatch, CodeStaleBase, CodeBatchAborted:
		return http.StatusConflict
	case CodeInvalidTransition:
		return http.StatusUnprocessableEntity
	case CodeFormatUnsupported:
		return http.StatusUnsupportedMediaType
	default:
		return http.StatusInternalServerError
	}
}

// Public returns a copy with sorted context keys for stable output.
func (e *Error) Public() map[string]any {
	ctx := make(map[string]any, len(e.Context))
	keys := make([]string, 0, len(e.Context))
	for key := range e.Context {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		ctx[key] = e.Context[key]
	}
	return map[string]any{
		"code":    e.Code,
		"message": e.Message,
		"context": ctx,
	}
}
