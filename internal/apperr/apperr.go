package apperr

import (
	"errors"
	"fmt"
	"net/http"
)

const (
	CodeSampleInvalid      = "E_SAMPLE_INVALID"
	CodeStaleSnapshot      = "E_STALE_SNAPSHOT"
	CodeStaleBase          = "E_STALE_BASE"
	CodeSampleConflict     = "E_SAMPLE_CONFLICT"
	CodeConflictingRequest = "E_CONFLICTING_REQUEST"
	CodeNotFound           = "E_NOT_FOUND"
	CodePolicyInvalid      = "E_POLICY_INVALID"
	CodeInternal           = "E_INTERNAL"
	CodeMethodNotAllowed   = "E_METHOD_NOT_ALLOWED"
	CodeUnsupportedMedia   = "E_UNSUPPORTED_MEDIA_TYPE"
)

// Error is the single error shape exposed by the HTTP API and used internally so
// business failures never need to be inferred from strings.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
	Status  int            `json:"-"`
	Cause   error          `json:"-"`
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

func New(code, message string, status int, context map[string]any) *Error {
	return &Error{Code: code, Message: message, Status: status, Context: context}
}

func Wrap(code, message string, status int, context map[string]any, cause error) *Error {
	return &Error{Code: code, Message: message, Status: status, Context: context, Cause: cause}
}

func SampleInvalid(message string, context map[string]any) *Error {
	return New(CodeSampleInvalid, message, http.StatusBadRequest, context)
}

func PolicyInvalid(message string, context map[string]any) *Error {
	return New(CodePolicyInvalid, message, http.StatusBadRequest, context)
}

func StaleSnapshot(context map[string]any) *Error {
	return New(CodeStaleSnapshot, "policy snapshot version is not newer than the committed version", http.StatusConflict, context)
}

func StaleBase(context map[string]any) *Error {
	return New(CodeStaleBase, "base version is not the current policy version", http.StatusConflict, context)
}

func SampleConflict(message string, context map[string]any) *Error {
	return New(CodeSampleConflict, message, http.StatusConflict, context)
}

func ConflictingRequest(message string, context map[string]any) *Error {
	return New(CodeConflictingRequest, message, http.StatusConflict, context)
}

func NotFound(resource string, context map[string]any) *Error {
	if context == nil {
		context = map[string]any{}
	}
	context["resource"] = resource
	return New(CodeNotFound, resource+" was not found", http.StatusNotFound, context)
}

func Internal(message string, cause error) *Error {
	return Wrap(CodeInternal, message, http.StatusInternalServerError, nil, cause)
}

func From(err error) *Error {
	if err == nil {
		return nil
	}
	var app *Error
	if errors.As(err, &app) {
		return app
	}
	return Internal("internal server error", err)
}

// IsCode is useful in tests and for translating storage-level conditions.
func IsCode(err error, code string) bool {
	var app *Error
	return errors.As(err, &app) && app.Code == code
}
