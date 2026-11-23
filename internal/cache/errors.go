package cache

import (
	"errors"
	"fmt"
)

const (
	CodeEntryInvalid       = "E_ENTRY_INVALID"
	CodeInvalidTransition  = "E_INVALID_TRANSITION"
	CodeDigestMismatch     = "E_DIGEST_MISMATCH"
	CodeEntryExpired       = "E_ENTRY_EXPIRED"
	CodeConflictingWrite   = "E_CONFLICTING_WRITE"
	CodeCacheMiss          = "E_CACHE_MISS"
	CodeSnapshotInvalid    = "E_SNAPSHOT_INVALID"
	CodeSnapshotSequence   = "E_SNAPSHOT_SEQUENCE"
	CodeIO                 = "E_IO"
	CodeInternal           = "E_INTERNAL"
	CodeRecoveryIncomplete = "E_RECOVERY_INCOMPLETE"
	CodeNotFound           = "E_NOT_FOUND"
	CodeMethodNotAllowed   = "E_METHOD_NOT_ALLOWED"
)

// AppError is the single error type exposed by the cache. Code is stable and
// suitable for both HTTP clients and CI scripts; Context contains inspectable
// facts such as the cache key, expected digest, or previous state.
type AppError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
	cause   error
}

func (e *AppError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *AppError) Unwrap() error { return e.cause }

func newError(code, message string, context map[string]any) *AppError {
	return &AppError{Code: code, Message: message, Context: context}
}

func NewEntryInvalid(message string, context map[string]any, cause error) error {
	return &AppError{Code: CodeEntryInvalid, Message: message, Context: context, cause: cause}
}

func NewSnapshotInvalid(message string, context map[string]any) error {
	return newError(CodeSnapshotInvalid, message, context)
}

func NewInternal(message string, context map[string]any, cause error) error {
	return &AppError{Code: CodeInternal, Message: message, Context: context, cause: cause}
}

func wrapError(code, message string, context map[string]any, cause error) *AppError {
	return &AppError{Code: code, Message: message, Context: context, cause: cause}
}

func withContext(err error, values map[string]any) error {
	var appErr *AppError
	if !errors.As(err, &appErr) {
		return err
	}
	merged := make(map[string]any, len(appErr.Context)+len(values))
	for key, value := range appErr.Context {
		merged[key] = value
	}
	for key, value := range values {
		merged[key] = value
	}
	clone := *appErr
	clone.Context = merged
	return &clone
}

func CodeOf(err error) string {
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr.Code
	}
	return CodeInternal
}

func IsCode(err error, code string) bool { return CodeOf(err) == code }
