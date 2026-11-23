package bizerr

import (
	"errors"
	"fmt"
)

const (
	CodePlanInvalid        = "E_PLAN_INVALID"
	CodeFormatCorrupt      = "E_FORMAT_CORRUPT"
	CodeFormatUnsupported  = "E_FORMAT_UNSUPPORTED"
	CodeSnapshotStale      = "E_SNAPSHOT_STALE"
	CodeStaleBase          = "E_STALE_BASE"
	CodeConflictingRequest = "E_CONFLICTING_REQUEST"
	CodeSnapshotNotFound   = "E_SNAPSHOT_NOT_FOUND"
	CodeBadRequest         = "E_BAD_REQUEST"
	CodeIO                 = "E_IO"
	CodeInternal           = "E_INTERNAL"
)

// Error is the single error shape exposed by the HTTP API. Context must contain
// only JSON-serialisable values and should carry enough detail to reproduce the
// decision that produced the error.
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

func New(code, message string, ctx map[string]any) *Error {
	if ctx == nil {
		ctx = map[string]any{}
	}
	return &Error{Code: code, Message: message, Context: ctx}
}

func Errorf(code, format string, args ...any) *Error {
	return New(code, fmt.Sprintf(format, args...), nil)
}

func WithContext(err error, key string, value any) error {
	var be *Error
	if !errors.As(err, &be) {
		return err
	}
	cp := cloneContext(be.Context)
	cp[key] = value
	return &Error{Code: be.Code, Message: be.Message, Context: cp}
}

func As(err error) (*Error, bool) {
	var be *Error
	if errors.As(err, &be) {
		return be, true
	}
	return nil, false
}

func Code(err error) string {
	if be, ok := As(err); ok {
		return be.Code
	}
	return CodeInternal
}

func cloneContext(in map[string]any) map[string]any {
	out := make(map[string]any, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}
