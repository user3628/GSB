package blockpool

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Code is a stable business error code returned by the block pool and HTTP API.
type Code string

const (
	CodeSpecInvalid        Code = "E_SPEC_INVALID"
	CodeContentCorrupt     Code = "E_CONTENT_CORRUPT"
	CodeLeaseStale         Code = "E_LEASE_STALE"
	CodeLeaseHeld          Code = "E_LEASE_HELD"
	CodeNodeBusy           Code = "E_NODE_BUSY"
	CodeReclaimUnsafe      Code = "E_RECLAIM_UNSAFE"
	CodeConflictingRequest Code = "E_CONFLICTING_REQUEST"
	CodeNodeUnknown        Code = "E_NODE_UNKNOWN"
	CodeBlockNotFound      Code = "E_BLOCK_NOT_FOUND"
	CodeNotFound           Code = "E_NOT_FOUND"
	CodeInternal           Code = "E_INTERNAL"
	CodeContextCanceled    Code = "E_CONTEXT_CANCELED"
)

// Error is the single business error type used by the component.
// Context is deliberately JSON-shaped so HTTP and persisted idempotent results
// can carry the same diagnostic information.
type Error struct {
	Code    Code           `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if len(e.Context) == 0 {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	b, _ := json.Marshal(e.Context)
	return fmt.Sprintf("%s: %s (%s)", e.Code, e.Message, b)
}

// Errorf creates a stable business error. Context is supplied as alternating
// key/value pairs; a malformed trailing key is ignored.
func Errorf(code Code, message string, keyValues ...any) *Error {
	ctx := make(map[string]any, len(keyValues)/2)
	for i := 0; i+1 < len(keyValues); i += 2 {
		key, ok := keyValues[i].(string)
		if !ok || key == "" {
			continue
		}
		ctx[key] = keyValues[i+1]
	}
	if len(ctx) == 0 {
		ctx = nil
	}
	return &Error{Code: code, Message: message, Context: ctx}
}

func asBusinessError(err error) *Error {
	if err == nil {
		return nil
	}
	var be *Error
	if errors.As(err, &be) {
		return be
	}
	return Errorf(CodeInternal, "internal error", "cause", err.Error())
}

func errorFromJSON(raw []byte) *Error {
	var e Error
	if err := json.Unmarshal(raw, &e); err != nil || e.Code == "" {
		return Errorf(CodeInternal, "stored idempotent error is unreadable")
	}
	return &e
}
