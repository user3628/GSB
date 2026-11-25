// Package errcode defines the stable business error codes used across the
// alert-noise-reduction and root-cause-correlation engine.
package errcode

import "fmt"

// Business error codes. They are part of the acceptance contract and are
// emitted both through Go errors and the HTTP API payloads.
const (
	ESampleInvalid      = "E_SAMPLE_INVALID"
	EStaleBase          = "E_STALE_BASE"
	ESnapshotInvalid    = "E_SNAPSHOT_INVALID"
	EInvalidTransition  = "E_INVALID_TRANSITION"
	EPatrolQueueFull    = "E_PATROL_QUEUE_FULL"
	EConflictingRequest = "E_CONFLICTING_REQUEST"
	EIntentNotFound     = "E_INTENT_NOT_FOUND"
	EBindFailed         = "E_BIND_FAILED"
	EInternal           = "E_INTERNAL"
)

// BizError is an error carrying a stable business code plus a human readable
// detail message.
type BizError struct {
	Code string
	Msg  string
}

func (e *BizError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Msg)
}

// New builds a BizError.
func New(code, format string, args ...any) error {
	return &BizError{Code: code, Msg: fmt.Sprintf(format, args...)}
}

// CodeOf extracts the business code from an error, defaulting to EInternal.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if be, ok := err.(*BizError); ok {
		return be.Code
	}
	return EInternal
}
