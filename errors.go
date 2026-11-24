package walgroup

import (
	"errors"
	"fmt"
)

// Stable public business error codes. Callers should branch on these codes,
// not on error messages.
const (
	CodeSegmentCorrupt    = "E_SEGMENT_CORRUPT"
	CodeRotateFailed      = "E_ROTATE_FAILED"
	CodeInvalidTransition = "E_INVALID_TRANSITION"
	CodeConflictingTxn    = "E_CONFLICTING_TXN"
	CodeReclaimUnsafe     = "E_RECLAIM_UNSAFE"
	CodeReclaimFailed     = "E_RECLAIM_FAILED"
	CodeCheckpointInvalid = "E_CHECKPOINT_INVALID"
	CodeSnapshotRetired   = "E_SNAPSHOT_RETIRED"
	CodeCheckpointLag     = "E_CHECKPOINT_LAG"
	CodeGroupCommitFailed = "E_GROUP_COMMIT_FAILED"
	CodeInvalidArgument   = "E_INVALID_ARGUMENT"
	CodeNotFound          = "E_NOT_FOUND"
	CodeIO                = "E_IO"
	CodeClosed            = "E_CLOSED"
	CodeRecoveryRequired  = "E_RECOVERY_REQUIRED"
)

// Error is the structured error returned by every public operation. Fields
// that are not relevant to a particular code are omitted from JSON.
type Error struct {
	Code              string `json:"code"`
	Message           string `json:"message"`
	SegmentNo         uint64 `json:"segment_no,omitempty"`
	ExpectedStartLSN  uint64 `json:"expected_start_lsn,omitempty"`
	ActualStartLSN    uint64 `json:"actual_start_lsn,omitempty"`
	EarliestLSN       uint64 `json:"earliest_lsn,omitempty"`
	EarliestUncovered uint64 `json:"earliest_uncovered_lsn,omitempty"`
	MinServiceableLSN uint64 `json:"min_serviceable_lsn,omitempty"`
	TxnID             string `json:"txn_id,omitempty"`
	From              string `json:"from,omitempty"`
	To                string `json:"to,omitempty"`
	Cause             error  `json:"-"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

func newError(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func wrapError(code string, cause error, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Cause: cause}
}

// CodeOf returns the stable business code carried by err, if any.
func CodeOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// AsError returns the structured error carried by err, if any.
func AsError(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}
