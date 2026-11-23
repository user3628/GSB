package apperr

import (
	"fmt"
	"net/http"
)

const (
	CodeInvalidRequest      = "E_INVALID_REQUEST"
	CodeNotFound            = "E_NOT_FOUND"
	CodeBlockChecksum       = "E_BLOCK_CHECKSUM"
	CodeBlockConflict       = "E_BLOCK_CONFLICT"
	CodeSnapshotReferenced  = "E_SNAPSHOT_REFERENCED"
	CodeConflictingRequest  = "E_CONFLICTING_REQUEST"
	CodeOperationAborted    = "E_OPERATION_ABORTED"
	CodeRollbackRejected    = "E_ROLLBACK_REJECTED"
	CodeSnapshotNotFound    = "E_SNAPSHOT_NOT_FOUND"
	CodeSnapshotParent      = "E_SNAPSHOT_PARENT"
	CodeMetricStale         = "E_METRIC_STALE"
	CodeSnapshotCorrupt     = "E_SNAPSHOT_CORRUPT"
	CodeVolumeExists        = "E_VOLUME_EXISTS"
	CodeSnapshotExists      = "E_SNAPSHOT_EXISTS"
	CodeVolumeNotFound      = "E_VOLUME_NOT_FOUND"
	CodeMethodNotAllowed    = "E_METHOD_NOT_ALLOWED"
	CodeInternal            = "E_INTERNAL"
	CodeRecoveryUnavailable = "E_RECOVERY_UNAVAILABLE"
)

// Error is the only error shape exposed by the API. Context deliberately uses
// strings and integer counts only so it can be persisted verbatim.
type Error struct {
	Code       string            `json:"code"`
	Message    string            `json:"message"`
	Context    map[string]string `json:"context,omitempty"`
	HTTPStatus int               `json:"-"`

	// ReplayStatus/ReplayBody carry the exact persisted response of an
	// idempotent request. They are never serialized inside the error object.
	ReplayStatus int    `json:"-"`
	ReplayBody   []byte `json:"-"`
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func New(code, message string, status int) *Error {
	return &Error{Code: code, Message: message, Context: map[string]string{}, HTTPStatus: status}
}

func WithContext(err *Error, key string, value any) *Error {
	return err.WithMore(key, value)
}

func (e *Error) WithMore(pairs ...any) *Error {
	if e.Context == nil {
		e.Context = map[string]string{}
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		e.Context[fmt.Sprint(pairs[i])] = fmt.Sprint(pairs[i+1])
	}
	return e
}

func Invalid(message string) *Error {
	return New(CodeInvalidRequest, message, http.StatusBadRequest)
}

func NotFound(message string) *Error {
	return New(CodeNotFound, message, http.StatusNotFound)
}

func Internal(err error) *Error {
	e := New(CodeInternal, "internal error", http.StatusInternalServerError)
	if err != nil {
		e.Context["detail"] = err.Error()
	}
	return e
}

func As(err error) *Error {
	if e, ok := err.(*Error); ok {
		return e
	}
	return Internal(err)
}
