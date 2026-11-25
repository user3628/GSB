package prefixcache

import "fmt"

const (
	CodeStaleVersion   = "E_STALE_VERSION"
	CodeSlotConflict   = "E_SLOT_CONFLICT"
	CodeInvalidPolicy  = "E_INVALID_POLICY"
	CodeInvalidRequest = "E_INVALID_REQUEST"
	CodeNotFound       = "E_NOT_FOUND"
	CodeLockTimeout    = "E_LOCK_TIMEOUT"
	CodeInterrupted    = "E_INTERRUPTED"
	CodeInternal       = "E_INTERNAL"
)

// Error is the stable, machine-readable error returned by the HTTP layer and
// available through errors.As for embedded use.
type Error struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	RequestID       string `json:"request_id,omitempty"`
	SlotNo          int    `json:"slot_no"`
	CurrentVersion  int64  `json:"current_version"`
	ObservedVersion int64  `json:"observed_version"`
	Field           string `json:"field,omitempty"`
	Cause           error  `json:"-"`
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	msg := fmt.Sprintf("%s: %s", e.Code, e.Message)
	if e.RequestID != "" {
		msg += fmt.Sprintf(" request_id=%s", e.RequestID)
	}
	if e.SlotNo != 0 {
		msg += fmt.Sprintf(" slot_no=%d", e.SlotNo)
	}
	if e.CurrentVersion != 0 || e.ObservedVersion != 0 {
		msg += fmt.Sprintf(" observed=%d current=%d", e.ObservedVersion, e.CurrentVersion)
	}
	return msg
}

func (e *Error) Unwrap() error { return e.Cause }

func newError(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

func staleError(observed, current int64, message string) *Error {
	if message == "" {
		message = "observed version is not the current effective version"
	}
	return &Error{
		Code:            CodeStaleVersion,
		Message:         message,
		CurrentVersion:  current,
		ObservedVersion: observed,
	}
}

func slotConflictError(requestID string, slotNo int, message string) *Error {
	return &Error{
		Code:      CodeSlotConflict,
		Message:   message,
		RequestID: requestID,
		SlotNo:    slotNo,
	}
}

func invalidRequestError(requestID, message string) *Error {
	return &Error{Code: CodeInvalidRequest, Message: message, RequestID: requestID}
}
