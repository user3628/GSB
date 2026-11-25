package domain

import "fmt"

// ErrorCode is the stable machine-readable code returned by every failure path.
type ErrorCode string

const (
	ESpecInvalid        ErrorCode = "E_SPEC_INVALID"
	EQuotaExceeded      ErrorCode = "E_QUOTA_EXCEEDED"
	EInvalidTransition  ErrorCode = "E_INVALID_TRANSITION"
	EExecQueueFull      ErrorCode = "E_EXEC_QUEUE_FULL"
	EQuotaLeak          ErrorCode = "E_QUOTA_LEAK"
	EConflictingRequest ErrorCode = "E_CONFLICTING_REQUEST"
	ENotFound           ErrorCode = "E_NOT_FOUND"
	EPolicyVersion      ErrorCode = "E_POLICY_VERSION"
	EIntentIncomplete   ErrorCode = "E_INTENT_INCOMPLETE"
	EInternal           ErrorCode = "E_INTERNAL"
)

// BizError carries a stable code plus structured details for HTTP rendering.
type BizError struct {
	Code    ErrorCode      `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *BizError) Error() string {
	if len(e.Details) == 0 {
		return string(e.Code) + ": " + e.Message
	}
	return fmt.Sprintf("%s: %s %v", e.Code, e.Message, e.Details)
}

func NewError(code ErrorCode, msg string, details map[string]any) *BizError {
	return &BizError{Code: code, Message: msg, Details: details}
}

// AsBizError extracts a *BizError from a generic error when present.
func AsBizError(err error) (*BizError, bool) {
	if err == nil {
		return nil, false
	}
	if be, ok := err.(*BizError); ok {
		return be, true
	}
	return nil, false
}
