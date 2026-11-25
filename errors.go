package feedbackstream

import "fmt"

const (
	CodeSpecInvalid       = "E_SPEC_INVALID"
	CodePipelineInvalid   = "E_PIPELINE_INVALID"
	CodeNumericOverflow   = "E_NUMERIC_OVERFLOW"
	CodeBackpressureStall = "E_BACKPRESSURE_STALL"
	CodeStaleBase         = "E_STALE_BASE"
	CodeConflictRequest   = "E_CONFLICTING_REQUEST"
	CodePolicyVersion     = "E_POLICY_VERSION_INVALID"
	CodeNotFound          = "E_NOT_FOUND"
	CodeStateInvalid      = "E_STATE_INVALID"
)

// Error is the stable coded error returned by the local engine and CLI.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if len(e.Details) == 0 {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s (%v)", e.Code, e.Message, e.Details)
}

func (e *Error) With(key string, value any) *Error {
	if e.Details == nil {
		e.Details = map[string]any{}
	}
	e.Details[key] = value
	return e
}

func NewError(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

func SpecInvalid(message string) *Error {
	return NewError(CodeSpecInvalid, message)
}

func AsError(err error) (*Error, bool) {
	for err != nil {
		if e, ok := err.(*Error); ok {
			return e, true
		}
		type unwrapper interface{ Unwrap() []error }
		if u, ok := err.(unwrapper); ok {
			for _, child := range u.Unwrap() {
				if e, ok := AsError(child); ok {
					return e, true
				}
			}
		}
		if u, ok := err.(interface{ Unwrap() error }); ok {
			err = u.Unwrap()
			continue
		}
		return nil, false
	}
	return nil, false
}
