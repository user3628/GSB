package errs

import "errors"

const (
	SpecInvalid       = "E_SPEC_INVALID"
	DependencyCycle   = "E_DEPENDENCY_CYCLE"
	StaleBase         = "E_STALE_BASE"
	StaleVersion      = "E_STALE_VERSION"
	WriteTimeout      = "E_WRITE_TIMEOUT"
	WriteQueueFull    = "E_WRITE_QUEUE_FULL"
	RetryExhausted    = "E_RETRY_EXHAUSTED"
	ConflictRequest   = "E_CONFLICTING_REQUEST"
	Interrupted       = "E_INTERRUPTED"
	EndpointRefused   = "E_ENDPOINT_REFUSED"
	RoundTimeExceeded = "E_ROUND_TIME_BUDGET_EXCEEDED"
	Internal          = "E_INTERNAL"
)

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Code + ": " + e.Message
}

func New(code, message string, details ...any) *Error {
	e := &Error{Code: code, Message: message}
	if len(details) > 0 {
		e.Details = details[0]
	}
	return e
}

func Code(err error) string {
	var target *Error
	if errors.As(err, &target) {
		return target.Code
	}
	return Internal
}

func AsError(err error) *Error {
	var target *Error
	if errors.As(err, &target) {
		return target
	}
	return New(Internal, err.Error())
}
