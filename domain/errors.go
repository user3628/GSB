package domain

import "fmt"

const (
	CodeReleaseInvalid      = "E_RELEASE_INVALID"
	CodeConflictingBatch    = "E_CONFLICTING_BATCH"
	CodeInsufficientSamples = "E_INSUFFICIENT_SAMPLES"
	CodeObserveMore         = "E_OBSERVE_MORE"
	CodeGateFailed          = "E_GATE_FAILED"
	CodeInvalidTransition   = "E_INVALID_TRANSITION"
	CodeVersionRetired      = "E_VERSION_RETIRED"
	CodeNotFound            = "E_NOT_FOUND"
	CodeInternal            = "E_INTERNAL"
	CodeSimulatedCrash      = "E_SIMULATED_CRASH"
)

// BusinessError is the single error envelope used across service and HTTP.
type BusinessError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *BusinessError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code, message string) *BusinessError {
	return &BusinessError{Code: code, Message: message, Context: map[string]any{}}
}

func NewErrorf(code, format string, args ...any) *BusinessError {
	return NewError(code, fmt.Sprintf(format, args...))
}

func WithContext(err *BusinessError, key string, value any) *BusinessError {
	if err.Context == nil {
		err.Context = map[string]any{}
	}
	err.Context[key] = value
	return err
}

func AsBusinessError(err error) (*BusinessError, bool) {
	if err == nil {
		return nil, false
	}
	if be, ok := err.(*BusinessError); ok {
		return be, true
	}
	return nil, false
}
