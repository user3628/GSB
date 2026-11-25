package apperr

import (
	"errors"
	"fmt"
)

const (
	CodeBoxInvalid          = "E_BOX_INVALID"
	CodeTemplateMiss        = "E_TEMPLATE_MISS"
	CodeLatencyBudget       = "E_LATENCY_BUDGET"
	CodeIncrementalMismatch = "E_INCREMENTAL_MISMATCH"
	CodeStaleCheckpoint     = "E_STALE_CHECKPOINT"
	CodeNotFound            = "E_NOT_FOUND"
	CodeInvalidArgument     = "E_INVALID_ARGUMENT"
	CodeStorage             = "E_STORAGE"
	CodeStopped             = "E_STOPPED"
	CodeInternal            = "E_INTERNAL"
)

// Error is the stable, machine-readable error envelope used by the CLI and
// HTTP API. Optional fields are omitted when they do not apply.
type Error struct {
	Code           string  `json:"code"`
	Message        string  `json:"message"`
	ImageID        string  `json:"image_id,omitempty"`
	BoxIndex       int     `json:"box_index,omitempty"`
	CandidateIndex int     `json:"candidate_index,omitempty"`
	QueryID        string  `json:"query_id,omitempty"`
	ElapsedMS      float64 `json:"elapsed_ms,omitempty"`
	Stage          string  `json:"stage,omitempty"`
	Reason         string  `json:"reason,omitempty"`
	Item           string  `json:"item,omitempty"`
	Expected       string  `json:"expected,omitempty"`
	Actual         string  `json:"actual,omitempty"`
	Wrapped        error   `json:"-"`
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.ImageID != "" {
		return fmt.Sprintf("%s: %s (image_id=%s)", e.Code, e.Message, e.ImageID)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Wrapped }

func New(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

func From(err error) *Error {
	if err == nil {
		return nil
	}
	var target *Error
	if errors.As(err, &target) {
		return target
	}
	return &Error{Code: CodeInternal, Message: err.Error(), Wrapped: err}
}

func WithImage(code, message, imageID string) *Error {
	return &Error{Code: code, Message: message, ImageID: imageID}
}

func Wrap(code, message string, err error) *Error {
	e := &Error{Code: code, Message: message, Wrapped: err}
	if err != nil {
		e.Message = message + ": " + err.Error()
	}
	return e
}
