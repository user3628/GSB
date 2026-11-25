package model

import (
	"encoding/json"
	"fmt"
)

// Stable business error codes used across all stages and transport layers.
const (
	CodeInsufficientProbe = "E_INSUFFICIENT_PROBE"
	CodeNumeric           = "E_NUMERIC"
	CodeQuantization      = "E_QUANTIZATION"
	CodeStaleVersion      = "E_STALE_VERSION"
	CodeHeadInvalid       = "E_HEAD_INVALID"
	CodeSentenceInvalid   = "E_SENTENCE_INVALID"
	CodePolicyInvalid     = "E_POLICY_INVALID"
	CodeConflict          = "E_CONFLICT"
)

// BizError is the structured exception-chain element. Cause may wrap another
// BizError so a failure recorded in JSON keeps the full chain.
type BizError struct {
	Code       string            `json:"code"`
	Message    string            `json:"message"`
	Layer      *int              `json:"layer,omitempty"`
	Head       *int              `json:"head,omitempty"`
	ProbeID    string            `json:"probe_id,omitempty"`
	CurrentVer int64             `json:"current_version,omitempty"`
	Context    map[string]string `json:"context,omitempty"`
	Cause      *BizError         `json:"cause,omitempty"`
}

func (e *BizError) Error() string {
	loc := ""
	if e.Layer != nil && e.Head != nil {
		loc = fmt.Sprintf(" [layer=%d head=%d]", *e.Layer, *e.Head)
	} else if e.ProbeID != "" {
		loc = " [probe=" + e.ProbeID + "]"
	}
	msg := e.Code + loc + ": " + e.Message
	if e.Cause != nil {
		msg += " <- " + e.Cause.Error()
	}
	return msg
}

func newErr(code, msg string) *BizError {
	return &BizError{Code: code, Message: msg}
}

// Wrap nests another error as the cause, preserving the structured chain.
func (e *BizError) Wrap(cause *BizError) *BizError {
	clone := *e
	clone.Cause = cause
	return &clone
}

// WithHead attaches layer/head coordinates to the structured error.
func (e *BizError) WithHead(layer, head int) *BizError {
	clone := *e
	clone.Layer = &layer
	clone.Head = &head
	return &clone
}

// WithProbe attaches a probe sentence identifier to the structured error.
func (e *BizError) WithProbe(id string) *BizError {
	clone := *e
	clone.ProbeID = id
	return &clone
}

// Marshal guarantees the error can be embedded verbatim in JSON responses.
func (e *BizError) MarshalJSON() ([]byte, error) {
	type alias BizError
	return json.Marshal((*alias)(e))
}

// AsBizError extracts a *BizError from a wrapped error chain.
func AsBizError(err error) (*BizError, bool) {
	if err == nil {
		return nil, false
	}
	if be, ok := err.(*BizError); ok {
		return be, true
	}
	type wrapper interface{ Unwrap() error }
	if w, ok := err.(wrapper); ok {
		return AsBizError(w.Unwrap())
	}
	return nil, false
}
