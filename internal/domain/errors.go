package domain

import (
	"encoding/json"
	"fmt"
)

const (
	CodeSpecInvalid        = "E_SPEC_INVALID"
	CodeTraceInvalid       = "E_TRACE_INVALID"
	CodeInsufficientSample = "E_INSUFFICIENT_SAMPLES"
	CodePrecisionBreach    = "E_PRECISION_BREACH"
	CodeInsufficientCap    = "E_INSUFFICIENT_CAPACITY"
	CodeConflicting        = "E_CONFLICTING_REQUEST"
	CodeBatchLimit         = "E_BATCH_LIMIT"
	CodeLatencyLimit       = "E_LATENCY_LIMIT"
	CodeNotFound           = "E_NOT_FOUND"
	CodeBadRequest         = "E_BAD_REQUEST"
	CodeInternal           = "E_INTERNAL"
)

// BizError is the single error envelope used by the domain and HTTP layers.
type BizError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *BizError) Error() string {
	if len(e.Context) == 0 {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	b, _ := json.Marshal(e.Context)
	return fmt.Sprintf("%s: %s: %s", e.Code, e.Message, b)
}

func NewBizError(code, message string, context map[string]any) *BizError {
	return &BizError{Code: code, Message: message, Context: context}
}

func AsBizError(err error) *BizError {
	if err == nil {
		return nil
	}
	if b, ok := err.(*BizError); ok {
		return b
	}
	return NewBizError(CodeInternal, err.Error(), nil)
}
