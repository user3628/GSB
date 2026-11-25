package model

import "fmt"

// Business error codes. These strings are part of the stable external contract.
const (
	CodeCalibSample     = "E_CALIB_SAMPLE"
	CodeQuantParam      = "E_QUANT_PARAM"
	CodeNumericMismatch = "E_NUMERIC_MISMATCH"
	CodeAuditInput      = "E_AUDIT_INPUT"
	CodeStaleVersion    = "E_STALE_VERSION"
	CodeState           = "E_STATE"
	CodeIO              = "E_IO"
)

// BizError is a stable business error carrying a code plus a structured diagnostic
// context so every failure can be reproduced and audited.
type BizError struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Context map[string]string `json:"context,omitempty"`
}

func (e *BizError) Error() string {
	if len(e.Context) == 0 {
		return e.Code + ": " + e.Message
	}
	return fmt.Sprintf("%s: %s (%v)", e.Code, e.Message, e.Context)
}

func NewBizError(code, message string, ctx map[string]string) *BizError {
	return &BizError{Code: code, Message: message, Context: ctx}
}

// CodeOf extracts the business code from any error, returning "" for non-business errors.
func CodeOf(err error) string {
	if be, ok := err.(*BizError); ok {
		return be.Code
	}
	return ""
}
