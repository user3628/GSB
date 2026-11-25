package model

import (
	"errors"
	"fmt"
)

const (
	CodeFeatureInvalid  = "E_FEATURE_INVALID"
	CodeQuantInvalid    = "E_QUANT_INVALID"
	CodeNumericMismatch = "E_NUMERIC_MISMATCH"
	CodeQueryInvalid    = "E_QUERY_INVALID"
	CodeStaleVersion    = "E_STALE_VERSION"
	CodeStaleCheckpoint = "E_STALE_CHECKPOINT"
	CodePolicyInvalid   = "E_POLICY_INVALID"
	CodeInternal        = "E_INTERNAL"
)

// BizError is the structured diagnostic error used by CLI and HTTP.
type BizError struct {
	Code                 string   `json:"code"`
	Message              string   `json:"message"`
	ItemID               string   `json:"item_id,omitempty"`
	Stage                string   `json:"stage,omitempty"`
	Dim                  *int     `json:"dim,omitempty"`
	CurrentVersion       int      `json:"current_version,omitempty"`
	MinAcceptableVersion int      `json:"min_acceptable_version,omitempty"`
	CurrentSeq           int      `json:"current_seq,omitempty"`
	ExpectedSeq          int      `json:"expected_seq,omitempty"`
	MaxAbsDiff           *float64 `json:"max_abs_diff,omitempty"`
	MaxRelDiff           *float64 `json:"max_rel_diff,omitempty"`
	Reason               string   `json:"reason,omitempty"`
	Details              any      `json:"details,omitempty"`
	Partial              any      `json:"partial,omitempty"`
}

func (e *BizError) Error() string {
	if e.ItemID != "" {
		return fmt.Sprintf("%s: %s (item_id=%s stage=%s)", e.Code, e.Message, e.ItemID, e.Stage)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewBiz(code, stage, itemID, format string, args ...any) *BizError {
	return &BizError{Code: code, Stage: stage, ItemID: itemID, Message: fmt.Sprintf(format, args...)}
}

func AsBiz(err error) (*BizError, bool) {
	var biz *BizError
	if errors.As(err, &biz) {
		return biz, true
	}
	return nil, false
}

func DimError(code, stage, itemID string, dim int, format string, args ...any) *BizError {
	biz := NewBiz(code, stage, itemID, format, args...)
	d := dim
	biz.Dim = &d
	return biz
}
