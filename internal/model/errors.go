package model

import (
	"errors"
	"fmt"
)

const (
	CodeSpecInvalid        = "E_SPEC_INVALID"
	CodeRecordCorrupt      = "E_RECORD_CORRUPT"
	CodeFormatUnsupported  = "E_FORMAT_UNSUPPORTED"
	CodePoolFull           = "E_POOL_FULL"
	CodePrecisionDrift     = "E_PRECISION_DRIFT"
	CodeStaleSnapshot      = "E_STALE_SNAPSHOT"
	CodeStaleBase          = "E_STALE_BASE"
	CodeWriteQueueFull     = "E_WRITE_QUEUE_FULL"
	CodeConflictingRequest = "E_CONFLICTING_REQUEST"
	CodeInProgress         = "E_IN_PROGRESS"
	CodeNotFound           = "E_NOT_FOUND"
	CodeInvalidJSON        = "E_INVALID_JSON"
	CodeInternal           = "E_INTERNAL"
)

// BizError is the stable error envelope shared by the engine and HTTP API.
type BizError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context"`
}

func (e *BizError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code, message string, context map[string]any) *BizError {
	if context == nil {
		context = map[string]any{}
	}
	return &BizError{Code: code, Message: message, Context: context}
}

func CodeOf(err error) string {
	var biz *BizError
	if errors.As(err, &biz) {
		return biz.Code
	}
	return CodeInternal
}

func AsBizError(err error) *BizError {
	var biz *BizError
	if errors.As(err, &biz) {
		return biz
	}
	return NewError(CodeInternal, err.Error(), nil)
}
