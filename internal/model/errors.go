package model

import (
	"errors"
	"fmt"
	"net/http"
)

// Business error codes exposed by the API and persisted with failed executions.
const (
	CodeSnapshotInvalid = "E_SNAPSHOT_INVALID"
	CodeBitmapMismatch  = "E_BITMAP_MISMATCH"
	CodeNumericOverflow = "E_NUMERIC_OVERFLOW"
	CodeDivideByZero    = "E_DIVIDE_BY_ZERO"
	CodeDictIndex       = "E_DICT_INDEX"
	CodeExecAborted     = "E_EXEC_ABORTED"
	CodeStaleVersion    = "E_STALE_VERSION"
	CodeInvalidRequest  = "E_INVALID_REQUEST"
	CodeNotFound        = "E_NOT_FOUND"
	CodeExecConflict    = "E_EXEC_CONFLICT"
	CodeInternal        = "E_INTERNAL"
)

// BizError is the single error contract used by the executor and HTTP layer.
type BizError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *BizError) Error() string {
	if len(e.Context) == 0 {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s %v", e.Code, e.Message, e.Context)
}

func NewBizError(code, message string) *BizError {
	return &BizError{Code: code, Message: message, Context: map[string]any{}}
}

func (e *BizError) With(key string, value any) *BizError {
	if e.Context == nil {
		e.Context = map[string]any{}
	}
	e.Context[key] = value
	return e
}

func AsBizError(err error) (*BizError, bool) {
	var biz *BizError
	if errors.As(err, &biz) {
		return biz, true
	}
	return nil, false
}

func HTTPStatusForCode(code string) int {
	switch code {
	case CodeNotFound:
		return http.StatusNotFound
	case CodeStaleVersion, CodeExecConflict, CodeExecAborted:
		return http.StatusConflict
	case CodeSnapshotInvalid, CodeBitmapMismatch, CodeNumericOverflow,
		CodeDivideByZero, CodeDictIndex:
		return http.StatusUnprocessableEntity
	case CodeInvalidRequest:
		return http.StatusBadRequest
	case CodeInternal:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

type ErrorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

func ToErrorResponse(err error) ErrorResponse {
	if biz, ok := AsBizError(err); ok {
		return ErrorResponse{Error: ErrorBody{Code: biz.Code, Message: biz.Message, Context: biz.Context}}
	}
	return ErrorResponse{Error: ErrorBody{
		Code:    CodeInternal,
		Message: err.Error(),
		Context: map[string]any{},
	}}
}
