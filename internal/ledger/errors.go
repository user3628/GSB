package ledger

import (
	"errors"
	"fmt"
	"net/http"
)

const (
	CodeStaleSnapshot      = "E_STALE_SNAPSHOT"
	CodeLedgerInvalid      = "E_LEDGER_INVALID"
	CodeEventInvalid       = "E_EVENT_INVALID"
	CodeConflictingSettle  = "E_CONFLICTING_SETTLE"
	CodeCreditExceeded     = "E_CREDIT_EXCEEDED"
	CodePoolOvercommit     = "E_POOL_OVERCOMMIT"
	CodePriorityConflict   = "E_PRIORITY_CONFLICT"
	CodeConflictingRequest = "E_CONFLICTING_REQUEST"
	CodeStaleSettlement    = "E_STALE_SETTLEMENT"
	CodeBadRequest         = "E_BAD_REQUEST"
	CodeNotFound           = "E_NOT_FOUND"
	CodeInternal           = "E_INTERNAL"
)

type BizError struct {
	Code       string         `json:"code"`
	Message    string         `json:"message"`
	Context    map[string]any `json:"context,omitempty"`
	HTTPStatus int            `json:"-"`
}

func (e *BizError) Error() string {
	if len(e.Context) == 0 {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s (%v)", e.Code, e.Message, e.Context)
}

func NewBizError(code, message string, status int, context map[string]any) *BizError {
	if status == 0 {
		status = http.StatusBadRequest
	}
	if context == nil {
		context = map[string]any{}
	}
	return &BizError{Code: code, Message: message, Context: context, HTTPStatus: status}
}

func Biz(code, message string, context map[string]any) *BizError {
	status := http.StatusUnprocessableEntity
	switch code {
	case CodeStaleSnapshot, CodeConflictingSettle, CodePoolOvercommit, CodePriorityConflict, CodeConflictingRequest, CodeStaleSettlement:
		status = http.StatusConflict
	case CodeNotFound:
		status = http.StatusNotFound
	case CodeInternal:
		status = http.StatusInternalServerError
	case CodeBadRequest:
		status = http.StatusBadRequest
	}
	return NewBizError(code, message, status, context)
}

func AsBiz(err error) *BizError {
	if err == nil {
		return nil
	}
	var be *BizError
	if errors.As(err, &be) {
		return be
	}
	return NewBizError(CodeInternal, err.Error(), http.StatusInternalServerError, nil)
}
