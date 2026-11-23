package model

import (
	"errors"
	"fmt"
	"net/http"
)

const (
	CodeKeyInvalid          = "E_KEY_INVALID"
	CodeInvalidTransition   = "E_INVALID_TRANSITION"
	CodeStaleSnapshot       = "E_STALE_SNAPSHOT"
	CodeSnapshotInvalid     = "E_SNAPSHOT_INVALID"
	CodeSnapshotChecksum    = "E_SNAPSHOT_CHECKSUM"
	CodeVersionRetired      = "E_VERSION_RETIRED"
	CodePurgeIncomplete     = "E_PURGE_INCOMPLETE"
	CodePurgeAborted        = "E_PURGE_ABORTED"
	CodeOriginTimeout       = "E_ORIGIN_TIMEOUT"
	CodeOriginRefused       = "E_ORIGIN_REFUSED"
	CodeBodyCorrupt         = "E_BODY_CORRUPT"
	CodeOriginStatus        = "E_ORIGIN_STATUS"
	CodeRevalidateTimeout   = "E_REVALIDATE_TIMEOUT"
	CodeOriginQueueFull     = "E_ORIGIN_QUEUE_FULL"
	CodeBudgetExhausted     = "E_BUDGET_EXHAUSTED"
	CodeNodeUnknown         = "E_NODE_UNKNOWN"
	CodeNotFound            = "E_NOT_FOUND"
	CodeBadRequest          = "E_BAD_REQUEST"
	CodeRequestAborted      = "E_REQUEST_ABORTED"
	CodeIdempotencyConflict = "E_IDEMPOTENCY_IN_PROGRESS"
	CodeInternal            = "E_INTERNAL"
)

// BizError is the only error shape returned by the HTTP API. Details are
// intentionally JSON-friendly so diagnostics can be replayed and compared.
type BizError struct {
	Code       string         `json:"code"`
	Message    string         `json:"message"`
	Details    map[string]any `json:"details,omitempty"`
	HTTPStatus int            `json:"-"`
}

func (e *BizError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewBiz(code, message string) *BizError {
	return &BizError{Code: code, Message: message, HTTPStatus: StatusForCode(code)}
}

func WithDetails(err *BizError, details map[string]any) *BizError {
	if err == nil {
		return nil
	}
	cp := *err
	cp.Details = details
	return &cp
}

func AsBiz(err error) *BizError {
	if err == nil {
		return nil
	}
	var be *BizError
	if errors.As(err, &be) {
		return be
	}
	return NewBiz(CodeInternal, err.Error())
}

func StatusForCode(code string) int {
	switch code {
	case CodeKeyInvalid, CodeSnapshotInvalid, CodeSnapshotChecksum, CodeBadRequest:
		return http.StatusBadRequest
	case CodeStaleSnapshot, CodeInvalidTransition, CodeVersionRetired, CodePurgeIncomplete, CodePurgeAborted, CodeRequestAborted, CodeIdempotencyConflict:
		return http.StatusConflict
	case CodeNodeUnknown, CodeNotFound:
		return http.StatusNotFound
	case CodeOriginQueueFull:
		return http.StatusTooManyRequests
	case CodeBudgetExhausted:
		return http.StatusServiceUnavailable
	case CodeOriginTimeout, CodeRevalidateTimeout:
		return http.StatusGatewayTimeout
	case CodeOriginRefused, CodeBodyCorrupt, CodeOriginStatus:
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}
