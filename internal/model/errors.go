package model

import "fmt"

// BizError 携带业务码与上下文，HTTP 层统一序列化为 JSON 失败响应。
type BizError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Context string `json:"context,omitempty"`
}

func (e *BizError) Error() string {
	if e.Context == "" {
		return e.Code + ": " + e.Message
	}
	return e.Code + ": " + e.Message + " (" + e.Context + ")"
}

func NewBizError(code, message, context string) *BizError {
	return &BizError{Code: code, Message: message, Context: context}
}

func wrapf(code, format string, args ...any) *BizError {
	return &BizError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// 业务码（需求 8 至少覆盖以下五类）。
const (
	CodeInstanceInvalid   = "E_INSTANCE_INVALID"
	CodeProbeRefused      = "E_PROBE_REFUSED"
	CodeProbeTimeout      = "E_PROBE_TIMEOUT"
	CodeDispatchQueueFull = "E_DISPATCH_QUEUE_FULL"
	CodeInvalidTransition = "E_INVALID_TRANSITION"

	CodeProbeFailed    = "E_PROBE_FAILED"
	CodeNoCandidate    = "E_NO_CANDIDATE"
	CodeRoundConflict  = "E_ROUND_CONFLICT"
	CodeNotFound       = "E_NOT_FOUND"
	CodeInvalidPayload = "E_INVALID_PAYLOAD"
	CodeBudgetExceeded = "E_BUDGET_EXCEEDED"
)
