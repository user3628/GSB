package model

import "fmt"

const (
	CodeEmptyInput             = "E_EMPTY_INPUT"
	CodeInvalidConfig          = "E_INVALID_CONFIG"
	CodeInvalidWeight          = "E_INVALID_WEIGHT"
	CodeIllegalStageTransition = "E_ILLEGAL_STAGE_TRANSITION"
	CodeMissingPrerequisite    = "E_MISSING_PREREQUISITE"
	CodeStaleCheckpoint        = "E_STALE_CHECKPOINT"
	CodeIncrementalMismatch    = "E_INCREMENTAL_MISMATCH"
	CodeWorkerFailure          = "E_WORKER_FAILURE"
	CodeBudgetExhausted        = "E_BUDGET_EXHAUSTED"
	CodeDataLoad               = "E_DATA_LOAD"
	CodeHTTP                   = "E_HTTP"
	CodeInternal               = "E_INTERNAL"
)

type AppError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	QueryID string         `json:"query_id,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *AppError) Error() string {
	if e.QueryID != "" {
		return fmt.Sprintf("%s (%s): %s", e.Code, e.QueryID, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code, message, queryID string) *AppError {
	return &AppError{Code: code, Message: message, QueryID: queryID}
}

func NewErrorDetails(code, message, queryID string, details map[string]any) *AppError {
	return &AppError{Code: code, Message: message, QueryID: queryID, Details: details}
}

func AsAppError(err error) *AppError {
	if err == nil {
		return nil
	}
	if appErr, ok := err.(*AppError); ok {
		return appErr
	}
	if workerErr, ok := err.(*WorkerFailure); ok {
		return workerErr.AppError()
	}
	return NewError(CodeInternal, err.Error(), "")
}

type WorkerFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Shard   int    `json:"shard"`
	QueryID string `json:"query_id"`
}

func (e *WorkerFailure) Error() string {
	return fmt.Sprintf("%s shard=%d query=%s: %s", e.Code, e.Shard, e.QueryID, e.Message)
}

func (e *WorkerFailure) AppError() *AppError {
	return NewErrorDetails(e.Code, e.Message, e.QueryID, map[string]any{"shard": e.Shard})
}
