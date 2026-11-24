package abeval

import (
	"encoding/json"
	"errors"
)

const (
	CodeInvalidConfig   = "E_INVALID_CONFIG"
	CodeInvalidEvent    = "E_INVALID_EVENT"
	CodeStageCoverage   = "E_STAGE_COVERAGE"
	CodeStaleCheckpoint = "E_STALE_CHECKPOINT"
	CodeVersionRollback = "E_VERSION_ROLLBACK"
	CodeNotFound        = "E_NOT_FOUND"
	CodeConflict        = "E_CONFLICT"
	CodeBusy            = "E_BUSY"
)

var (
	ErrGracefulStop = errors.New("graceful stop requested after checkpoint")
	ErrCompleted    = errors.New("batch already completed")
)

type Error struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Stage     string         `json:"stage,omitempty"`
	EventID   string         `json:"event_id,omitempty"`
	Field     string         `json:"field,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
	Retryable bool           `json:"retryable,omitempty"`
}

func (e *Error) Error() string {
	if data, err := json.Marshal(e); err == nil {
		return string(data)
	}
	return e.Code + ": " + e.Message
}

func NewError(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

func (e *Error) WithStage(stage string) *Error {
	e.Stage = stage
	return e
}

func (e *Error) WithEventID(eventID string) *Error {
	e.EventID = eventID
	return e
}

func (e *Error) WithField(field string) *Error {
	e.Field = field
	return e
}

func (e *Error) WithDetail(key string, value any) *Error {
	if e.Details == nil {
		e.Details = make(map[string]any)
	}
	e.Details[key] = value
	return e
}

func AsError(err error) (*Error, bool) {
	var businessError *Error
	if errors.As(err, &businessError) {
		return businessError, true
	}
	return nil, false
}

func ErrorCode(err error) string {
	if businessError, ok := AsError(err); ok {
		return businessError.Code
	}
	return ""
}
