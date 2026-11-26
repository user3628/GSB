package errs

import (
	"errors"
	"fmt"
)

const (
	EShardInvalid    = "E_SHARD_INVALID"
	EBlockCorrupt    = "E_BLOCK_CORRUPT"
	EBlockUnbounded  = "E_BLOCK_UNBOUNDED"
	EDecodeQueueFull = "E_DECODE_QUEUE_FULL"
	ENumericOverflow = "E_NUMERIC_OVERFLOW"
	EConflictBatch   = "E_CONFLICTING_BATCH"
	EConflictQuery   = "E_CONFLICTING_QUERY"
	EInvalidRequest  = "E_INVALID_REQUEST"
	ENotFound        = "E_NOT_FOUND"
	EInternal        = "E_INTERNAL"
)

type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	BlockNo int            `json:"block_no,omitempty"`
	Offset  int64          `json:"offset,omitempty"`
	Window  string         `json:"window,omitempty"`
	Group   string         `json:"group,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	msg := fmt.Sprintf("%s: %s", e.Code, e.Message)
	if e.BlockNo >= 0 {
		msg += fmt.Sprintf(" (block=%d offset=%d)", e.BlockNo, e.Offset)
	}
	if e.Window != "" || e.Group != "" {
		msg += fmt.Sprintf(" (window=%s group=%s)", e.Window, e.Group)
	}
	return msg
}

func (e *Error) WithDetail(key string, value any) *Error {
	if e.Details == nil {
		e.Details = make(map[string]any)
	}
	e.Details[key] = value
	return e
}

func New(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), BlockNo: -1}
}

func BlockCorrupt(blockNo int, offset int64, format string, args ...any) *Error {
	return &Error{
		Code:    EBlockCorrupt,
		Message: fmt.Sprintf(format, args...),
		BlockNo: blockNo,
		Offset:  offset,
		Details: map[string]any{},
	}
}

func NumericOverflow(window string, group int64, format string, args ...any) *Error {
	return &Error{
		Code:    ENumericOverflow,
		Message: fmt.Sprintf(format, args...),
		Window:  window,
		Group:   fmt.Sprintf("%d", group),
		Details: map[string]any{"group_key": group},
	}
}

func As(err error) (*Error, bool) {
	var coded *Error
	if errors.As(err, &coded) {
		return coded, true
	}
	return nil, false
}

func Code(err error) string {
	if coded, ok := As(err); ok {
		return coded.Code
	}
	return ""
}
