// Package gwerr 定义网关统一业务错误码与错误结构。
package gwerr

import "fmt"

// 业务错误码
const (
	ERecordCorrupt      = "E_RECORD_CORRUPT"
	EFormatUnsupported  = "E_FORMAT_UNSUPPORTED"
	EUnitOverflow       = "E_UNIT_OVERFLOW"
	EConflictingImport  = "E_CONFLICTING_IMPORT"
	EBackendUnavailable = "E_BACKEND_UNAVAILABLE"
	ESnapshotInvalid    = "E_SNAPSHOT_INVALID"
	EModelNotFound      = "E_MODEL_NOT_FOUND"
	EBadRequest         = "E_BAD_REQUEST"
	EInternal           = "E_INTERNAL"
)

// Error 是带业务码与上下文的统一错误。
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// New 构造业务错误。
func New(code, msg string, ctx map[string]any) *Error {
	return &Error{Code: code, Message: msg, Context: ctx}
}

// Corrupt 构造 E_RECORD_CORRUPT 错误并指明偏移。
func Corrupt(offset int64, msg string) *Error {
	return New(ERecordCorrupt, msg, map[string]any{"offset": offset})
}
