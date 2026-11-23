package engine

import (
	"encoding/json"
	"net/http"
	"sort"
)

const (
	CodeFormatCorrupt       = "E_FORMAT_CORRUPT"
	CodeFormatUnsupported   = "E_FORMAT_UNSUPPORTED"
	CodeNormalizeOverflow   = "E_NORMALIZE_OVERFLOW"
	CodeInvalidPolicy       = "E_INVALID_POLICY"
	CodeInvalidTruncation   = "E_INVALID_TRUNCATION_RULE"
	CodeConflictingRequest  = "E_CONFLICTING_REQUEST"
	CodeVersionNotMonotonic = "E_VERSION_NOT_MONOTONIC"
	CodeVersionRetired      = "E_VERSION_RETIRED"
	CodeBatchRejected       = "E_BATCH_REJECTED"
	CodeImportAborted       = "E_IMPORT_ABORTED"
	CodeInvalidRequest      = "E_INVALID_REQUEST"
	CodeNotFound            = "E_NOT_FOUND"
	CodeInternal            = "E_INTERNAL"
)

// APIError is both an internal error and the deterministic public error body.
type APIError struct {
	Code          string   `json:"code"`
	Message       string   `json:"message"`
	Status        int      `json:"-"`
	ByteOffset    *int64   `json:"byteOffset,omitempty"`
	Line          int      `json:"line,omitempty"`
	UnknownFields []string `json:"unknownFields,omitempty"`
	ReportID      string   `json:"reportId,omitempty"`
	Details       any      `json:"details,omitempty"`
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	return e.Code + ": " + e.Message
}

func (e *APIError) Envelope() []byte {
	if e.UnknownFields != nil {
		sort.Strings(e.UnknownFields)
	}
	b, _ := json.Marshal(struct {
		Error *APIError `json:"error"`
	}{Error: e})
	return b
}

func newError(status int, code, message string) *APIError {
	return &APIError{Code: code, Message: message, Status: status}
}

func badRequest(code, message string) *APIError {
	return newError(http.StatusBadRequest, code, message)
}

func unsupported(message string, unknown []string) *APIError {
	e := newError(http.StatusUnprocessableEntity, CodeFormatUnsupported, message)
	e.UnknownFields = unknown
	return e
}
