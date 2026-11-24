package apperr

import (
	"errors"
	"fmt"
)

const (
	SpecInvalid        = "E_SPEC_INVALID"
	RecordCorrupt      = "E_RECORD_CORRUPT"
	FormatUnsupported  = "E_FORMAT_UNSUPPORTED"
	MigrationAmbiguous = "E_MIGRATION_AMBIGUOUS"
	StaleBase          = "E_STALE_BASE"
	NotFound           = "E_NOT_FOUND"
	Internal           = "E_INTERNAL"
)

// Error is the common business error contract used by codecs, the store and HTTP.
type Error struct {
	Code        string         `json:"code"`
	Message     string         `json:"message"`
	RecordIndex int            `json:"record_index,omitempty"`
	Details     map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string {
	if e.RecordIndex > 0 {
		return fmt.Sprintf("%s: %s (record %d)", e.Code, e.Message, e.RecordIndex)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func New(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

func WithDetails(code, message string, details map[string]any) *Error {
	return &Error{Code: code, Message: message, Details: details}
}

func Record(code, message string, index int, details map[string]any) *Error {
	return &Error{Code: code, Message: message, RecordIndex: index, Details: details}
}

func As(err error) *Error {
	var target *Error
	if errors.As(err, &target) {
		return target
	}
	return &Error{Code: Internal, Message: err.Error()}
}
