package fixedpoint

import "fmt"

// Code is the stable machine-readable error code used across the service.
type Code string

const (
	ESpecInvalid        Code = "E_SPEC_INVALID"
	EUnitInvalid        Code = "E_UNIT_INVALID"
	ENumericOverflow    Code = "E_NUMERIC_OVERFLOW"
	EPriorityConflict   Code = "E_PRIORITY_CONFLICT"
	EErrorBoundExceeded Code = "E_ERROR_BOUND_EXCEEDED"
	ESnapshotInvalid    Code = "E_SNAPSHOT_INVALID"
	EStaleBase          Code = "E_STALE_BASE"
	EConflictingRequest Code = "E_CONFLICTING_REQUEST"
	ENotFound           Code = "E_NOT_FOUND"
	EInvalidInput       Code = "E_INVALID_INPUT"
)

// Error carries a stable code plus contextual fields for callers and HTTP layer.
type Error struct {
	Code    Code
	Message string
	Field   string
	Line    int
	Step    int
	ItemID  string
	Extra   map[string]string
}

func (e *Error) Error() string {
	s := string(e.Code) + ": " + e.Message
	if e.Field != "" {
		s += fmt.Sprintf(" (field=%s)", e.Field)
	}
	if e.Line > 0 {
		s += fmt.Sprintf(" (line=%d)", e.Line)
	}
	if e.Step > 0 {
		s += fmt.Sprintf(" (step=%d)", e.Step)
	}
	return s
}

// NewError builds a coded error.
func NewError(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// WithField annotates the offending field.
func (e *Error) WithField(field string) *Error { e.Field = field; return e }

// WithLine annotates the 1-based input line number.
func (e *Error) WithLine(line int) *Error { e.Line = line; return e }

// WithStep annotates the conversion-chain step (1-based).
func (e *Error) WithStep(step int) *Error { e.Step = step; return e }

// WithItem annotates the audited item identifier.
func (e *Error) WithItem(item string) *Error { e.ItemID = item; return e }

// CodeOf extracts the stable code from any error, defaulting to E_INVALID_INPUT.
func CodeOf(err error) Code {
	if err == nil {
		return ""
	}
	if ce, ok := err.(*Error); ok {
		return ce.Code
	}
	return EInvalidInput
}
