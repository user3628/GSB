package authlab

import (
	"errors"
	"fmt"
)

// Code is a stable business error code returned by the authentication engine.
type Code string

const (
	CodeTokenInvalid        Code = "E_TOKEN_INVALID"
	CodeCrossTenant         Code = "E_CROSS_TENANT"
	CodeSnapshotVersion     Code = "E_SNAPSHOT_VERSION"
	CodeTokenReuse          Code = "E_TOKEN_REUSE"
	CodeSessionFixation     Code = "E_SESSION_FIXATION"
	CodeAuthContention      Code = "E_AUTH_CONTENTION"
	CodePrivilegeEscalation Code = "E_PRIVILEGE_ESCALATION"
	CodeAuthInvalid         Code = "E_AUTH_INVALID"
	CodeNotFound            Code = "E_NOT_FOUND"
	CodeInternal            Code = "E_INTERNAL"
)

// Error carries a stable code plus an internal operation/detail pair. Detail is
// intentionally omitted from Error() so callers cannot accidentally leak
// whether a tenant, user, token, or session exists.
type Error struct {
	Code   Code
	Op     string
	Detail string
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Op == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Op)
}

func newError(code Code, op, detail string) *Error {
	return &Error{Code: code, Op: op, Detail: detail}
}

// CodeOf returns the stable business code for err. Infrastructure errors map to
// E_INTERNAL; a nil error maps to "OK".
func CodeOf(err error) Code {
	if err == nil {
		return "OK"
	}
	var ae *Error
	if errors.As(err, &ae) {
		return ae.Code
	}
	return CodeInternal
}

// IsCode reports whether err is a stable business error with code.
func IsCode(err error, code Code) bool {
	return CodeOf(err) == code
}

var (
	// ErrNotFound is used by storage adapters when a tenant-scoped record does
	// not exist. The engine maps it to an endpoint-specific non-enumerating code.
	ErrNotFound = newError(CodeNotFound, "storage", "record not found")
)
