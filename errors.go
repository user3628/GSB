package tokenforge

import (
	"errors"
	"fmt"
)

// Code is the stable business code returned by the domain component.
type Code string

const (
	CodeOK                Code = "OK"
	CodeTokenInvalid      Code = "E_TOKEN_INVALID"
	CodeTokenExpired      Code = "E_TOKEN_EXPIRED"
	CodeTokenSequence     Code = "E_TOKEN_SEQUENCE"
	CodeTokenReuse        Code = "E_TOKEN_REUSE"
	CodeTokenRevoked      Code = "E_TOKEN_REVOKED"
	CodeTokenConsumed     Code = "E_TOKEN_CONSUMED"
	CodeQuotaOverflow     Code = "E_QUOTA_OVERFLOW"
	CodeQuotaExhausted    Code = "E_QUOTA_EXHAUSTED"
	CodeQuotaContention   Code = "E_QUOTA_CONTENTION"
	CodeSnapshotVersion   Code = "E_SNAPSHOT_VERSION"
	CodeSnapshotIntegrity Code = "E_SNAPSHOT_INTEGRITY"
	CodeTenantNotFound    Code = "E_TENANT_NOT_FOUND"
	CodeTenantExists      Code = "E_TENANT_EXISTS"
	CodeInvalidArgument   Code = "E_INVALID_ARGUMENT"
	CodeCancelled         Code = "E_CANCELLED"
	CodeAborted           Code = "E_ABORTED"
	CodeInternal          Code = "E_INTERNAL"
)

// DomainError carries a stable business code alongside an optional cause.
type DomainError struct {
	Code    Code
	Message string
	Cause   error
}

func (e *DomainError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *DomainError) Unwrap() error { return e.Cause }

func domainError(code Code, message string, cause error) error {
	return &DomainError{Code: code, Message: message, Cause: cause}
}

// CodeOf extracts the stable business code from an error. Successful calls use
// CodeOK. Unknown errors are mapped to E_INTERNAL so hosts never have to parse
// error strings.
func CodeOf(err error) Code {
	if err == nil {
		return CodeOK
	}
	var de *DomainError
	if errors.As(err, &de) {
		return de.Code
	}
	return CodeInternal
}

func codeError(code Code, message string) error {
	return &DomainError{Code: code, Message: message}
}
