package truststore

import (
	"errors"
	"fmt"
)

// Code is the stable, machine-readable outcome of an operation or a chain
// validation. Host integrations should branch on these values rather than on
// error strings.
type Code string

const (
	CodeOK                    Code = "OK"
	CodeChainBroken           Code = "E_CHAIN_BROKEN"
	CodeSelfSignedNonRoot     Code = "E_SELF_SIGNED_NONROOT"
	CodePathLenExceeded       Code = "E_PATH_LEN_EXCEEDED"
	CodeSignatureInvalid      Code = "E_SIGNATURE_INVALID"
	CodeCertificateExpired    Code = "E_CERT_EXPIRED"
	CodeCertificateNotYet     Code = "E_CERT_NOT_YET_VALID"
	CodeTrustInvalid          Code = "E_TRUST_INVALID"
	CodeDependencyInvalidated Code = "E_DEP_INVALIDATED"
	CodeSnapshotVersion       Code = "E_SNAPSHOT_VERSION"
	CodeTrustContention       Code = "E_TRUST_CONTENTION"
	CodeScopeDenied           Code = "E_SCOPE_DENIED"
	CodeInputInvalid          Code = "E_INPUT_INVALID"
	CodeNotFound              Code = "E_NOT_FOUND"
	CodeStoreFailure          Code = "E_STORE_FAILURE"
)

// Error carries a stable business code together with the underlying cause.
type Error struct {
	Code    Code
	Op      string
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	prefix := string(e.Code)
	if e.Op != "" {
		prefix += " " + e.Op
	}
	if e.Message != "" {
		prefix += ": " + e.Message
	}
	if e.Err != nil {
		prefix += ": " + e.Err.Error()
	}
	return prefix
}

func (e *Error) Unwrap() error { return e.Err }

// NewError constructs a coded domain error.
func NewError(code Code, op, message string, cause error) *Error {
	return &Error{Code: code, Op: op, Message: message, Err: cause}
}

// CodeOf returns the stable code carried by err. Uncoded errors map to
// E_STORE_FAILURE so callers never need to inspect error strings.
func CodeOf(err error) Code {
	if err == nil {
		return CodeOK
	}
	var domainErr *Error
	if errors.As(err, &domainErr) {
		return domainErr.Code
	}
	return CodeStoreFailure
}

// HasCode reports whether err has the supplied business code.
func HasCode(err error, code Code) bool { return CodeOf(err) == code }

func invalid(op, format string, args ...any) error {
	return NewError(CodeTrustInvalid, op, fmt.Sprintf(format, args...), nil)
}
