package authquota

import (
	"errors"
	"fmt"
	"net/http"
)

// Stable business codes returned to OAuth clients and integration tests.
const (
	CodeInvalidGrant        = "E_INVALID_GRANT"
	CodePKCEMismatch        = "E_PKCE_MISMATCH"
	CodePrecisionOverflow   = "E_PRECISION_OVERFLOW"
	CodeTokenReuse          = "E_TOKEN_REUSE"
	CodeSnapshotVersion     = "E_SNAPSHOT_VERSION"
	CodeQuotaContention     = "E_QUOTA_CONTENTION"
	CodeQuotaExhausted      = "E_QUOTA_EXHAUSTED"
	CodeIdempotencyConflict = "E_IDEMPOTENCY_CONFLICT"
	CodeInvalidRequest      = "E_INVALID_REQUEST"
	CodeNotFound            = "E_NOT_FOUND"
	CodeInternal            = "E_INTERNAL"
)

// BusinessError is intentionally small and JSON-friendly. Hosts can map Code
// to their own transport status without depending on error strings.
type BusinessError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"http_status"`
	Cause      error  `json:"-"`
}

func (e *BusinessError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *BusinessError) Unwrap() error { return e.Cause }

func bizError(code, message string, cause error) *BusinessError {
	status := http.StatusBadRequest
	switch code {
	case CodeInternal:
		status = http.StatusInternalServerError
	case CodeNotFound:
		status = http.StatusNotFound
	case CodeQuotaContention:
		status = http.StatusTooManyRequests
	case CodeQuotaExhausted:
		status = http.StatusPaymentRequired
	case CodeTokenReuse:
		status = http.StatusUnauthorized
	case CodeInvalidGrant:
		status = http.StatusUnauthorized
	case CodeSnapshotVersion:
		status = http.StatusConflict
	}
	return &BusinessError{Code: code, Message: message, HTTPStatus: status, Cause: cause}
}

// ErrorCode returns a stable code for a BusinessError or E_INTERNAL for any
// other error. A nil error returns an empty string.
func ErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var target *BusinessError
	if errors.As(err, &target) {
		return target.Code
	}
	return CodeInternal
}

// IsCode reports whether err is a BusinessError with the requested code.
func IsCode(err error, code string) bool { return ErrorCode(err) == code }
