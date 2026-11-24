package ledger

import (
	"errors"
	"fmt"
	"net/http"
)

const (
	CodeLedgerInvalid         = "E_LEDGER_INVALID"
	CodeSnapshotInvalid       = "E_SNAPSHOT_INVALID"
	CodeDigestMismatch        = "E_DIGEST_MISMATCH"
	CodeVersionGap            = "E_VERSION_GAP"
	CodeWeightImbalance       = "E_WEIGHT_IMBALANCE"
	CodeStaleBase             = "E_STALE_BASE"
	CodeMigrationRace         = "E_MIGRATION_RACE"
	CodeMigrationRatioInvalid = "E_MIGRATION_RATIO_INVALID"
	CodeStaleRead             = "E_STALE_READ"
	CodeProbeTimeout          = "E_PROBE_TIMEOUT"
	CodeProbeRefused          = "E_PROBE_REFUSED"
	CodeProbeMismatch         = "E_PROBE_MISMATCH"
	CodeProbeLate             = "E_PROBE_LATE_RESPONSE"
	CodeConflictingLedger     = "E_CONFLICTING_LEDGER"
	CodeNotFound              = "E_NOT_FOUND"
	CodeMethodNotAllowed      = "E_METHOD_NOT_ALLOWED"
	CodeInternal              = "E_INTERNAL"
	CodeRebuildFailed         = "E_REBUILD_FAILED"
)

// Error is the unified JSON business error used by the HTTP API and Go callers.
type Error struct {
	Code       string         `json:"code"`
	Message    string         `json:"message"`
	Context    map[string]any `json:"context,omitempty"`
	httpStatus int            `json:"-"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) HTTPStatus() int {
	if e == nil || e.httpStatus == 0 {
		return http.StatusInternalServerError
	}
	return e.httpStatus
}

func (e *Error) WithContext(key string, value any) *Error {
	if e.Context == nil {
		e.Context = map[string]any{}
	}
	e.Context[key] = value
	return e
}

func (e *Error) WithContextMap(ctx map[string]any) *Error {
	if e.Context == nil {
		e.Context = map[string]any{}
	}
	for k, v := range ctx {
		e.Context[k] = v
	}
	return e
}

func NewError(code, message string) *Error {
	return &Error{Code: code, Message: message, httpStatus: DefaultHTTPStatus(code)}
}

func NewErrorf(code, format string, args ...any) *Error {
	return NewError(code, fmt.Sprintf(format, args...))
}

func WrapError(code string, err error) *Error {
	if err == nil {
		return nil
	}
	var le *Error
	if errors.As(err, &le) {
		return le
	}
	return NewError(code, err.Error())
}

func AsError(err error) (*Error, bool) {
	var le *Error
	if errors.As(err, &le) {
		return le, true
	}
	return nil, false
}

func DefaultHTTPStatus(code string) int {
	switch code {
	case CodeLedgerInvalid, CodeSnapshotInvalid, CodeDigestMismatch,
		CodeMigrationRatioInvalid, CodeProbeMismatch:
		return http.StatusUnprocessableEntity
	case CodeWeightImbalance, CodeStaleBase, CodeMigrationRace,
		CodeStaleRead, CodeConflictingLedger, CodeVersionGap, CodeProbeLate:
		return http.StatusConflict
	case CodeProbeTimeout:
		return http.StatusGatewayTimeout
	case CodeProbeRefused:
		return http.StatusBadGateway
	case CodeNotFound:
		return http.StatusNotFound
	case CodeMethodNotAllowed:
		return http.StatusMethodNotAllowed
	default:
		return http.StatusInternalServerError
	}
}
