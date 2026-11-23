// Package arbiter implements row-level exclusive write leases with TTL and
// versioned read snapshots on top of the SQLite store. It deliberately does
// not provide cross-statement MVCC transactions: writers serialize per row via
// leases while readers lock no rows and pin a committed snapshot version.
package arbiter

// ErrorCode is the stable wire identifier of every engine-level failure.
type ErrorCode string

const (
	CodeBadRequest         ErrorCode = "E_BAD_REQUEST"
	CodeSnapshotAhead      ErrorCode = "E_SNAPSHOT_AHEAD"
	CodeVersionRetired     ErrorCode = "E_VERSION_RETIRED"
	CodeNotFound           ErrorCode = "E_NOT_FOUND"
	CodeRowDeleted         ErrorCode = "E_ROW_DELETED"
	CodeLeaseHeld          ErrorCode = "E_WRITE_LEASE_HELD"
	CodeLeaseStale         ErrorCode = "E_LEASE_STALE"
	CodeLeaseLost          ErrorCode = "E_LEASE_LOST"
	CodeEditStale          ErrorCode = "E_EDIT_STALE"
	CodeLeaseTimeout       ErrorCode = "E_LEASE_TIMEOUT"
	CodeConflictingRequest ErrorCode = "E_CONFLICTING_REQUEST"
	CodePrecheckIncomplete ErrorCode = "E_PRECHECK_INCOMPLETE"
)

// APIError carries a stable code plus machine-readable detail fields so
// clients can reconcile fast-path and full-path conclusions field by field.
type APIError struct {
	Code    ErrorCode
	Message string
	Details map[string]any
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	return string(e.Code) + ": " + e.Message
}

func apiError(code ErrorCode, msg string, details map[string]any) *APIError {
	return &APIError{Code: code, Message: msg, Details: details}
}
