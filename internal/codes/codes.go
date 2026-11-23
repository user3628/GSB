package codes

import "fmt"

const (
	OK                 = "E_OK"
	IllegalTransition  = "E_ILLEGAL_TRANSITION"
	BadChecksum        = "E_BAD_CHECKSUM"
	PayloadOverflow    = "E_PAYLOAD_OVERFLOW"
	DepInvalidated     = "E_DEP_INVALIDATED"
	DepCycle           = "E_DEP_CYCLE"
	SnapshotVersion    = "E_SNAPSHOT_VERSION"
	CaseBudget         = "E_CASE_BUDGET"
	BadRequest         = "E_BAD_REQUEST"
	TruncatedFrame     = "E_TRUNCATED_FRAME"
	ConnectionError    = "E_CONNECTION"
	AssertionMismatch  = "E_ASSERTION_MISMATCH"
	CasesetVersion     = "E_CASESET_VERSION"
	InjectedChecksum   = BadChecksum
	TargetResponse     = "E_TARGET_RESPONSE"
	ReplayNotCommitted = "E_REPLAY_NOT_COMMITTED"
)

// Error is the stable error envelope used by the CLI, report, and diagnostics.
type Error struct {
	Code            string   `json:"code"`
	Message         string   `json:"message"`
	FallbackVersion int      `json:"fallback_version,omitempty"`
	Chain           []string `json:"root_chain,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return e.Code
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func New(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func From(err error) *Error {
	if err == nil {
		return nil
	}
	if coded, ok := err.(*Error); ok {
		return coded
	}
	return &Error{Code: BadRequest, Message: err.Error()}
}
