package engine

import "fmt"

// Code is a stable business result code.
type Code string

const (
	CodeOK                Code = "OK"
	CodeDeviceInvalid     Code = "E_DEVICE_INVALID"
	CodeScoreOverflow     Code = "E_SCORE_OVERFLOW"
	CodeCredSignature     Code = "E_CRED_SIGNATURE"
	CodeCredExpired       Code = "E_CRED_EXPIRED"
	CodeCapabilityDenied  Code = "E_CAPABILITY_DENIED"
	CodeScoreRegressed    Code = "E_SCORE_REGRESSED"
	CodeTrustInsufficient Code = "E_TRUST_INSUFFICIENT"
	CodeDeviceBudget      Code = "E_DEVICE_BUDGET"
	CodeGlobalBudget      Code = "E_GLOBAL_BUDGET_EXHAUSTED"
	CodeSnapshotVersion   Code = "E_SNAPSHOT_VERSION"
	CodeAuthContention    Code = "E_AUTH_CONTENTION"
)

// Error carries a business code plus a human-readable message.
type Error struct {
	Code Code
	Msg  string
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Msg }

func errf(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}
