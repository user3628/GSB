// Package cerr defines business error codes shared across the inspection
// components. Every failure surfaced by the API carries one of these codes
// plus a structured context map.
package cerr

import "encoding/json"

// Business error codes.
const (
	ManifestInvalid    = "E_MANIFEST_INVALID"
	PageMismatch       = "E_PAGE_MISMATCH"
	NoQuorum           = "E_NO_QUORUM"
	ConflictingRequest = "E_CONFLICTING_REQUEST"
	RecoveryIncomplete = "E_RECOVERY_INCOMPLETE"
	NotFound           = "E_NOT_FOUND"
	CheckpointNotFound = "E_CHECKPOINT_NOT_FOUND"
	BadRequest         = "E_BAD_REQUEST"
	Internal           = "E_INTERNAL"
)

// Error is a business error with a stable machine-readable code and a
// structured context bag (expected/actual lengths, vote counts, ...).
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *Error) Error() string {
	b, _ := json.Marshal(e)
	return string(b)
}

// New builds an *Error. Context entries are supplied as key/value pairs.
func New(code, msg string, kv ...any) *Error {
	e := &Error{Code: code, Message: msg}
	if len(kv) > 0 {
		e.Context = map[string]any{}
		for i := 0; i+1 < len(kv); i += 2 {
			key, _ := kv[i].(string)
			e.Context[key] = kv[i+1]
		}
	}
	return e
}

// CodeOf extracts the business code of an error, defaulting to E_INTERNAL.
func CodeOf(err error) string {
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return Internal
}
