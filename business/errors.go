// Package business defines the stable business codes and error envelope used by
// the sandbox policy workbench.
package business

import (
	"errors"
	"fmt"
)

// Code is a stable, machine-readable business result code.
type Code string

const (
	CodePolicyMalformed  Code = "E_POLICY_MALFORMED"
	CodeDepInvalidated   Code = "E_DEP_INVALIDATED"
	CodeDepRestricted    Code = "E_DEP_RESTRICTED"
	CodeProbeFailed      Code = "E_PROBE_FAILED"
	CodeProbeMismatch    Code = "E_PROBE_MISMATCH"
	CodeSnapshotVersion  Code = "E_SNAPSHOT_VERSION"
	CodeChangeContention Code = "E_CHANGE_CONTENTION"
	CodeChangeConflict   Code = "E_CHANGE_CONFLICT"
	CodeNamespaceScope   Code = "E_NAMESPACE_SCOPE"
	CodeBaselineMissing  Code = "E_BASELINE_MISSING"
	CodeInternal         Code = "E_INTERNAL"
)

// Detail reasons make malformed/injection failures unambiguous while keeping
// the top-level code compatible with the policy contract.
const (
	ReasonDuplicateIdentifier = "duplicate_identifier"
	ReasonReferenceMissing    = "reference_missing"
	ReasonDependencyCycle     = "dependency_cycle"
	ReasonHierarchyInvalid    = "hierarchy_invalid"
	ReasonLayerMismatch       = "layer_mismatch"
	ReasonInvalidField        = "invalid_field"
)

// Error is safe to marshal directly into diagnostics and CLI error output.
// Field order is intentional because JSON output must be deterministic.
type Error struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
	Entry   string `json:"entry,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Version int    `json:"version,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Entry != "" {
		return fmt.Sprintf("%s [%s]: %s", e.Code, e.Entry, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// New creates a business error with a stable code and message.
func New(code Code, message string) *Error {
	return &Error{Code: code, Message: message}
}

// At creates a business error tied to a specific policy item.
func At(code Code, entry, reason, message string) *Error {
	return &Error{Code: code, Entry: entry, Reason: reason, Message: message}
}

// From extracts a business error from an error chain. Unknown errors become
// E_INTERNAL so callers never need to special-case operational failures.
func From(err error) *Error {
	if err == nil {
		return nil
	}
	var be *Error
	if errors.As(err, &be) {
		return be
	}
	return &Error{Code: CodeInternal, Message: err.Error()}
}

// Wrap preserves a business error and wraps ordinary errors as E_INTERNAL.
func Wrap(err error) error {
	if err == nil {
		return nil
	}
	var be *Error
	if errors.As(err, &be) {
		return be
	}
	return &Error{Code: CodeInternal, Message: err.Error()}
}

// Codes returns a sorted, de-duplicated list of business codes.
func Codes(values ...Code) []Code {
	seen := make(map[Code]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	out := make([]Code, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sortCodes(out)
	return out
}

func sortCodes(codes []Code) {
	for i := 1; i < len(codes); i++ {
		for j := i; j > 0 && codes[j] < codes[j-1]; j-- {
			codes[j], codes[j-1] = codes[j-1], codes[j]
		}
	}
}
