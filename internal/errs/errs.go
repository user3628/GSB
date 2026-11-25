package errs

import (
	"errors"
	"fmt"
	"sort"
)

const (
	CodeSnapshotInvalid     = "E_SNAPSHOT_INVALID"
	CodePhysicalTableMiss   = "E_PHYSICAL_TABLE_MISSING"
	CodeRangeOverlap        = "E_RANGE_OVERLAP"
	CodeStaleBase           = "E_STALE_BASE"
	CodeObservedAhead       = "E_OBSERVED_AHEAD"
	CodeRebuildInterrupted  = "E_REBUILD_INTERRUPTED"
	CodeStaleRead           = "E_STALE_READ"
	CodeConflictingRequest  = "E_CONFLICTING_REQUEST"
	CodeVersionNotMonotonic = "E_VERSION_NOT_MONOTONIC"
	CodeTableNotFound       = "E_TABLE_NOT_FOUND"
	CodeQueryInvalid        = "E_QUERY_INVALID"
	CodeInvalidJSON         = "E_INVALID_JSON"
	CodeNotFound            = "E_NOT_FOUND"
	CodeInternal            = "E_INTERNAL"
	CodeCacheInconsistent   = "E_CACHE_INCONSISTENT"
)

// Error is the stable business-error envelope used by both storage and HTTP.
type Error struct {
	Code     string         `json:"code"`
	Category string         `json:"category,omitempty"`
	Message  string         `json:"message"`
	Context  map[string]any `json:"context,omitempty"`
}

func (e *Error) Error() string {
	if e.Category == "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s (%s): %s", e.Code, e.Category, e.Message)
}

func New(code, message string, context map[string]any) *Error {
	return &Error{Code: code, Message: message, Context: context}
}

func Snapshot(code, message string, context map[string]any) *Error {
	return &Error{
		Code:     code,
		Category: CodeSnapshotInvalid,
		Message:  message,
		Context:  context,
	}
}

func As(err error) *Error {
	var target *Error
	if errors.As(err, &target) {
		return target
	}
	return nil
}

// SortedCopy returns deterministic context keys for logs and tests.
func SortedCopy(in map[string]any) []string {
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
