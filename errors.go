package rewardpack

import (
	"errors"
	"fmt"
)

// Stable business error codes exposed by the component.
const (
	CodeEmptyText           = "E_EMPTY_TEXT"
	CodeNumeric             = "E_NUMERIC"
	CodeDimMismatch         = "E_DIM_MISMATCH"
	CodeStaleCheckpoint     = "E_STALE_CHECKPOINT"
	CodeVersionRollback     = "E_VERSION_ROLLBACK"
	CodeVersionConflict     = "E_VERSION_CONFLICT"
	CodeIncrementalMismatch = "E_INCREMENTAL_MISMATCH"
	CodeIllegalStage        = "E_ILLEGAL_STAGE"
	CodeMissingArtifact     = "E_MISSING_ARTIFACT"
	CodeInvalidConfig       = "E_INVALID_CONFIG"
	CodeInvalidInput        = "E_INVALID_INPUT"
	CodeStopped             = "E_STOPPED"
	CodeLockTimeout         = "E_LOCK_TIMEOUT"
	CodeInternal            = "E_INTERNAL"
)

// BusinessError carries a stable machine-readable code plus deterministic
// diagnostic context. JSON marshaling is intentionally stable so callers can
// compare errors without relying on message wording.
type BusinessError struct {
	Code     string `json:"code"`
	PairID   string `json:"pair_id,omitempty"`
	Stage    string `json:"stage,omitempty"`
	Field    string `json:"field,omitempty"`
	Factor   string `json:"factor,omitempty"`
	Row      int    `json:"row,omitempty"`
	Column   int    `json:"column,omitempty"`
	Expected any    `json:"expected,omitempty"`
	Actual   any    `json:"actual,omitempty"`
	Context  string `json:"context,omitempty"`
	Message  string `json:"message"`
}

func (e *BusinessError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// CodeOf returns the stable business error code, if err contains one.
func CodeOf(err error) string {
	var be *BusinessError
	if errors.As(err, &be) {
		return be.Code
	}
	return ""
}

// HasCode reports whether err contains a BusinessError with code.
func HasCode(err error, code string) bool { return CodeOf(err) == code }

func errorWith(code string, err error) error {
	if err == nil {
		return nil
	}
	if be := (*BusinessError)(nil); errors.As(err, &be) {
		return be
	}
	return &BusinessError{Code: code, Message: err.Error()}
}
