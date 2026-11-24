package model

import (
	"errors"
	"fmt"
)

// Code is a stable, machine-readable lifecycle or adjudication result.
type Code string

const (
	CodeOK                 Code = "OK"
	CodeSubjectScope       Code = "E_SUBJECT_SCOPE"
	CodeKeyNotFound        Code = "E_KEY_NOT_FOUND"
	CodeWeakAlgo           Code = "E_WEAK_ALGO"
	CodeKeyExpired         Code = "E_KEY_EXPIRED"
	CodePhaseTransition    Code = "E_PHASE_TRANSITION"
	CodeRotationContention Code = "E_ROTATION_CONTENTION"
	CodeKeyBudget          Code = "E_KEY_BUDGET"
	CodeSnapshotVersion    Code = "E_SNAPSHOT_VERSION"
	CodeSnapshotReplaced   Code = "E_SNAPSHOT_REPLACED"
	CodeSnapshotIntegrity  Code = "E_SNAPSHOT_INTEGRITY"
	CodeDuplicateKey       Code = "E_DUPLICATE_KEY"
	CodePolicyNotFound     Code = "E_POLICY_NOT_FOUND"
	CodeConfig             Code = "E_CONFIG"
)

// BizError carries the stable code that is also emitted in reports.
type BizError struct {
	Code             Code
	Message          string
	RecoveredThrough int64
}

func (e *BizError) Error() string {
	if e.Message == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code Code, format string, args ...any) error {
	return &BizError{Code: code, Message: fmt.Sprintf(format, args...)}
}

func CodeOf(err error) Code {
	if err == nil {
		return CodeOK
	}
	var biz *BizError
	if errors.As(err, &biz) {
		return biz.Code
	}
	return CodeConfig
}

func RecoveredThrough(err error) (int64, bool) {
	var biz *BizError
	if errors.As(err, &biz) && biz.RecoveredThrough >= 0 {
		return biz.RecoveredThrough, true
	}
	return 0, false
}
