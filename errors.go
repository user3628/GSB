package mfa

import (
	"errors"
	"fmt"
)

type Code string

const (
	CodeOK               Code = "OK"
	CodePolicyMalformed  Code = "E_POLICY_MALFORMED"
	CodeChallengeFailed  Code = "E_CHALLENGE_FAILED"
	CodeConsumed         Code = "E_CODE_CONSUMED"
	CodeNoViableFactor   Code = "E_NO_VIABLE_FACTOR"
	CodeRebindContention Code = "E_REBIND_CONTENTION"
	CodeSnapshotVersion  Code = "E_SNAPSHOT_VERSION"
	CodeAuthBudget       Code = "E_AUTH_BUDGET"
	CodeInternal         Code = "E_INTERNAL"
)

type BizError struct {
	Code   Code
	Reason string
	Field  string
}

func (e *BizError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s: %s (%s)", e.Code, e.Reason, e.Field)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Reason)
}

func policyError(field, reason string) *BizError {
	return &BizError{Code: CodePolicyMalformed, Reason: reason, Field: field}
}

func bizError(code Code, reason string) *BizError {
	return &BizError{Code: code, Reason: reason}
}

func ErrorCode(err error) Code {
	if err == nil {
		return CodeOK
	}
	var biz *BizError
	if errors.As(err, &biz) {
		return biz.Code
	}
	return CodeInternal
}

func ErrorReason(err error) string {
	if err == nil {
		return ""
	}
	var biz *BizError
	if errors.As(err, &biz) {
		return biz.Reason
	}
	return err.Error()
}
