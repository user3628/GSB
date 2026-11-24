package domain

import "net/http"

const (
	CodeRecordInvalid        = "E_RECORD_INVALID"
	CodeLabelInvalid         = "E_LABEL_INVALID"
	CodeShardKeyMissing      = "E_SHARD_KEY_MISSING"
	CodeSampleRatioInvalid   = "E_SAMPLE_RATIO_INVALID"
	CodeIDConflict           = "E_ID_CONFLICT"
	CodeStageInterrupted     = "E_STAGE_INTERRUPTED"
	CodeStaleSnapshot        = "E_STALE_SNAPSHOT"
	CodeStaleBase            = "E_STALE_BASE"
	CodeShardDigestMismatch  = "E_SHARD_DIGEST_MISMATCH"
	CodePolicyDigestMismatch = "E_POLICY_DIGEST_MISMATCH"
	CodePolicyNotFound       = "E_POLICY_NOT_FOUND"
	CodeBatchNotFound        = "E_BATCH_NOT_FOUND"
	CodeBatchRunning         = "E_BATCH_RUNNING"
	CodeBatchAborted         = "E_BATCH_ABORTED"
	CodeConflict             = "E_CONFLICT"
	CodeInternal             = "E_INTERNAL"
	CodeRecoveryInProgress   = "E_RECOVERY_IN_PROGRESS"
)

type BusinessError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"-"`
	Details    any    `json:"details,omitempty"`
}

func (e *BusinessError) Error() string {
	return e.Code + ": " + e.Message
}

func NewError(code, message string, details ...any) *BusinessError {
	status := http.StatusUnprocessableEntity
	switch code {
	case CodeLabelInvalid, CodeShardKeyMissing, CodeSampleRatioInvalid,
		CodeRecordInvalid, CodePolicyDigestMismatch:
		status = http.StatusBadRequest
	case CodeStaleSnapshot, CodeStaleBase, CodeIDConflict, CodeConflict:
		status = http.StatusConflict
	case CodeStageInterrupted, CodeBatchRunning, CodeRecoveryInProgress:
		status = http.StatusConflict
	case CodeShardDigestMismatch:
		status = http.StatusInternalServerError
	case CodePolicyNotFound, CodeBatchNotFound:
		status = http.StatusNotFound
	case CodeBatchAborted:
		status = http.StatusGone
	case CodeInternal:
		status = http.StatusInternalServerError
	}
	berr := &BusinessError{Code: code, Message: message, HTTPStatus: status}
	if len(details) > 0 {
		berr.Details = details[0]
	}
	return berr
}

func AsBusinessError(err error) *BusinessError {
	if err == nil {
		return nil
	}
	if berr, ok := err.(*BusinessError); ok {
		return berr
	}
	return nil
}
