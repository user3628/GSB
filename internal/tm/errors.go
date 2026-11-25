package tm

import "fmt"

type BizError struct {
	Code       string         `json:"code"`
	Message    string         `json:"message"`
	SegmentID  string         `json:"segment_id,omitempty"`
	Stage      string         `json:"stage,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
	HTTPStatus int            `json:"-"`
}

func (e *BizError) Error() string {
	if e.SegmentID != "" {
		return fmt.Sprintf("%s: segment=%s stage=%s: %s", e.Code, e.SegmentID, e.Stage, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *BizError) WithDetail(key string, value any) *BizError {
	if e.Details == nil {
		e.Details = map[string]any{}
	}
	e.Details[key] = value
	return e
}

func AsBizError(err error) (*BizError, bool) {
	for err != nil {
		if be, ok := err.(*BizError); ok {
			return be, true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return nil, false
		}
		err = u.Unwrap()
	}
	return nil, false
}

func NewSegmentEmpty(segmentID, stage, side string) *BizError {
	return &BizError{
		Code:       "E_SEGMENT_EMPTY",
		Message:    "segment contains no translatable sentence after normalization",
		SegmentID:  segmentID,
		Stage:      stage,
		HTTPStatus: 422,
		Details:    map[string]any{"side": side},
	}
}

func NewAlignInvalid(segmentID string, sourceOffset, targetOffset [2]int, reason string) *BizError {
	return &BizError{
		Code:       "E_ALIGN_INVALID",
		Message:    "alignment score is invalid",
		SegmentID:  segmentID,
		Stage:      "alignment",
		HTTPStatus: 422,
		Details: map[string]any{
			"source_offset": sourceOffset,
			"target_offset": targetOffset,
			"reason":        reason,
		},
	}
}

func NewStaleBase(currentVersion, baseVersion int64) *BizError {
	return &BizError{
		Code:       "E_STALE_BASE",
		Message:    "increment base version does not match effective version",
		Stage:      "version",
		HTTPStatus: 409,
		Details: map[string]any{
			"current_version": currentVersion,
			"base_version":    baseVersion,
		},
	}
}

func NewRollback(rejectedVersion, currentVersion int64) *BizError {
	return &BizError{
		Code:       "E_VERSION_ROLLBACK",
		Message:    "version rollback is rejected",
		Stage:      "version",
		HTTPStatus: 409,
		Details: map[string]any{
			"rejected_version": rejectedVersion,
			"current_version":  currentVersion,
		},
	}
}

func NewStaleCheckpoint(submittedSeq, currentSeq int64, reason string) *BizError {
	return &BizError{
		Code:       "E_STALE_CHECKPOINT",
		Message:    "publication sequence is stale or not continuous",
		Stage:      "publish",
		HTTPStatus: 409,
		Details: map[string]any{
			"submitted_seq": submittedSeq,
			"current_seq":   currentSeq,
			"reason":        reason,
		},
	}
}

func NewStageJump(segmentID, fromStage, toStage string) *BizError {
	return &BizError{
		Code:       "E_INVALID_STAGE_TRANSITION",
		Message:    "illegal pipeline stage jump",
		SegmentID:  segmentID,
		Stage:      toStage,
		HTTPStatus: 409,
		Details:    map[string]any{"from_stage": fromStage, "to_stage": toStage},
	}
}

func NewMissingPrerequisite(segmentID, stage, prerequisite string) *BizError {
	return &BizError{
		Code:       "E_MISSING_PREREQUISITE",
		Message:    "missing prerequisite stage artifact",
		SegmentID:  segmentID,
		Stage:      stage,
		HTTPStatus: 409,
		Details:    map[string]any{"prerequisite": prerequisite},
	}
}

func NewIncrementalMismatch(firstUnitID, incrementalDigest, fullDigest string) *BizError {
	return &BizError{
		Code:       "E_INCREMENTAL_MISMATCH",
		Message:    "incremental rebuild does not match full rebuild",
		Stage:      "consistency",
		HTTPStatus: 500,
		Details: map[string]any{
			"first_unit_id":       firstUnitID,
			"incremental_digest":  incrementalDigest,
			"full_digest":         fullDigest,
			"incremental_summary": incrementalDigest,
			"full_summary":        fullDigest,
		},
	}
}
