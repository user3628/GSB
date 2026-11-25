package ttsflow

import (
	"errors"
	"fmt"
)

const (
	CodeEmptyText           = "E_EMPTY_TEXT"
	CodePhonemeMapping      = "E_PHONEME_MAPPING"
	CodeSplit               = "E_SPLIT"
	CodeNumeric             = "E_NUMERIC"
	CodeLengthMismatch      = "E_LENGTH_MISMATCH"
	CodeStaleCheckpoint     = "E_STALE_CHECKPOINT"
	CodeIncrementalMismatch = "E_INCREMENTAL_MISMATCH"
	CodeRollback            = "E_ROLLBACK"
	CodeConfigInvalid       = "E_CONFIG_INVALID"
	CodeDatasetInvalid      = "E_DATASET_INVALID"
	CodeGracefulStop        = "E_GRACEFUL_STOP"
	CodeLock                = "E_LOCK"
)

// BizError is the stable domain error returned across the public API. Its
// diagnostic fields are deliberately present even when empty so CLI consumers
// can rely on one schema.
type BizError struct {
	Code             string `json:"code"`
	Message          string `json:"message"`
	UTTID            string `json:"utt_id,omitempty"`
	BlockSeq         int    `json:"block_seq"`
	SegmentSeq       int    `json:"segment_seq"`
	Stage            string `json:"stage,omitempty"`
	FrameNo          int    `json:"frame_no"`
	DeclaredValue    int    `json:"declared_value,omitempty"`
	ActualValue      int    `json:"actual_value,omitempty"`
	CurrentSeq       int    `json:"current_seq"`
	ExpectedSeq      int    `json:"expected_seq"`
	SnapshotKind     string `json:"snapshot_kind,omitempty"`
	FirstDiffSegment int    `json:"first_diff_segment"`
	RuneOffset       int    `json:"rune_offset"`
	Context          string `json:"context,omitempty"`
	Cause            error  `json:"-"`
}

func (e *BizError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *BizError) Unwrap() error { return e.Cause }

func biz(code, message string) *BizError { return &BizError{Code: code, Message: message} }

func withUTT(err *BizError, utt string) *BizError {
	if err != nil {
		err.UTTID = utt
	}
	return err
}

// IsCode reports whether err (or a wrapped error) has the stable business code.
func IsCode(err error, code string) bool {
	var be *BizError
	return errors.As(err, &be) && be.Code == code
}

// AsBizError extracts the first *BizError in an error chain.
func AsBizError(err error) (*BizError, bool) {
	var be *BizError
	if errors.As(err, &be) {
		return be, true
	}
	return nil, false
}

var (
	// ErrGracefulStop is returned after the current indivisible unit has been
	// committed and a stop request has been observed.
	ErrGracefulStop = &BizError{Code: CodeGracefulStop, Message: "graceful stop completed after the current unit"}
)
