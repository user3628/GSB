package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
)

// BizCode is a stable business error code. The string value never changes so
// that operators can rely on it across releases.
type BizCode string

const (
	CodeOK                  BizCode = ""
	CodePoseInvalid         BizCode = "E_POSE_INVALID"
	CodeNaNInput            BizCode = "E_NAN_INPUT"
	CodeEmptyViews          BizCode = "E_EMPTY_VIEWS"
	CodeBadDensity          BizCode = "E_BAD_DENSITY"
	CodeOutOfRange          BizCode = "E_SAMPLE_OUT_OF_RANGE"
	CodeLatencyBudget       BizCode = "E_LATENCY_BUDGET"
	CodeStaleCheckpoint     BizCode = "E_STALE_CHECKPOINT"
	CodeCheckpointTampered  BizCode = "E_CHECKPOINT_TAMPERED"
	CodeCheckpointRollback  BizCode = "E_CHECKPOINT_ROLLBACK"
	CodeIncrementalMismatch BizCode = "E_INCREMENTAL_MISMATCH"
	CodeNumericMismatch     BizCode = "E_NUMERIC_MISMATCH"
	CodeNotFound            BizCode = "E_NOT_FOUND"
	CodeInvalidRequest      BizCode = "E_INVALID_REQUEST"
	CodeLocked              BizCode = "E_LOCKED"
	CodeInternal            BizCode = "E_INTERNAL"
)

// BizError is the single structured error type of the service. Every failure
// path uses it, so JSON output and stderr output always carry identical code
// and fields.
type BizError struct {
	Code      BizCode           `json:"code"`
	Message   string            `json:"message"`
	ShardID   string            `json:"shard_id,omitempty"`
	ViewID    string            `json:"view_id,omitempty"`
	Field     string            `json:"field,omitempty"`
	QueryID   string            `json:"query_id,omitempty"`
	ElapsedMs int64             `json:"elapsed_ms,omitempty"`
	Voxel     [3]int32          `json:"voxel,omitempty"`
	MaxDev    float64           `json:"max_deviation,omitempty"`
	Details   map[string]string `json:"details,omitempty"`
}

func (e *BizError) Error() string {
	b, _ := json.Marshal(e)
	return string(b)
}

func bizError(code BizCode, format string, args ...any) *BizError {
	return &BizError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Emit writes the structured error to stderr exactly once per failure. The
// JSON envelope emitted here matches the HTTP body field-for-field.
func (e *BizError) Emit() {
	b, _ := json.Marshal(e)
	fmt.Fprintln(os.Stderr, string(b))
}

func asBiz(err error) *BizError {
	if err == nil {
		return nil
	}
	if be, ok := err.(*BizError); ok {
		return be
	}
	return &BizError{Code: CodeInternal, Message: err.Error()}
}
