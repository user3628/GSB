package model

import (
	"encoding/json"
	"fmt"
)

const (
	CodeSnapshotInvalid            = "E_SNAPSHOT_INVALID"
	CodePrecisionToleranceExceeded = "E_PRECISION_TOLERANCE_EXCEEDED"
	CodeConflictingRequest         = "E_CONFLICTING_REQUEST"
	CodeExecutionAborted           = "E_EXECUTION_ABORTED"
	CodeInvalidRequest             = "E_INVALID_REQUEST"
	CodeNotFound                   = "E_NOT_FOUND"
	CodeNotReady                   = "E_NOT_READY"
	CodeExecutionInProgress        = "E_EXECUTION_IN_PROGRESS"
	CodeInternal                   = "E_INTERNAL"
)

type BusinessError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *BusinessError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code, message string, kv ...any) *BusinessError {
	ctx := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		key, ok := kv[i].(string)
		if !ok {
			continue
		}
		ctx[key] = kv[i+1]
	}
	return &BusinessError{Code: code, Message: message, Context: ctx}
}

func (e *BusinessError) With(key string, value any) *BusinessError {
	if e.Context == nil {
		e.Context = make(map[string]any)
	}
	e.Context[key] = value
	return e
}

func ErrorPayload(err error) (int, map[string]any) {
	if err == nil {
		return 200, map[string]any{"ok": true}
	}
	if be, ok := err.(*BusinessError); ok {
		status := 400
		switch be.Code {
		case CodeNotFound:
			status = 404
		case CodeConflictingRequest, CodeExecutionInProgress:
			status = 409
		case CodeExecutionAborted:
			status = 410
		case CodeInternal:
			status = 500
		}
		return status, map[string]any{"ok": false, "error": be}
	}
	return 500, map[string]any{
		"ok": false,
		"error": &BusinessError{
			Code:    CodeInternal,
			Message: err.Error(),
		},
	}
}

type JoinType string

const (
	JoinInner JoinType = "inner"
	JoinLeft  JoinType = "left"
)

type SumAlgorithm string

const (
	SumNaive           SumAlgorithm = "naive"
	SumKahan           SumAlgorithm = "kahan"
	SumDoubleDoubleFix SumAlgorithm = "double_double_fixed"
	SumAuto            SumAlgorithm = "auto"
)

type PrecisionPath string

const (
	PathFast PrecisionPath = "fast"
	PathHigh PrecisionPath = "high_precision"
)

type SnapshotMeta struct {
	Side         string `json:"side"`
	Version      uint64 `json:"version"`
	GeneratedAt  int64  `json:"generated_at_unix_nano"`
	Rows         int    `json:"rows"`
	SHA256       string `json:"sha256"`
	Path         string `json:"path"`
	ChecksumPath string `json:"checksum_path"`
	LoadedAt     int64  `json:"loaded_at_unix_nano,omitempty"`
}

type SnapshotPairMeta struct {
	Left  SnapshotMeta `json:"left"`
	Right SnapshotMeta `json:"right"`
}

type LoadSnapshotsRequest struct {
	Left  SnapshotLoadInput `json:"left"`
	Right SnapshotLoadInput `json:"right"`
}

type SnapshotLoadInput struct {
	Path         string `json:"path"`
	ChecksumPath string `json:"checksum_path,omitempty"`
	SHA256       string `json:"sha256,omitempty"`
}

type AggregateRequest struct {
	RequestID         string  `json:"request_id"`
	JoinType          string  `json:"join_type"`
	PartitionCount    int     `json:"partition_count"`
	SkewThreshold     float64 `json:"skew_threshold,omitempty"`
	Algorithm         string  `json:"algorithm"`
	FixedScaleBits    int     `json:"fixed_scale_bits,omitempty"`
	RelativeTolerance float64 `json:"relative_tolerance"`
	ComputeBudget     int64   `json:"compute_budget,omitempty"`
	MergeRule         string  `json:"merge_rule,omitempty"`
}

type RouteDecision struct {
	RequestedAlgorithm   string   `json:"requested_algorithm"`
	ResolvedAlgorithm    string   `json:"resolved_algorithm"`
	Path                 string   `json:"path"`
	Basis                []string `json:"basis"`
	PrecisionTarget      float64  `json:"relative_tolerance"`
	ComputeBudget        int64    `json:"compute_budget"`
	EstimatedWork        int64    `json:"estimated_work"`
	BudgetSatisfied      bool     `json:"budget_satisfied"`
	ErrorModel           string   `json:"error_model"`
	ApplicableConclusion string   `json:"applicable_conclusion"`
}

type PartitionStat struct {
	Partition     int     `json:"partition"`
	LeftRecords   int     `json:"left_records"`
	RightRecords  int     `json:"right_records"`
	RecordCount   int     `json:"record_count"`
	JoinedRecords int     `json:"joined_records"`
	BytesEstimate int64   `json:"bytes_estimate"`
	RecordShare   float64 `json:"record_share"`
	Skewed        bool    `json:"skewed"`
	AffectedKeys  []int64 `json:"affected_keys,omitempty"`
}

type SkewReport struct {
	Threshold        float64 `json:"threshold"`
	MaxRecordShare   float64 `json:"max_record_share"`
	SkewedPartitions []int   `json:"skewed_partitions"`
	Detected         bool    `json:"detected"`
}

type GroupResult struct {
	Key                  int64    `json:"key"`
	Sum                  *float64 `json:"sum"`
	Average              *float64 `json:"average"`
	GroupCount           int64    `json:"group_count"`
	ValueCount           int64    `json:"value_count"`
	AbsSum               float64  `json:"abs_sum"`
	AbsoluteErrorBound   float64  `json:"absolute_error_bound"`
	RelativeErrorBound   float64  `json:"relative_error_bound"`
	AverageAbsErrorBound float64  `json:"average_absolute_error_bound"`
	AverageRelErrorBound float64  `json:"average_relative_error_bound"`
	CompensationDigest   string   `json:"compensation_state_digest"`
	MaxRelativeDeviation float64  `json:"max_relative_deviation"`
}

type ErrorDeclaration struct {
	Algorithm               string  `json:"algorithm"`
	Path                    string  `json:"path"`
	Parameter               any     `json:"parameter,omitempty"`
	Model                   string  `json:"model"`
	AbsoluteErrorBoundSum   float64 `json:"absolute_error_bound_sum"`
	MaxRelativeBound        float64 `json:"max_relative_bound"`
	MaxAverageRelativeBound float64 `json:"max_average_relative_bound"`
	ApplicableConclusion    string  `json:"applicable_conclusion"`
	CompensationDigest      string  `json:"compensation_digest"`
}

type PartitionInvariance struct {
	Tolerance          float64            `json:"relative_tolerance"`
	Checked            bool               `json:"checked"`
	CheckedPartitions  []int              `json:"checked_partition_counts"`
	ReferencePartition int                `json:"reference_partition_count"`
	Passed             bool               `json:"passed"`
	MaxDeviationByKey  map[int64]float64  `json:"max_deviation_by_key"`
	ReferenceValues    map[int64]*float64 `json:"reference_values"`
}

type AggregateResponse struct {
	RequestID           string              `json:"request_id"`
	Status              string              `json:"status"`
	CommitVersion       int64               `json:"commit_version"`
	Attempt             int                 `json:"attempt"`
	Fingerprint         string              `json:"fingerprint"`
	Snapshots           SnapshotPairMeta    `json:"snapshots"`
	JoinType            string              `json:"join_type"`
	PartitionCount      int                 `json:"partition_count"`
	MergeRule           string              `json:"merge_rule"`
	MergeOrderBasis     string              `json:"merge_order_basis"`
	Route               RouteDecision       `json:"route"`
	Skew                SkewReport          `json:"skew"`
	Partitions          []PartitionStat     `json:"partitions"`
	Groups              []GroupResult       `json:"groups"`
	ErrorDeclaration    ErrorDeclaration    `json:"error_declaration"`
	PartitionInvariance PartitionInvariance `json:"partition_invariance"`
	ComputedAt          int64               `json:"computed_at_unix_nano"`
}

type RequestRecord struct {
	RequestID     string
	Fingerprint   string
	Status        string
	RequestJSON   string
	ResponseJSON  string
	ErrorJSON     string
	CommitVersion int64
	Attempts      int
}

type RecoveryEvent struct {
	ID            int64  `json:"id"`
	RequestID     string `json:"request_id"`
	Action        string `json:"action"`
	Detail        string `json:"detail"`
	CreatedAtNano int64  `json:"created_at_unix_nano"`
}

type RecoveryReport struct {
	GeneratedAtNano           int64           `json:"generated_at_unix_nano"`
	LastCompleteCommitVersion int64           `json:"last_complete_commit_version"`
	AbortedRequests           []string        `json:"aborted_requests"`
	PendingIntentsRemaining   int             `json:"pending_intents_remaining"`
	RequestsByStatus          map[string]int  `json:"requests_by_status"`
	IntentsByState            map[string]int  `json:"intents_by_state"`
	Events                    []RecoveryEvent `json:"events"`
	FactSource                string          `json:"fact_source"`
	Actions                   []string        `json:"actions"`
}

func JSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
