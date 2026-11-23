// Package freshness contains the deterministic request-freshness rules shared by
// the writer, recovery path, and diagnostic APIs.
package freshness

import "fmt"

const (
	CodeOK                  = "OK"
	CodeRequestInvalid      = "E_REQUEST_INVALID"
	CodeSignatureMismatch   = "E_SIGNATURE_MISMATCH"
	CodeTimestampTooOld     = "E_TIMESTAMP_TOO_OLD"
	CodeTimestampAhead      = "E_TIMESTAMP_AHEAD"
	CodeNonceReplayed       = "E_NONCE_REPLAYED"
	CodeConflictingRequest  = "E_CONFLICTING_REQUEST"
	CodePrecheckIncomplete  = "E_PRECHECK_INCOMPLETE"
	CodeBatchNotFound       = "E_BATCH_NOT_FOUND"
	CodeVersionNotVisible   = "E_VERSION_NOT_VISIBLE"
	CodeInternal            = "E_INTERNAL"
	CodeVersionInvalid      = "E_VERSION_INVALID"
	CodeRequestTooLarge     = "E_REQUEST_TOO_LARGE"
	CodeDiagnosticAborted   = "E_DIAGNOSTIC_ABORTED"
	CodeRecoveryUnavailable = "E_RECOVERY_UNAVAILABLE"
)

const (
	ModeFull = "full"
	ModeFast = "fast"

	ResultAccepted      = "accepted"
	ResultRejected      = "rejected"
	ResultNotPrechecked = "not_prechecked"

	OutcomeAccepted = "accepted"
	OutcomeRejected = "rejected"
	OutcomePartial  = "partial"

	BoundaryLower = "lower"
	BoundaryUpper = "upper"
	BoundaryNone  = "none"

	StagePassed        = "passed"
	StageFailed        = "failed"
	StageSkipped       = "skipped"
	StageNotPrechecked = "not_prechecked"
)

// ToleranceBoundaryRule is intentionally explicit and included in every
// decision so clients can implement exactly the same comparison.
const ToleranceBoundaryRule = "inclusive: -tolerance_ms <= timestamp_ms-gateway_reference_time_ms <= +tolerance_ms"

// BatchInput is the wire representation. Integers are decoded directly into
// int64, so a JSON integer outside int64 is rejected as E_REQUEST_INVALID
// before any intent is created.
type BatchInput struct {
	SchemaVersion          int            `json:"schema_version"`
	BatchID                string         `json:"batch_id"`
	Mode                   string         `json:"mode"`
	GatewayReferenceTimeMS *int64         `json:"gateway_reference_time_ms,omitempty"`
	Requests               []RequestInput `json:"requests"`
}

type RequestInput struct {
	RequestID   string `json:"request_id,omitempty"`
	TimestampMS int64  `json:"timestamp_ms"`
	RouteKey    string `json:"route_key"`
	ClientID    string `json:"client_id"`
	Nonce       string `json:"nonce"`
	Signature   string `json:"signature"`
	Weight      int64  `json:"weight"`
	ToleranceMS int64  `json:"tolerance_ms"`
	WindowMS    int64  `json:"window_ms"`
}

// Request is the normalized, validated form. RequestID is filled deterministically
// when omitted. Signature is lower-cased for comparison.
type Request struct {
	RequestID   string `json:"request_id"`
	TimestampMS int64  `json:"timestamp_ms"`
	RouteKey    string `json:"route_key"`
	ClientID    string `json:"client_id"`
	Nonce       string `json:"nonce"`
	Signature   string `json:"signature"`
	Weight      int64  `json:"weight"`
	ToleranceMS int64  `json:"tolerance_ms"`
	WindowMS    int64  `json:"window_ms"`
}

type ValidatedBatch struct {
	BatchID          string    `json:"batch_id"`
	Mode             string    `json:"mode"`
	ReferenceTimeMS  int64     `json:"gateway_reference_time_ms"`
	ReferenceSource  string    `json:"reference_source"`
	Requests         []Request `json:"requests"`
	TotalWeight      int64     `json:"total_weight"`
	ContentHash      string    `json:"content_hash"`
	StructuralIssues []Issue   `json:"structural_issues,omitempty"`
}

type Issue struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// AppError is safe to serialize to an API error response.
type AppError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func (e *AppError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewAppError(code, message string, details any) *AppError {
	return &AppError{Code: code, Message: message, Details: details}
}

type StageResult struct {
	Status                 string         `json:"status"`
	Code                   string         `json:"code,omitempty"`
	WindowVersion          int64          `json:"window_version"`
	ToleranceConfigVersion string         `json:"tolerance_config_version"`
	Details                map[string]any `json:"details,omitempty"`
}

type ItemDecision struct {
	Ordinal                int            `json:"ordinal"`
	RequestID              string         `json:"request_id"`
	TimestampMS            int64          `json:"timestamp_ms"`
	RouteKey               string         `json:"route_key"`
	ClientID               string         `json:"client_id"`
	Nonce                  string         `json:"nonce"`
	Weight                 int64          `json:"weight"`
	ToleranceMS            int64          `json:"tolerance_ms"`
	WindowMS               int64          `json:"window_ms"`
	Result                 string         `json:"result"`
	Code                   string         `json:"code"`
	Message                string         `json:"message"`
	Prechecked             bool           `json:"prechecked"`
	OffsetMS               int64          `json:"offset_ms"`
	ToleranceLowerBoundMS  int64          `json:"tolerance_lower_bound_ms"`
	ToleranceUpperBoundMS  int64          `json:"tolerance_upper_bound_ms"`
	TriggeredBoundary      string         `json:"triggered_boundary"`
	BoundaryInclusive      bool           `json:"boundary_inclusive"`
	WindowVersion          int64          `json:"window_version"`
	ToleranceConfigVersion string         `json:"tolerance_config_version"`
	FirstOccupiedAtMS      *int64         `json:"first_occupied_at_ms,omitempty"`
	RemainingWindowMS      *int64         `json:"remaining_window_ms,omitempty"`
	ReplayBatchID          string         `json:"replay_batch_id,omitempty"`
	ReplayRequestID        string         `json:"replay_request_id,omitempty"`
	SignatureStage         StageResult    `json:"signature_stage"`
	TimestampStage         StageResult    `json:"timestamp_stage"`
	NonceStage             StageResult    `json:"nonce_stage"`
	PrecheckSelection      string         `json:"precheck_selection"`
	DiagnosticExplanation  map[string]any `json:"diagnostic_explanation,omitempty"`
}

type BatchDecision struct {
	BatchID                string         `json:"batch_id"`
	Mode                   string         `json:"mode"`
	ContentHash            string         `json:"content_hash"`
	ReferenceTimeMS        int64          `json:"gateway_reference_time_ms"`
	ReferenceSource        string         `json:"reference_source"`
	WindowVersion          int64          `json:"window_version"`
	ToleranceConfigVersion string         `json:"tolerance_config_version"`
	Outcome                string         `json:"outcome"`
	Code                   string         `json:"code"`
	Message                string         `json:"message,omitempty"`
	RequestCount           int            `json:"request_count"`
	PrecheckedCount        int            `json:"prechecked_count"`
	TotalWeight            int64          `json:"total_weight"`
	SampleRate             float64        `json:"sample_rate"`
	PrecheckLimit          int            `json:"precheck_limit"`
	PrecheckMethod         string         `json:"precheck_method"`
	Confidence             string         `json:"confidence"`
	ConfidenceNote         string         `json:"confidence_note"`
	BoundaryRule           string         `json:"tolerance_boundary_rule"`
	Items                  []ItemDecision `json:"items"`
	IdempotentReplay       bool           `json:"idempotent_replay"`
	CommittedAtMS          int64          `json:"committed_at_ms"`
}

type ConfigView struct {
	Version             string `json:"version"`
	SigningKeyID        string `json:"signing_key_id"`
	FastPrecheckLimit   int    `json:"fast_precheck_limit"`
	MaxToleranceMS      int64  `json:"max_tolerance_ms"`
	MaxWindowMS         int64  `json:"max_window_ms"`
	MaxRequestsPerBatch int    `json:"max_requests_per_batch"`
	QueryBudgetMS       int    `json:"default_query_budget_ms"`
}

type RecoveryReport struct {
	StartedAtMS        int64          `json:"started_at_ms"`
	FinishedAtMS       int64          `json:"finished_at_ms"`
	PendingIntents     int            `json:"pending_intents"`
	ConfirmedCommitted int            `json:"confirmed_committed"`
	Aborted            int            `json:"aborted"`
	Details            []RecoveryFact `json:"details"`
	FactsOnly          bool           `json:"facts_only"`
}

type RecoveryFact struct {
	IntentID         int64  `json:"intent_id"`
	BatchID          string `json:"batch_id"`
	PlannedVersion   int64  `json:"planned_window_version"`
	Action           string `json:"action"`
	DecisionPresent  bool   `json:"decision_present"`
	CommitFactsFound bool   `json:"commit_facts_found"`
}
