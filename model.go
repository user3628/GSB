package feedbackstream

const (
	MetricScale              int64 = 1_000_000
	BoundaryStartInclEndExcl       = "START_INCLUSIVE_END_EXCLUSIVE"
	LateOriginalWindow             = "ASSIGN_TIMESTAMP_WINDOW_THEN_REJECT_IF_COMMITTED"

	StatusQueued       = "QUEUED"
	StatusRunning      = "RUNNING"
	StatusBlocked      = "BLOCKED"
	StatusShed         = "SHED"
	StatusCompleted    = "COMPLETED"
	StatusLateRecorded = "LATE_RECORDED"
	StatusFailed       = "FAILED"

	IntentPending   = "PENDING"
	IntentCommitted = "COMMITTED"
	IntentAborted   = "ABORTED"
	IntentRecovered = "RECOVERED"
	IntentDiscarded = "DISCARDED"

	IntentStartBatch       = "START_BATCH"
	IntentWindowConclusion = "COMMIT_WINDOW_CONCLUSIONS"
	IntentBackpressure     = "BACKPRESSURE_TRANSITION"
	IntentSwitchPolicy     = "SWITCH_POLICY"
)

// Event is one deterministic local stream record.
type Event struct {
	TimestampMS int64  `json:"timestamp_ms"`
	PartitionID int    `json:"partition_id"`
	GroupKey    string `json:"group_key"`
	MetricFixed int64  `json:"metric_fixed"`
	Sequence    int64  `json:"sequence"`
}

type EventSnapshot struct {
	Schema      string  `json:"schema"`
	MetricScale int64   `json:"metric_scale"`
	Generator   string  `json:"generator"`
	Seed        int64   `json:"seed"`
	Events      []Event `json:"events"`
}

type WindowSpec struct {
	Type          string `json:"type"`
	SizeMS        int64  `json:"size_ms"`
	OriginMS      int64  `json:"origin_ms"`
	BoundaryRule  string `json:"boundary_rule"`
	LatePolicy    string `json:"late_policy"`
	AllowedLateMS int64  `json:"allowed_late_ms"`
}

type FieldSignature struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Signature struct {
	Schema string           `json:"schema"`
	Fields []FieldSignature `json:"fields"`
}

type OperatorConfig struct {
	MetricGTE    *int64   `json:"metric_gte,omitempty"`
	SelectFields []string `json:"select_fields,omitempty"`
	GroupBy      []string `json:"group_by,omitempty"`
	Aggregations []string `json:"aggregations,omitempty"`
}

type OperatorSpec struct {
	ID              string         `json:"id"`
	Kind            string         `json:"kind"`
	InputSignature  *Signature     `json:"input_signature,omitempty"`
	OutputSignature Signature      `json:"output_signature"`
	Repartition     bool           `json:"repartition,omitempty"`
	Config          OperatorConfig `json:"config,omitempty"`
}

type EdgeSpec struct {
	From        string `json:"from"`
	To          string `json:"to"`
	Repartition bool   `json:"repartition,omitempty"`
}

type PipelineSpec struct {
	Operators []OperatorSpec `json:"operators"`
	Edges     []EdgeSpec     `json:"edges,omitempty"`
}

type FusionPair struct {
	Left  string `json:"left"`
	Right string `json:"right"`
}

type FusionSpec struct {
	FusibleKinds []string     `json:"fusible_kinds"`
	Pairs        []FusionPair `json:"pairs"`
	MaxGroupSize int          `json:"max_group_size,omitempty"`
}

type BufferLimit struct {
	MaxInflightBatches int     `json:"max_inflight_batches"`
	MaxBytes           int64   `json:"max_bytes"`
	HighWatermarkRatio float64 `json:"high_watermark_ratio"`
}

type CostModel struct {
	BaseCostMS     int64 `json:"base_cost_ms"`
	PerEventCostMS int64 `json:"per_event_cost_ms"`
}

// PolicySnapshot is the versioned operator-chain strategy.
type PolicySnapshot struct {
	SchemaVersion        string                 `json:"schema_version"`
	Version              int64                  `json:"version"`
	Kind                 string                 `json:"kind"`
	GeneratedAtMS        int64                  `json:"generated_at_ms"`
	Description          string                 `json:"description,omitempty"`
	MetricScale          int64                  `json:"metric_scale"`
	Window               WindowSpec             `json:"window"`
	Pipeline             PipelineSpec           `json:"pipeline"`
	Fusion               FusionSpec             `json:"fusion"`
	DefaultBuffer        BufferLimit            `json:"default_buffer"`
	OperatorBuffers      map[string]BufferLimit `json:"operator_buffers,omitempty"`
	PartitionConcurrency int                    `json:"partition_concurrency"`
	RoundTimeBudgetMS    int64                  `json:"round_time_budget_ms"`
	MaxWaitQueue         int                    `json:"max_wait_queue"`
	CostModel            CostModel              `json:"cost_model"`
	ChecksumSHA256       string                 `json:"checksum_sha256"`
}

type OperatorPatch struct {
	ID          string          `json:"id"`
	Config      *OperatorConfig `json:"config,omitempty"`
	Repartition *bool           `json:"repartition,omitempty"`
}

type PolicyPatch struct {
	Kind                 string                 `json:"kind"`
	BaseVersion          int64                  `json:"base_version"`
	GeneratedAtMS        int64                  `json:"generated_at_ms"`
	Reason               string                 `json:"reason,omitempty"`
	Operators            []OperatorPatch        `json:"operators,omitempty"`
	Buffers              map[string]BufferLimit `json:"buffers,omitempty"`
	PartitionConcurrency *int                   `json:"partition_concurrency,omitempty"`
	RoundTimeBudgetMS    *int64                 `json:"round_time_budget_ms,omitempty"`
}

type FusionGroup struct {
	Index           int       `json:"index"`
	ID              string    `json:"id"`
	OperatorIDs     []string  `json:"operator_ids"`
	Kinds           []string  `json:"kinds"`
	InputSignature  Signature `json:"input_signature"`
	OutputSignature Signature `json:"output_signature"`
	SignatureHash   string    `json:"signature_hash"`
}

type OperatorDecision struct {
	OperatorID    string   `json:"operator_id"`
	Kind          string   `json:"kind"`
	Fused         bool     `json:"fused"`
	GroupID       string   `json:"group_id,omitempty"`
	BoundaryAfter bool     `json:"boundary_after"`
	Reasons       []string `json:"reasons"`
}

type EdgeDecision struct {
	From    string   `json:"from"`
	To      string   `json:"to"`
	Fused   bool     `json:"fused"`
	GroupID string   `json:"group_id,omitempty"`
	Reasons []string `json:"reasons"`
}

type FusionPlan struct {
	Groups            []FusionGroup      `json:"groups"`
	OperatorDecisions []OperatorDecision `json:"operator_decisions"`
	EdgeDecisions     []EdgeDecision     `json:"edge_decisions"`
}

type Batch struct {
	ID             string  `json:"batch_id"`
	PartitionID    int     `json:"partition_id"`
	WindowStart    int64   `json:"window_start_ms"`
	WindowEnd      int64   `json:"window_end_ms"`
	Events         []Event `json:"events"`
	EventCount     int     `json:"event_count"`
	ByteSize       int64   `json:"byte_size"`
	PayloadHash    string  `json:"payload_hash"`
	Status         string  `json:"status,omitempty"`
	CurrentGroupID string  `json:"current_group_id,omitempty"`
}

type ProjectedEvent struct {
	Event
	WindowStartMS int64 `json:"window_start_ms"`
	WindowEndMS   int64 `json:"window_end_ms"`
}

type WindowAggregate struct {
	PartitionID   int    `json:"partition_id"`
	WindowStartMS int64  `json:"window_start_ms"`
	WindowEndMS   int64  `json:"window_end_ms"`
	GroupKey      string `json:"group_key"`
	Count         int64  `json:"count"`
	Sum           int64  `json:"sum_fixed"`
	Min           int64  `json:"min_fixed"`
	Max           int64  `json:"max_fixed"`
}

type GroupOutput struct {
	GroupID       string            `json:"group_id"`
	SignatureHash string            `json:"signature_hash"`
	Projected     []ProjectedEvent  `json:"projected,omitempty"`
	Aggregates    []WindowAggregate `json:"aggregates,omitempty"`
}

type BatchResult struct {
	BatchID       string              `json:"batch_id"`
	PartitionID   int                 `json:"partition_id"`
	Status        string              `json:"status"`
	Outputs       []GroupOutput       `json:"outputs"`
	Backpressure  *BackpressureReport `json:"backpressure,omitempty"`
	CompletedOnce bool                `json:"completed_once"`
}

type Candidate struct {
	BatchID     string `json:"batch_id"`
	PartitionID int    `json:"partition_id"`
	WindowStart int64  `json:"window_start_ms"`
	EventCount  int    `json:"event_count"`
	CostMS      int64  `json:"estimated_cost_ms"`
	ByteSize    int64  `json:"byte_size"`
}

type ShedDecision struct {
	BatchID              string `json:"batch_id"`
	PartitionID          int    `json:"partition_id"`
	WindowStartMS        int64  `json:"window_start_ms"`
	Rank                 int    `json:"rank"`
	EstimatedCostMS      int64  `json:"estimated_cost_ms"`
	ConsumedBudgetBefore int64  `json:"consumed_budget_before_ms"`
	RemainingBudgetMS    int64  `json:"remaining_budget_ms"`
	ReasonCode           string `json:"reason_code"`
	Reason               string `json:"reason"`
}

type AdmissionDecision struct {
	BatchID       string `json:"batch_id"`
	PartitionID   int    `json:"partition_id"`
	WindowStartMS int64  `json:"window_start_ms"`
	Admitted      bool   `json:"admitted"`
	CostMS        int64  `json:"cost_ms"`
	Reason        string `json:"reason,omitempty"`
}

type RoundPlan struct {
	RoundID              string              `json:"round_id"`
	BudgetMS             int64               `json:"budget_ms"`
	PartitionConcurrency int                 `json:"partition_concurrency"`
	Admissions           []AdmissionDecision `json:"admissions"`
	Shed                 []ShedDecision      `json:"shed"`
	ConsumedBudgetMS     int64               `json:"consumed_budget_ms"`
	RemainingBudgetMS    int64               `json:"remaining_budget_ms"`
}

type Watermark struct {
	PolicyVersion    int64   `json:"policy_version"`
	GroupID          string  `json:"fusion_group_id"`
	InflightBatches  int     `json:"inflight_batches"`
	InflightBytes    int64   `json:"inflight_bytes"`
	MaxBatches       int     `json:"max_batches"`
	MaxBytes         int64   `json:"max_bytes"`
	BatchUtilization float64 `json:"batch_utilization"`
	ByteUtilization  float64 `json:"byte_utilization"`
	WatermarkRatio   float64 `json:"watermark_ratio"`
	Backpressured    bool    `json:"backpressured"`
}

type BackpressureReport struct {
	Watermark              Watermark `json:"watermark"`
	TriggerGroupID         string    `json:"trigger_group_id"`
	BlockedUpstreamBatches int       `json:"blocked_upstream_batches"`
	WaitingQueueFull       bool      `json:"waiting_queue_full"`
}

type RecoveryAction struct {
	IntentID    string `json:"intent_id"`
	Kind        string `json:"kind"`
	AggregateID string `json:"aggregate_id,omitempty"`
	Action      string `json:"action"`
	Detail      string `json:"detail,omitempty"`
}

type RecoveryReport struct {
	DBPath                 string           `json:"db_path"`
	ScannedPendingIntents  int              `json:"scanned_pending_intents"`
	RolledBackBatches      []string         `json:"rolled_back_batches"`
	DiscardedWindowIntents []string         `json:"discarded_window_intents"`
	AbortedPolicySwitches  []string         `json:"aborted_policy_switches"`
	AbortedBackpressure    []string         `json:"aborted_backpressure"`
	Actions                []RecoveryAction `json:"actions"`
	ActivePolicyVersion    int64            `json:"active_policy_version"`
	Notice                 string           `json:"notice"`
}
