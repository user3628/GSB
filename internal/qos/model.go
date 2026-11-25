package qos

import "time"

const (
	CodeOK               = "OK"
	CodePolicyInvalid    = "E_POLICY_INVALID"
	CodeStaleBase        = "E_STALE_BASE"
	CodeStaleSnapshot    = "E_STALE_SNAPSHOT"
	CodeDependencyCycle  = "E_DEPENDENCY_CYCLE"
	CodeStaleRead        = "E_STALE_READ"
	CodeNoPolicy         = "E_NO_POLICY"
	CodeNotFound         = "E_NOT_FOUND"
	CodeSimulationFailed = "E_SIMULATION_FAILED"
	CodeRequestConflict  = "E_REQUEST_CONFLICT"
	CodeAborted          = "E_ABORTED"
	CodeInternal         = "E_INTERNAL"
)

// MatchRule identifies the packets owned by a shaping queue.
type MatchRule struct {
	SrcPortMin int   `json:"src_port_min"`
	SrcPortMax int   `json:"src_port_max"`
	DSCPValues []int `json:"dscp_values"`
}

// Queue is one shaping queue. Rates are bits per second and burst sizes are bytes.
type Queue struct {
	ID       string    `json:"queue_id"`
	CIRBPS   int64     `json:"cir_bps"`
	PIRBPS   int64     `json:"pir_bps"`
	CBSBytes int64     `json:"cbs_bytes"`
	PBSBytes int64     `json:"pbs_bytes"`
	Priority int       `json:"priority"`
	Match    MatchRule `json:"match"`
	ParentID *string   `json:"parent_id"`
}

// SnapshotRequest replaces the complete policy at a strictly increasing version.
type SnapshotRequest struct {
	RequestID string  `json:"request_id"`
	Version   int64   `json:"version"`
	Queues    []Queue `json:"queues"`
}

// QueuePatch changes selected mutable fields. New queues must supply all fields.
type QueuePatch struct {
	QueueID  string     `json:"queue_id"`
	CIRBPS   *int64     `json:"cir_bps,omitempty"`
	PIRBPS   *int64     `json:"pir_bps,omitempty"`
	CBSBytes *int64     `json:"cbs_bytes,omitempty"`
	PBSBytes *int64     `json:"pbs_bytes,omitempty"`
	Priority *int       `json:"priority,omitempty"`
	Match    *MatchRule `json:"match,omitempty"`
}

// ParentChange edits one dependency edge. A nil ParentID makes the queue a root.
type ParentChange struct {
	QueueID  string  `json:"queue_id"`
	ParentID *string `json:"parent_id"`
}

// IncrementRequest applies a local patch against exactly one current base version.
type IncrementRequest struct {
	RequestID     string         `json:"request_id"`
	BaseVersion   int64          `json:"base_version"`
	QueuePatches  []QueuePatch   `json:"queue_patches,omitempty"`
	ParentChanges []ParentChange `json:"parent_changes,omitempty"`
	DeleteQueues  []string       `json:"delete_queue_ids,omitempty"`
}

type MutationResponse struct {
	Code               string   `json:"code"`
	Kind               string   `json:"kind"`
	Version            int64    `json:"version"`
	BaseVersion        int64    `json:"base_version,omitempty"`
	AffectedQueueIDs   []string `json:"affected_queue_ids"`
	RecomputedQueueIDs []string `json:"recomputed_queue_ids"`
	RemovedQueueIDs    []string `json:"removed_queue_ids,omitempty"`
	UnchangedQueueIDs  []string `json:"unchanged_queue_ids"`
	CommittedAt        string   `json:"committed_at"`
	PreviousVersion    int64    `json:"previous_version"`
	PolicyDigest       string   `json:"policy_digest"`
	DerivedInvalidated bool     `json:"derived_invalidated"`
}

type InheritanceSource struct {
	Kind            string `json:"kind"` // declared or parent_limit
	SelfValue       int64  `json:"self_value"`
	ParentQueueID   string `json:"parent_queue_id,omitempty"`
	ParentEffective int64  `json:"parent_effective,omitempty"`
}

type EffectiveRates struct {
	QueueID             string            `json:"queue_id"`
	DeclaredCIRBPS      int64             `json:"declared_cir_bps"`
	EffectiveCIRBPS     int64             `json:"effective_cir_bps"`
	CIRSRC              InheritanceSource `json:"cir_source"`
	DeclaredPIRBPS      int64             `json:"declared_pir_bps"`
	EffectivePIRBPS     int64             `json:"effective_pir_bps"`
	PIRSRC              InheritanceSource `json:"pir_source"`
	ParentID            *string           `json:"parent_id"`
	ParentChain         []string          `json:"parent_chain"`
	Depth               int               `json:"depth"`
	CalculationVersion  int64             `json:"calculation_version"`
	ValidThroughVersion int64             `json:"valid_through_version"`
}

type TokenBucketView struct {
	QueueID              string   `json:"queue_id"`
	EffectiveCIRBPS      int64    `json:"effective_cir_bps"`
	EffectivePIRBPS      int64    `json:"effective_pir_bps"`
	CBSBytes             int64    `json:"cbs_bytes"`
	PBSBytes             int64    `json:"pbs_bytes"`
	Priority             int      `json:"priority"`
	PriorityLayer        string   `json:"priority_layer"`
	ParentChain          []string `json:"parent_chain"`
	TokenUnit            string   `json:"token_unit"`
	InitialState         string   `json:"initial_state"`
	CommittedBucket      string   `json:"committed_bucket"`
	PeakBucket           string   `json:"peak_bucket"`
	RefillContract       string   `json:"refill_contract"`
	RoundingContract     string   `json:"rounding_contract"`
	MsTruncationContract string   `json:"ms_truncation_contract"`
	CalculationVersion   int64    `json:"calculation_version"`
	ValidThroughVersion  int64    `json:"valid_through_version"`
}

type ScoreCheck struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Penalty int    `json:"penalty"`
	Detail  string `json:"detail"`
}

type DerivedReference struct {
	Field   string `json:"field"`
	QueueID string `json:"queue_id"`
	Source  string `json:"source"`
	Value   int64  `json:"value"`
}

type ConsistencyScore struct {
	QueueID             string             `json:"queue_id"`
	Score               int                `json:"score"`
	Checks              []ScoreCheck       `json:"checks"`
	References          []DerivedReference `json:"references"`
	ParentChain         []string           `json:"parent_chain"`
	CalculationVersion  int64              `json:"calculation_version"`
	ValidThroughVersion int64              `json:"valid_through_version"`
}

type QueueViewResponse struct {
	Code                string           `json:"code"`
	Version             int64            `json:"version"`
	UsedVersion         int64            `json:"used_version"`
	Queue               Queue            `json:"queue"`
	EffectiveRates      EffectiveRates   `json:"effective_rates"`
	TokenBucketView     TokenBucketView  `json:"token_bucket_view"`
	Consistency         ConsistencyScore `json:"consistency"`
	CalculationVersion  int64            `json:"calculation_version"`
	ValidThroughVersion int64            `json:"valid_through_version"`
}

// TrafficSample is one deterministic offline packet.
type TrafficSample struct {
	TimestampMS int64  `json:"timestamp_ms"`
	QueueID     string `json:"queue_id"`
	Bytes       int64  `json:"bytes"`
	DSCP        int    `json:"dscp"`
}

type SimulationRequest struct {
	RequestID     string   `json:"request_id"`
	PolicyVersion int64    `json:"policy_version"`
	BaseVersion   int64    `json:"base_version,omitempty"`
	QueueIDs      []string `json:"queue_ids,omitempty"`
}

type RefillRounding struct {
	Bucket                   string `json:"bucket"`
	RateBPS                  int64  `json:"rate_bps"`
	DeltaMS                  int64  `json:"delta_ms"`
	RawMilliBits             int64  `json:"raw_millibits"`
	CarryBeforeMilliBits     int64  `json:"carry_before_millibits"`
	WholeBits                int64  `json:"whole_bits"`
	CarryAfterMilliBits      int64  `json:"carry_after_millibits"`
	CumulativeErrorMilliBits int64  `json:"cumulative_error_millibits"`
	CapacityOverflowBits     int64  `json:"capacity_overflow_bits"`
	Explanation              string `json:"explanation"`
}

type PacketResult struct {
	Sequence             int              `json:"sequence"`
	TimestampMS          int64            `json:"timestamp_ms"`
	QueueID              string           `json:"queue_id"`
	Bytes                int64            `json:"bytes"`
	DSCP                 int              `json:"dscp"`
	Priority             int              `json:"priority"`
	TokensBeforeBits     int64            `json:"tokens_before_bits"`
	PeakTokensBeforeBits int64            `json:"peak_tokens_before_bits"`
	Refill               []RefillRounding `json:"refill"`
	TokensAfterBits      int64            `json:"tokens_after_bits"`
	PeakTokensAfterBits  int64            `json:"peak_tokens_after_bits"`
	Color                string           `json:"color"`
	SentBytes            int64            `json:"sent_bytes"`
	DroppedBytes         int64            `json:"dropped_bytes"`
	TokenBalanceBits     int64            `json:"token_balance_bits"`
	PeakBalanceBits      int64            `json:"peak_balance_bits"`
	MsTruncationMS       int64            `json:"ms_truncation_ms"`
}

type QueueSimulationSummary struct {
	QueueID                      string `json:"queue_id"`
	Priority                     int    `json:"priority"`
	EffectiveCIRBPS              int64  `json:"effective_cir_bps"`
	EffectivePIRBPS              int64  `json:"effective_pir_bps"`
	OfferedBytes                 int64  `json:"offered_bytes"`
	SentBytes                    int64  `json:"sent_bytes"`
	DroppedBytes                 int64  `json:"dropped_bytes"`
	GreenPackets                 int    `json:"green_packets"`
	YellowPackets                int    `json:"yellow_packets"`
	RedPackets                   int    `json:"red_packets"`
	FinalTokenBalanceBits        int64  `json:"final_token_balance_bits"`
	FinalPeakBalanceBits         int64  `json:"final_peak_balance_bits"`
	CumulativeErrorMilliBits     int64  `json:"cumulative_error_millibits"`
	CumulativePeakErrorMilliBits int64  `json:"cumulative_peak_error_millibits"`
	Conclusion                   string `json:"conclusion"`
	PolicyVersion                int64  `json:"policy_version"`
	CalculationVersion           int64  `json:"calculation_version"`
	ValidThroughVersion          int64  `json:"valid_through_version"`
}

type SimulationSummaryResponse struct {
	Code             string                   `json:"code"`
	SimulationID     string                   `json:"simulation_id"`
	PolicyVersion    int64                    `json:"policy_version"`
	ObservedVersion  int64                    `json:"observed_version"`
	UsedVersion      int64                    `json:"used_version"`
	SampleSeed       int64                    `json:"sample_seed"`
	SampleSHA256     string                   `json:"sample_sha256"`
	Packets          int                      `json:"packets"`
	Queues           []QueueSimulationSummary `json:"queues"`
	RoundingContract string                   `json:"rounding_contract"`
	CreatedAt        string                   `json:"created_at"`
}

type PolicySnapshotResponse struct {
	Code         string              `json:"code"`
	Version      int64               `json:"version"`
	UsedVersion  int64               `json:"used_version"`
	Queues       []QueueViewResponse `json:"queues"`
	PolicyDigest string              `json:"policy_digest"`
}

type QueueDiagnosticResponse struct {
	Code                     string                 `json:"code"`
	SimulationID             string                 `json:"simulation_id"`
	PolicyVersion            int64                  `json:"policy_version"`
	ObservedVersion          int64                  `json:"observed_version"`
	UsedVersion              int64                  `json:"used_version"`
	CalculationVersion       int64                  `json:"calculation_version"`
	ValidThroughVersion      int64                  `json:"valid_through_version"`
	Summary                  QueueSimulationSummary `json:"summary"`
	TokenBalanceBits         int64                  `json:"token_balance_bits"`
	PeakTokenBalanceBits     int64                  `json:"peak_token_balance_bits"`
	SentBytes                int64                  `json:"sent_bytes"`
	DroppedBytes             int64                  `json:"dropped_bytes"`
	Conclusion               string                 `json:"conclusion"`
	Packets                  []PacketResult         `json:"packets"`
	CumulativeErrorMilliBits int64                  `json:"cumulative_error_millibits"`
	RoundingContract         string                 `json:"rounding_contract"`
}

type RecoveryReportEntry struct {
	RequestID       string `json:"request_id"`
	Kind            string `json:"kind"`
	Phase           string `json:"phase"`
	Status          string `json:"status"`
	RollbackVersion int64  `json:"rollback_version"`
	DerivedRollback bool   `json:"derived_rollback"`
	TokenRollback   bool   `json:"token_rollback"`
	Message         string `json:"message"`
}

type RecoveryReportResponse struct {
	Code           string                `json:"code"`
	CurrentVersion int64                 `json:"current_version"`
	PendingIntents int                   `json:"pending_intents"`
	Reports        []RecoveryReportEntry `json:"reports"`
	CheckedAt      string                `json:"checked_at"`
}

type IntentStatus struct {
	RequestID        string         `json:"request_id"`
	Kind             string         `json:"kind"`
	Status           string         `json:"status"`
	BaseVersion      int64          `json:"base_version"`
	TargetVersion    int64          `json:"target_version"`
	Phase            string         `json:"phase"`
	CommittedVersion int64          `json:"committed_version"`
	Response         any            `json:"response,omitempty"`
	Error            *BusinessError `json:"error,omitempty"`
	CreatedAt        string         `json:"created_at"`
	CompletedAt      string         `json:"completed_at,omitempty"`
}

type SampleMeta struct {
	Seed           int64  `json:"seed"`
	PolicyFile     string `json:"policy_file"`
	TrafficFile    string `json:"traffic_file"`
	PolicySHA256   string `json:"policy_sha256"`
	TrafficSHA256  string `json:"traffic_sha256"`
	TrafficPackets int    `json:"traffic_packets"`
	QueueCount     int    `json:"queue_count"`
	GeneratedAt    string `json:"generated_at"`
}

type PolicyState struct {
	Version    int64   `json:"version"`
	Queues     []Queue `json:"queues"`
	Digest     string  `json:"digest"`
	CommitKind string  `json:"commit_kind"`
	UpdatedAt  string  `json:"updated_at"`
}

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339Nano) }
