package edgeconfig

import "time"

const (
	StatusPending   = "PENDING"
	StatusConverged = "CONVERGED"
	StatusDiverged  = "DIVERGED"
	StatusStale     = "STALE_READ"
	StatusRejected  = "REJECTED"
	StatusTimeout   = "TIMEOUT"
	StatusFailed    = "FAILED"

	IntentPending   = "PENDING"
	IntentCommitted = "COMMITTED"
	IntentAborted   = "ABORTED"
)

type ConfigKey struct {
	Path    string `json:"path"`
	Type    string `json:"type"`
	Value   any    `json:"value"`
	OrderNo int    `json:"order_no"`
}

type Node struct {
	ID             string   `json:"id"`
	Region         string   `json:"region"`
	SubscribedKeys []string `json:"subscribed_keys"`
	Endpoint       string   `json:"endpoint"`
	DependsOn      []string `json:"depends_on,omitempty"`
}

type StrategyPolicy struct {
	PerKeyExactMaxChangedKeys  int   `json:"per_key_exact_max_changed_keys"`
	RegionBucketMinChangedKeys int   `json:"region_bucket_min_changed_keys"`
	LargeSubscriptionRatioPPM  int64 `json:"large_subscription_ratio_ppm"`
	FullSubscriptionSwitchKeys int   `json:"full_subscription_switch_keys"`
}

type ProbePolicy struct {
	TimeoutMS   int64 `json:"timeout_ms"`
	Concurrency int   `json:"concurrency"`
}

type NumericPolicy struct {
	Scale                int64            `json:"scale"`
	ThresholdUnits       int64            `json:"threshold_units"`
	RelativeTolerancePPM int64            `json:"relative_tolerance_ppm"`
	DefaultWeight        int64            `json:"default_weight"`
	NodeWeights          map[string]int64 `json:"node_weights"`
	Rounding             string           `json:"rounding"`
	TieBreak             string           `json:"tie_break"`
}

type Policy struct {
	Strategy StrategyPolicy `json:"strategy"`
	Probe    ProbePolicy    `json:"probe"`
	Numeric  NumericPolicy  `json:"numeric"`
}

type Topology struct {
	BatchID       string      `json:"batch_id,omitempty"`
	SchemaVersion int         `json:"schema_version"`
	GeneratedAtMS int64       `json:"generated_at_ms"`
	Keys          []ConfigKey `json:"keys"`
	Nodes         []Node      `json:"nodes"`
	Policy        Policy      `json:"policy"`
}

type Snapshot struct {
	CommitID      string      `json:"commit_id"`
	Version       int64       `json:"version"`
	GeneratedAtMS int64       `json:"generated_at_ms"`
	Digest        string      `json:"digest"`
	Keys          []ConfigKey `json:"keys"`
}

type SubscriptionChange struct {
	NodeID string   `json:"node_id"`
	Add    []string `json:"add,omitempty"`
	Remove []string `json:"remove,omitempty"`
}

type Increment struct {
	CommitID            string               `json:"commit_id"`
	BaseVersion         int64                `json:"base_version"`
	GeneratedAtMS       int64                `json:"generated_at_ms"`
	Changes             []ConfigKey          `json:"changes,omitempty"`
	RemoveKeys          []string             `json:"remove_keys,omitempty"`
	SubscriptionChanges []SubscriptionChange `json:"subscription_changes,omitempty"`
	Reason              string               `json:"reason,omitempty"`
}

type Report struct {
	ReportID        string `json:"report_id,omitempty"`
	NodeID          string `json:"node_id"`
	AppliedDigest   string `json:"applied_digest"`
	EffectiveAtMS   int64  `json:"effective_at_ms"`
	ObservedVersion int64  `json:"observed_version"`
}

type ReportDecision struct {
	Decision           string `json:"decision"`
	Code               string `json:"code,omitempty"`
	NodeID             string `json:"node_id"`
	Converged          bool   `json:"converged"`
	UsedVersion        int64  `json:"used_version"`
	ExpectedVersion    int64  `json:"expected_version"`
	CurrentViewVersion int64  `json:"current_view_version"`
	ExpectedDigest     string `json:"expected_digest"`
	AppliedDigest      string `json:"applied_digest"`
	EffectiveAtMS      int64  `json:"effective_at_ms"`
}

type ImpactedNode struct {
	NodeID      string   `json:"node_id"`
	Region      string   `json:"region"`
	Hop         int      `json:"hop"`
	TriggerKeys []string `json:"trigger_keys"`
	Seeds       bool     `json:"seeds,omitempty"`
}

type StrategyMetrics struct {
	TotalNodes            int `json:"total_nodes"`
	ChangedKeys           int `json:"changed_keys"`
	NaiveScanProduct      int `json:"naive_scan_product"`
	IndexLookups          int `json:"index_lookups,omitempty"`
	SubscriberVisits      int `json:"subscriber_visits,omitempty"`
	BucketRegions         int `json:"bucket_regions,omitempty"`
	CandidateNodesScanned int `json:"candidate_nodes_scanned,omitempty"`
	FullSubscriptionNodes int `json:"full_subscription_nodes"`
}

type AffectedReport struct {
	BaseVersion    int64           `json:"base_version"`
	TargetVersion  int64           `json:"target_version"`
	ChangedKeys    []string        `json:"changed_keys"`
	SelectedPath   string          `json:"selected_path"`
	SelectionBasis string          `json:"selection_basis"`
	Metrics        StrategyMetrics `json:"metrics"`
	Nodes          []ImpactedNode  `json:"nodes"`
}

type ExpectedView struct {
	CommitVersion   int64  `json:"commit_version"`
	NodeID          string `json:"node_id"`
	Region          string `json:"region"`
	ExpectedVersion int64  `json:"expected_version"`
	ExpectedDigest  string `json:"expected_digest"`
	KeyCount        int    `json:"key_count"`
	DigestJSON      string `json:"digest_json"`
}

type ProbeRequest struct {
	RoundID     string   `json:"round_id"`
	RequestID   string   `json:"request_id,omitempty"`
	NodeIDs     []string `json:"node_ids,omitempty"`
	TimeoutMS   int64    `json:"timeout_ms,omitempty"`
	Concurrency int      `json:"concurrency,omitempty"`
}

type ProbeOutcome struct {
	NodeID          string `json:"node_id"`
	Endpoint        string `json:"endpoint"`
	Status          string `json:"status"`
	HTTPStatus      int    `json:"http_status,omitempty"`
	AppliedDigest   string `json:"applied_digest,omitempty"`
	ObservedVersion int64  `json:"observed_version,omitempty"`
	EffectiveAtMS   int64  `json:"effective_at_ms,omitempty"`
	ErrorCode       string `json:"error_code,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
	LatencyMS       int64  `json:"latency_ms"`
	TimedOut        bool   `json:"timed_out"`
	Converged       bool   `json:"converged"`
	ExpectedVersion int64  `json:"expected_version"`
}

type ProbeRoundResult struct {
	RoundID        string         `json:"round_id"`
	RequestID      string         `json:"request_id"`
	TargetVersion  int64          `json:"target_version"`
	StartedAtMS    int64          `json:"started_at_ms"`
	CompletedAtMS  int64          `json:"completed_at_ms"`
	TimeoutMS      int64          `json:"timeout_ms"`
	Concurrency    int            `json:"concurrency"`
	Outcomes       []ProbeOutcome `json:"outcomes"`
	ConvergedCount int            `json:"converged_count"`
	TimeoutCount   int            `json:"timeout_count"`
	RejectedCount  int            `json:"rejected_count"`
	FailedCount    int            `json:"failed_count"`
	Consistency    *Consistency   `json:"consistency,omitempty"`
}

type RoundingStep struct {
	NodeID       string `json:"node_id"`
	Region       string `json:"region"`
	Weight       int64  `json:"weight"`
	Matched      bool   `json:"matched"`
	ExactNumer   int64  `json:"exact_numerator"`
	Quotient     int64  `json:"quotient"`
	Remainder    int64  `json:"remainder"`
	RoundedUnits int64  `json:"rounded_units"`
	ErrorNumer   int64  `json:"error_numerator"`
	ErrorDenom   int64  `json:"error_denominator"`
	Rounding     string `json:"rounding"`
}

type FixedError struct {
	Numerator   int64 `json:"numerator"`
	Denominator int64 `json:"denominator"`
	Scale       int64 `json:"scale"`
	FixedUnits  int64 `json:"fixed_units"`
}

type TieBreak struct {
	Policy       string   `json:"policy"`
	OrderedNodes []string `json:"ordered_nodes"`
	BoundaryNode string   `json:"boundary_node,omitempty"`
	Applied      bool     `json:"applied"`
}

type Consistency struct {
	Scope                   string         `json:"scope"`
	CommitVersion           int64          `json:"commit_version"`
	RoundID                 string         `json:"round_id,omitempty"`
	Scale                   int64          `json:"scale"`
	TotalWeight             int64          `json:"total_weight"`
	MatchedWeight           int64          `json:"matched_weight"`
	ScoreUnits              int64          `json:"score_units"`
	Score                   string         `json:"score"`
	ThresholdUnits          int64          `json:"threshold_units"`
	Threshold               string         `json:"threshold"`
	RelativeTolerancePPM    int64          `json:"relative_tolerance_ppm"`
	Decision                string         `json:"decision"`
	RoundingMode            string         `json:"rounding_mode"`
	Steps                   []RoundingStep `json:"rounding_steps"`
	CumulativeRoundingError FixedError     `json:"cumulative_rounding_error"`
	TieBreak                TieBreak       `json:"tie_break"`
	NodeOrder               []string       `json:"node_order"`
	RegionScores            []Consistency  `json:"region_scores,omitempty"`
}

type Intent struct {
	IntentID       string `json:"intent_id"`
	Kind           string `json:"kind"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	RequestHash    string `json:"request_hash,omitempty"`
	TargetVersion  int64  `json:"target_version,omitempty"`
	Status         string `json:"status"`
	PayloadJSON    string `json:"payload_json,omitempty"`
	RecoveryAction string `json:"recovery_action,omitempty"`
	CreatedAtMS    int64  `json:"created_at_ms"`
	UpdatedAtMS    int64  `json:"updated_at_ms"`
}

type RecoveryReport struct {
	RecoveredAtMS       int64    `json:"recovered_at_ms"`
	LastCompleteVersion int64    `json:"last_complete_version"`
	PendingIntents      []Intent `json:"pending_intents"`
	AbortedIntentIDs    []string `json:"aborted_intent_ids"`
	RolledBackToVersion int64    `json:"rolled_back_to_version"`
	Actions             []string `json:"actions"`
	PersistedFactsOnly  bool     `json:"persisted_facts_only"`
}

func nowMS() int64 { return time.Now().UnixMilli() }
