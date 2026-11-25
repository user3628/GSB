package domain

// Request states. Lowercase values are persisted verbatim.
const (
	StatePending   = "PENDING"
	StateAdmitted  = "ADMITTED"
	StateBatched   = "BATCHED"
	StateRunning   = "RUNNING"
	StateCompleted = "COMPLETED"
	StateRejected  = "REJECTED"
	StatePreempted = "PREEMPTED"
)

const (
	HealthHealthy   = "HEALTHY"
	HealthDegraded  = "DEGRADED"
	HealthDraining  = "DRAINING"
	HealthUnhealthy = "UNHEALTHY"
)

// Intent lifecycle states.
const (
	IntentPending   = "PENDING"
	IntentCommitted = "COMMITTED"
	IntentAborted   = "ABORTED"
)

const (
	RoundRunning   = "RUNNING"
	RoundCommitted = "COMMITTED"
	RoundRecovered = "RECOVERED"
	RoundAborted   = "ABORTED"
)

const (
	BatchWaiting    = "WAITING"
	BatchQueued     = "QUEUED"
	BatchRunning    = "RUNNING"
	BatchDowngraded = "DOWNGRADED"
	BatchCompleted  = "COMPLETED"
	BatchAborted    = "ABORTED"
)

// ---------------------------------------------------------------------------
// External DTOs (use human quantities such as "40Gi" / "250m").
// ---------------------------------------------------------------------------

type GroupSpec struct {
	ID           string `json:"id"`
	MemoryLimit  string `json:"memory_limit"`
	ComputeLimit string `json:"compute_limit_milli"`
}

type NodeSpec struct {
	ID           string `json:"id"`
	GroupID      string `json:"group_id"`
	MemoryTotal  string `json:"memory_total"`
	ComputeTotal string `json:"compute_total_milli"`
	Health       string `json:"health"`
}

type RequestSpec struct {
	ID              string `json:"id"`
	ModelID         string `json:"model_id"`
	GroupID         string `json:"group_id"`
	MemoryNeed      string `json:"memory_need"`
	ComputeNeed     string `json:"compute_need_milli"`
	SamplesPerBatch int64  `json:"samples_per_batch"`
	LatencyLimitMs  int64  `json:"latency_limit_ms"`
	Priority        int    `json:"priority"`
}

type PolicyParams struct {
	GlobalMaxConcurrent int64            `json:"global_max_concurrent_batches"`
	WaitQueueCapacity   int              `json:"wait_queue_capacity"`
	RoundBudgetMs       int64            `json:"round_budget_ms"`
	LatencyBaseMs       int64            `json:"latency_base_ms"`
	LatencyPerSampleMs  float64          `json:"latency_per_sample_ms"`
	MaxSamplesPerModel  map[string]int64 `json:"max_samples_per_model"`
}

type PolicySnapshotSpec struct {
	Version    int64        `json:"version"`
	Params     PolicyParams `json:"params"`
	PrevDigest string       `json:"prev_digest,omitempty"`
}

type FragmentSpec struct {
	Version    int64          `json:"version"`
	Fields     map[string]any `json:"fields"`
	BaseDigest string         `json:"base_digest"`
}

type LoadSpec struct {
	Groups    []GroupSpec          `json:"groups"`
	Nodes     []NodeSpec           `json:"nodes"`
	Requests  []RequestSpec        `json:"requests"`
	Snapshots []PolicySnapshotSpec `json:"snapshots"`
	Fragments []FragmentSpec       `json:"fragments"`
}

// ---------------------------------------------------------------------------
// Normalized domain structs (absolute integer quantities).
// ---------------------------------------------------------------------------

type Group struct {
	ID              string `json:"id"`
	MemoryLimit     int64  `json:"memory_limit_bytes"`
	ComputeLimitM   int64  `json:"compute_limit_milli"`
	MemoryReserved  int64  `json:"memory_reserved_bytes"`
	ComputeReserved int64  `json:"compute_reserved_milli"`
	PickCounter     int64  `json:"pick_counter"`
}

type Node struct {
	ID            string `json:"id"`
	GroupID       string `json:"group_id"`
	MemoryTotal   int64  `json:"memory_total_bytes"`
	ComputeTotalM int64  `json:"compute_total_milli"`
	MemoryUsed    int64  `json:"memory_used_bytes"`
	ComputeUsedM  int64  `json:"compute_used_milli"`
	Health        string `json:"health"`
}

type Request struct {
	ID              string `json:"id"`
	ModelID         string `json:"model_id"`
	GroupID         string `json:"group_id"`
	MemoryNeed      int64  `json:"memory_need_bytes"`
	ComputeNeedM    int64  `json:"compute_need_milli"`
	SamplesPerBatch int64  `json:"samples_per_batch"`
	LatencyLimitMs  int64  `json:"latency_limit_ms"`
	Priority        int    `json:"priority"`
	State           string `json:"state"`
	Seq             int64  `json:"seq"`
	AssignedNodeID  string `json:"assigned_node_id"`
	PolicyVersion   int64  `json:"policy_version"`
}

type PolicySnapshot struct {
	Version    int64        `json:"version"`
	Digest     string       `json:"digest"`
	PrevDigest string       `json:"prev_digest"`
	Params     PolicyParams `json:"params"`
}

type Fragment struct {
	Version    int64
	BaseDigest string
	Digest     string
	Fields     map[string]any
}

type Batch struct {
	ID           string   `json:"id"`
	RoundID      int64    `json:"round_id"`
	ModelID      string   `json:"model_id"`
	NodeID       string   `json:"node_id"`
	GroupID      string   `json:"group_id"`
	Priority     int      `json:"priority"`
	Status       string   `json:"status"`
	MemberIDs    []string `json:"member_ids"`
	TotalSamples int64    `json:"total_samples"`
	TotalMemory  int64    `json:"total_memory_bytes"`
	EstLatencyMs int64    `json:"est_latency_ms"`
	Seq          int64    `json:"seq"`
}

type Intent struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	RoundID   int64  `json:"round_id"`
	RequestID string `json:"request_id"`
	BatchID   string `json:"batch_id"`
	Payload   string `json:"payload"`
	Status    string `json:"status"`
}

// QuotaView is the read-model returned by quota queries.
type QuotaView struct {
	GroupID          string      `json:"group_id"`
	MemoryLimit      int64       `json:"memory_limit_bytes"`
	MemoryReserved   int64       `json:"memory_reserved_bytes"`
	MemoryFree       int64       `json:"memory_free_bytes"`
	ComputeLimitM    int64       `json:"compute_limit_milli"`
	ComputeReservedM int64       `json:"compute_reserved_milli"`
	ComputeFreeM     int64       `json:"compute_free_milli"`
	Nodes            []NodeQuota `json:"nodes"`
}

type NodeQuota struct {
	NodeID        string `json:"node_id"`
	Health        string `json:"health"`
	MemoryTotal   int64  `json:"memory_total_bytes"`
	MemoryUsed    int64  `json:"memory_used_bytes"`
	MemoryFree    int64  `json:"memory_free_bytes"`
	ComputeTotalM int64  `json:"compute_total_milli"`
	ComputeUsedM  int64  `json:"compute_used_milli"`
	ComputeFreeM  int64  `json:"compute_free_milli"`
}

// QuotaExceedDetail is embedded into E_QUOTA_EXCEEDED responses.
type QuotaExceedDetail struct {
	GroupID     string `json:"group_id"`
	Resource    string `json:"resource"`
	CurrentUsed int64  `json:"current_used"`
	Limit       int64  `json:"limit"`
	Requested   int64  `json:"requested"`
	Shortfall   int64  `json:"shortfall"`
}
