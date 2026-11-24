package domain

import "time"

const FixedScale int64 = 1_000_000

type Precision string

const (
	PrecisionFP16 Precision = "FP16"
	PrecisionFP32 Precision = "FP32"
)

func (p Precision) Valid() bool {
	return p == PrecisionFP16 || p == PrecisionFP32
}

type Pool string

const (
	PoolVRAM       Pool = "VRAM"
	PoolPersistent Pool = "PERSISTENT"
	PoolPending    Pool = "PENDING"
)

type CopyStatus string

const (
	StatusResident  CopyStatus = "RESIDENT"
	StatusPersisted CopyStatus = "PERSISTED"
	StatusPending   CopyStatus = "PENDING"
	StatusAborted   CopyStatus = "ABORTED"
)

const RestorationContract = "master_fixed_micros * loss_scale -> IEEE754 binary16 round-to-nearest-even -> restore_fixed_micros = round-half-away(half / loss_scale)"

type Placement struct {
	ShardID      string    `json:"shard_id"`
	Precision    Precision `json:"precision"`
	Pool         Pool      `json:"pool"`
	LastAccessNS int64     `json:"last_access_ns"`
}

type PolicySnapshot struct {
	Version                int64       `json:"version"`
	GeneratedAt            time.Time   `json:"generated_at"`
	VRAMPoolBytes          int64       `json:"vram_pool_bytes"`
	WriteBudgetBytes       int64       `json:"write_budget_bytes"`
	RoundTimeBudgetMS      int64       `json:"round_time_budget_ms"`
	WriteQueueCapacity     int         `json:"write_queue_capacity"`
	TaskTimeMS             int64       `json:"task_time_ms"`
	LossScaleFixed         int64       `json:"loss_scale_fixed"`
	GrowthFactorFixed      int64       `json:"growth_factor_fixed"`
	OverflowThresholdFixed int64       `json:"overflow_threshold_fixed"`
	Placements             []Placement `json:"placements"`
	Checksum               string      `json:"checksum"`
}

type Policy struct {
	PolicySnapshot
	Source      string `json:"source"`
	BaseVersion int64  `json:"base_version,omitempty"`
}

type LossScalePatch struct {
	Action                 string `json:"action"`
	ValueFixed             *int64 `json:"value_fixed,omitempty"`
	GrowthFactorFixed      *int64 `json:"growth_factor_fixed,omitempty"`
	OverflowThresholdFixed *int64 `json:"overflow_threshold_fixed,omitempty"`
}

type DeltaFragment struct {
	BaseVersion int64           `json:"base_version"`
	GeneratedAt time.Time       `json:"generated_at"`
	Placements  []Placement     `json:"placements"`
	LossScale   *LossScalePatch `json:"loss_scale,omitempty"`
	Checksum    string          `json:"checksum"`
}

type CheckpointRequest struct {
	Round              int64  `json:"round"`
	WriteQueueCapacity int    `json:"write_queue_capacity,omitempty"`
	TaskTimeMS         int64  `json:"task_time_ms,omitempty"`
	FaultAfterIntent   string `json:"fault_after_intent,omitempty"`
}

type PartitionInput struct {
	CopyID         string    `json:"copy_id"`
	ShardID        string    `json:"shard_id"`
	Round          int64     `json:"round"`
	Precision      Precision `json:"precision"`
	SizeBytes      int64     `json:"size_bytes"`
	Digest         string    `json:"digest"`
	MasterCopyID   string    `json:"master_copy_id,omitempty"`
	MasterDigest   string    `json:"master_digest,omitempty"`
	RestoredDigest string    `json:"restored_digest,omitempty"`
	Restoration    string    `json:"restoration,omitempty"`
	MomentumDigest string    `json:"momentum_digest,omitempty"`
}

type PartitionBatch struct {
	Partitions []PartitionInput `json:"partitions"`
}

type PartitionValidation struct {
	Valid          bool  `json:"valid"`
	PartitionCount int   `json:"partition_count"`
	TotalBytes     int64 `json:"total_bytes"`
	FP16Count      int   `json:"fp16_count"`
	FP32Count      int   `json:"fp32_count"`
}

type LogicalShard struct {
	ShardID        string
	Round          int64
	Priority       int64
	FP32SizeBytes  int64
	FP16SizeBytes  int64
	ValuesFixed    []int64
	MomentumDigest string
}

type Copy struct {
	CopyID         string     `json:"copy_id"`
	ShardID        string     `json:"shard_id"`
	Round          int64      `json:"round"`
	Precision      Precision  `json:"precision"`
	SizeBytes      int64      `json:"size_bytes"`
	Digest         string     `json:"digest"`
	MasterCopyID   string     `json:"master_copy_id,omitempty"`
	MasterDigest   string     `json:"master_digest"`
	MomentumDigest string     `json:"momentum_digest"`
	Pool           Pool       `json:"pool"`
	Status         CopyStatus `json:"status"`
	LastAccessNS   int64      `json:"last_access_ns"`
	Priority       int64      `json:"priority"`
	LossScaleFixed int64      `json:"loss_scale_fixed"`
	Restoration    string     `json:"restoration,omitempty"`
	WriteState     string     `json:"write_state"`
	ValuesFixed    []int64    `json:"-"`
	FP16Bits       []uint16   `json:"-"`
}

type Eviction struct {
	CopyID         string     `json:"copy_id"`
	ShardID        string     `json:"shard_id"`
	FromPool       Pool       `json:"from_pool"`
	ToPool         Pool       `json:"to_pool"`
	PreviousStatus CopyStatus `json:"previous_status"`
	NewStatus      CopyStatus `json:"new_status"`
	Reason         string     `json:"reason"`
	Bytes          int64      `json:"bytes"`
}

type WriteTask struct {
	CopyID    string    `json:"copy_id"`
	ShardID   string    `json:"shard_id"`
	Precision Precision `json:"precision"`
	Bytes     int64     `json:"bytes"`
	Priority  int64     `json:"priority"`
	Round     int64     `json:"round"`
}

type SkippedWrite struct {
	WriteTask
	Reason string `json:"reason"`
}

type WriteSummary struct {
	BudgetBytes     int64          `json:"budget_bytes"`
	UsedBytes       int64          `json:"used_bytes"`
	RemainingBytes  int64          `json:"remaining_bytes"`
	TimeBudgetMS    int64          `json:"time_budget_ms"`
	TimeUsedMS      int64          `json:"time_used_ms"`
	TimeRemainingMS int64          `json:"time_remaining_ms"`
	Written         []WriteTask    `json:"written"`
	Skipped         []SkippedWrite `json:"skipped"`
	QueueCapacity   int            `json:"queue_capacity"`
	Queued          []SkippedWrite `json:"queued"`
}

type PoolSummary struct {
	Pool           Pool  `json:"pool"`
	CopyCount      int   `json:"copy_count"`
	TotalBytes     int64 `json:"total_bytes"`
	LimitBytes     int64 `json:"limit_bytes,omitempty"`
	RemainingBytes int64 `json:"remaining_bytes,omitempty"`
}

type CheckpointResult struct {
	Version       int64         `json:"checkpoint_version"`
	PolicyVersion int64         `json:"policy_version"`
	Round         int64         `json:"round"`
	CommittedAt   time.Time     `json:"committed_at"`
	IntentID      string        `json:"intent_id,omitempty"`
	FaultInjected string        `json:"fault_injected,omitempty"`
	Evictions     []Eviction    `json:"evictions"`
	Writes        WriteSummary  `json:"writes"`
	Pools         []PoolSummary `json:"pools"`
}

type PrecisionShardReport struct {
	ShardID                     string             `json:"shard_id"`
	FP16CopyID                  string             `json:"fp16_copy_id"`
	MasterCopyID                string             `json:"master_copy_id"`
	ElementCount                int                `json:"element_count"`
	LossScaleFixed              int64              `json:"loss_scale_fixed"`
	Restoration                 string             `json:"restoration"`
	MaxAbsDeviationFixed        int64              `json:"max_abs_deviation_fixed"`
	MaxRelativeDeviationPPM     int64              `json:"max_relative_deviation_ppm"`
	CumulativeAbsDeviationFixed int64              `json:"cumulative_abs_deviation_fixed"`
	ConsistencyOK               bool               `json:"consistency_ok"`
	Elements                    []ElementDeviation `json:"elements"`
}

type ElementDeviation struct {
	MasterFixed    int64  `json:"master_fixed"`
	FP16Bits       uint16 `json:"fp16_bits"`
	RestoredFixed  int64  `json:"restored_fixed"`
	DeviationFixed int64  `json:"deviation_fixed"`
}

type PrecisionReport struct {
	Version                     int64                  `json:"version"`
	PolicyVersion               int64                  `json:"policy_version"`
	Shards                      []PrecisionShardReport `json:"shards"`
	CheckedShardCount           int                    `json:"checked_shard_count"`
	CumulativeAbsDeviationFixed int64                  `json:"cumulative_abs_deviation_fixed"`
	MaxAbsDeviationFixed        int64                  `json:"max_abs_deviation_fixed"`
	MaxRelativeDeviationPPM     int64                  `json:"max_relative_deviation_ppm"`
}

type RecoveryBaseline struct {
	Precision      Precision `json:"precision"`
	MasterDigest   string    `json:"master_digest"`
	RestoredDigest string    `json:"restored_digest,omitempty"`
	MomentumDigest string    `json:"momentum_digest"`
	LossScaleFixed int64     `json:"loss_scale_fixed"`
	Restoration    string    `json:"restoration,omitempty"`
}

type DerivedCopy struct {
	CopyID              string           `json:"copy_id"`
	MasterDerivedDigest string           `json:"master_derived_digest"`
	ConsistencyOK       bool             `json:"consistency_ok"`
	VRAMEstimateBytes   int64            `json:"vram_estimate_bytes"`
	RecoveryBaseline    RecoveryBaseline `json:"recovery_baseline"`
	DependencyRefs      []string         `json:"dependency_refs"`
}

type AbortedOperation struct {
	OperationID           string    `json:"operation_id"`
	Phase                 string    `json:"phase"`
	BaseCheckpointVersion int64     `json:"base_checkpoint_version"`
	RecoveredAt           time.Time `json:"recovered_at"`
	PendingShards         []string  `json:"pending_shards"`
	AbortedEvictions      []string  `json:"aborted_evictions"`
}

type RecoveryReport struct {
	Version              int64              `json:"version"`
	PolicyVersion        int64              `json:"policy_version"`
	Round                int64              `json:"round"`
	CommittedOnly        bool               `json:"committed_only"`
	VRAMTotalBytes       int64              `json:"vram_total_bytes"`
	PersistentTotalBytes int64              `json:"persistent_total_bytes"`
	CopyCount            int                `json:"copy_count"`
	Checksum             string             `json:"checksum"`
	Baselines            []DerivedCopy      `json:"baselines"`
	AbortedOperations    []AbortedOperation `json:"aborted_operations"`
}

type CheckpointMeta struct {
	Version        int64     `json:"version"`
	PolicyVersion  int64     `json:"policy_version"`
	Round          int64     `json:"round"`
	CommittedAt    time.Time `json:"committed_at"`
	VRAMLimitBytes int64     `json:"vram_limit_bytes"`
	Checksum       string    `json:"checksum"`
}

type ShardsResponse struct {
	Version       int64  `json:"version"`
	PolicyVersion int64  `json:"policy_version"`
	Round         int64  `json:"round"`
	Copies        []Copy `json:"copies"`
}
