package engine

import "ropechunk/internal/model"

type ImportReport struct {
	Transition    model.ConfigTransition    `json:"transition"`
	Invalidations []model.InvalidationEvent `json:"invalidations"`
}

type ComputeOptions struct {
	Stage    string
	ShardIDs []string
	RunID    string
	Note     string
	// HookAfterPublish is a deterministic test/integration hook. It runs after
	// the checkpoint transaction commits, so cancelling from the hook models a
	// force-kill that leaves only durable facts behind.
	HookAfterPublish func(stage, shardID string, batch, head, rowBlock int, sequence int64)
}

type TaskOutcome struct {
	ShardID     string          `json:"shard_id"`
	Stage       string          `json:"stage"`
	Batch       int             `json:"batch"`
	Head        int             `json:"head"`
	RowBlock    int             `json:"row_block"`
	RowStart    int             `json:"row_start"`
	RowEnd      int             `json:"row_end"`
	Status      string          `json:"status"`
	Reason      string          `json:"reason,omitempty"`
	EstimatedUS int64           `json:"estimated_us"`
	RemainingUS int64           `json:"remaining_us"`
	Sequence    int64           `json:"sequence,omitempty"`
	Error       *model.AppError `json:"error,omitempty"`
}

type RoundReport struct {
	Round       int           `json:"round"`
	Stage       string        `json:"stage"`
	ShardID     string        `json:"shard_id"`
	BatchStep   int           `json:"batch_step"`
	Workers     int           `json:"workers"`
	WorkerQuota int           `json:"worker_quota"`
	Capacity    int           `json:"capacity"`
	BudgetUS    int64         `json:"budget_us"`
	UsedUS      int64         `json:"used_us"`
	RemainingUS int64         `json:"remaining_us"`
	Processed   int           `json:"processed"`
	Reused      int           `json:"reused"`
	Unprocessed int           `json:"unprocessed"`
	Outcomes    []TaskOutcome `json:"outcomes"`
}

type StageExecution struct {
	Stage       string            `json:"stage"`
	Fingerprint string            `json:"fingerprint"`
	Plan        model.MemoryPlan  `json:"memory_plan"`
	Rounds      []RoundReport     `json:"rounds"`
	Tasks       []TaskOutcome     `json:"tasks"`
	Processed   int               `json:"processed"`
	Reused      int               `json:"reused"`
	Unprocessed int               `json:"unprocessed"`
	Errors      []*model.AppError `json:"errors,omitempty"`
	Error       *model.AppError   `json:"error,omitempty"`
}

type ShardExecution struct {
	ShardID      string             `json:"shard_id"`
	Shape        model.Shape        `json:"shape"`
	Fingerprints model.Fingerprints `json:"fingerprints"`
	Stages       []StageExecution   `json:"stages"`
}

type ComputeReport struct {
	RunID         string                    `json:"run_id"`
	ConfigVersion int64                     `json:"config_version"`
	ConfigDigest  string                    `json:"config_digest"`
	Stage         string                    `json:"stage"`
	StartedAt     string                    `json:"started_at"`
	FinishedAt    string                    `json:"finished_at"`
	Stopped       bool                      `json:"stopped"`
	Shards        []ShardExecution          `json:"shards"`
	Invalidations []model.InvalidationEvent `json:"invalidations"`
	Processed     int                       `json:"processed"`
	Reused        int                       `json:"reused"`
	Unprocessed   int                       `json:"unprocessed"`
	Errors        []*model.AppError         `json:"errors,omitempty"`
}

type StageStatus struct {
	Stage          string `json:"stage"`
	Fingerprint    string `json:"fingerprint"`
	ExpectedRows   int    `json:"expected_rows"`
	CompleteRows   int    `json:"complete_rows"`
	IncompleteRows int    `json:"incomplete_rows"`
	StaleRows      int    `json:"stale_rows"`
}

type ShardStatus struct {
	ShardID      string             `json:"shard_id"`
	Shape        model.Shape        `json:"shape"`
	Fingerprints model.Fingerprints `json:"fingerprints"`
	Stages       []StageStatus      `json:"stages"`
}

type StatusReport struct {
	Config       model.Config  `json:"config"`
	ConfigDigest string        `json:"config_digest"`
	Shards       []ShardStatus `json:"shards"`
}

type ReferenceVerification struct {
	ShardID         string `json:"shard_id"`
	CheckedElements int    `json:"checked_elements"`
	Mismatches      int    `json:"mismatches"`
	FirstMismatch   string `json:"first_mismatch,omitempty"`
	DigestMatch     bool   `json:"digest_match"`
}

type VerificationReport struct {
	ConfigDigest string                  `json:"config_digest"`
	Shards       []ReferenceVerification `json:"shards"`
	AllMatch     bool                    `json:"all_match"`
}
