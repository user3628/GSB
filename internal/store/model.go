package store

import (
	"encoding/json"
	"math/big"

	"blocksnap/internal/metrics"
)

type Volume struct {
	ID            string `json:"id"`
	BlockCount    int64  `json:"block_count"`
	BlockSize     int64  `json:"block_size"`
	OverheadBytes int64  `json:"writeback_overhead_bytes_per_block"`
	LastSeq       int64  `json:"last_seq"`
	CapacityBytes string `json:"capacity_bytes"`
	CreatedAt     string `json:"created_at"`
}

type CreateVolumeRequest struct {
	VolumeID      string
	RequestID     string
	RequestHash   string
	BlockCount    int64
	BlockSize     int64
	OverheadBytes int64
}

type CreateVolumeResult struct {
	RequestID string `json:"request_id"`
	Volume    Volume `json:"volume"`
}

type BlockView struct {
	BlockNo   int64  `json:"block_no"`
	Version   int64  `json:"version"`
	WrittenAt string `json:"written_at"`
	Digest    string `json:"sha256"`
	Zero      bool   `json:"is_zero"`
	SizeBytes int64  `json:"size_bytes"`
}

type WriteBlockRequest struct {
	VolumeID        string
	BlockNo         int64
	RequestID       string
	RequestHash     string
	ExpectedVersion int64
	Content         []byte
	Digest          string
	WrittenAt       string
}

type WriteBlockResult struct {
	RequestID string    `json:"request_id"`
	VolumeID  string    `json:"volume_id"`
	Block     BlockView `json:"block"`
}

type CreateSnapshotRequest struct {
	VolumeID    string
	SnapshotID  string
	RequestID   string
	RequestHash string
	Kind        string
	ParentID    string
	Weight      int64
	CreatedAt   string
}

type SnapshotView struct {
	ID              string `json:"id"`
	Kind            string `json:"kind"`
	ParentID        string `json:"parent_id,omitempty"`
	ConsistencySeq  int64  `json:"consistency_seq"`
	Weight          int64  `json:"weight"`
	Ordinal         int64  `json:"ordinal"`
	CreatedAt       string `json:"created_at"`
	DeltaBlocks     int64  `json:"delta_blocks"`
	EffectiveBlocks int64  `json:"effective_blocks"`
}

type CreateSnapshotResult struct {
	RequestID string       `json:"request_id"`
	Snapshot  SnapshotView `json:"snapshot"`
}

type MergeRequest struct {
	VolumeID    string
	RequestID   string
	RequestHash string
	ParentID    string
	ChildID     string
}

type MergeResult struct {
	RequestID          string       `json:"request_id"`
	MergedInto         string       `json:"merged_into"`
	Removed            string       `json:"removed_snapshot"`
	ParentWeight       int64        `json:"parent_weight"`
	RewrittenRefs      int64        `json:"rewritten_parent_refs"`
	ReparentedChildren int64        `json:"reparented_children"`
	Snapshot           SnapshotView `json:"snapshot"`
}

type DeleteSnapshotRequest struct {
	VolumeID    string
	SnapshotID  string
	RequestID   string
	RequestHash string
}

type DeleteSnapshotResult struct {
	RequestID string `json:"request_id"`
	Deleted   string `json:"deleted_snapshot"`
}

type DecisionInput struct {
	MarginFloor       *big.Int
	CostBudgetBytes   *big.Int
	RelativeTolerance *big.Int
}

type RollbackRequest struct {
	VolumeID    string
	SnapshotID  string
	RequestID   string
	RequestHash string
	Decision    DecisionInput
}

type RollbackResult struct {
	RequestID         string           `json:"request_id"`
	VolumeID          string           `json:"volume_id"`
	SnapshotID        string           `json:"snapshot_id"`
	Decision          metrics.Decision `json:"decision"`
	WrittenBackBlocks int64            `json:"written_back_blocks"`
	WrittenBackBytes  string           `json:"written_back_bytes"`
	NewLiveSeq        int64            `json:"new_live_seq"`
	Analysis          metrics.Analysis `json:"analysis"`
}

type RecoveryReport struct {
	ID             int64           `json:"id"`
	StartedAt      string          `json:"started_at"`
	FinishedAt     string          `json:"finished_at"`
	Status         string          `json:"status"`
	PendingFound   int64           `json:"pending_found"`
	AbortedCount   int64           `json:"aborted_count"`
	CommittedCount int64           `json:"committed_count"`
	Details        json.RawMessage `json:"details"`
}
