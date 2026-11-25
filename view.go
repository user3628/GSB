package convergence

import "time"

type Summary struct {
	SnapshotVersion  int64          `json:"snapshot_version"`
	PolicyVersion    int64          `json:"policy_version"`
	ObjectCount      int            `json:"object_count"`
	ClusterCount     int            `json:"cluster_count"`
	DriftedCount     int            `json:"drifted_count"`
	ConvergedCount   int            `json:"converged_count"`
	CategoryCounts   map[string]int `json:"category_counts"`
	LayerCount       int            `json:"layer_count"`
	BatchCount       int            `json:"batch_count"`
	SplitBatchCount  int            `json:"split_batch_count"`
	CycleCount       int            `json:"cycle_count"`
	BatchesPending   int            `json:"batches_pending"`
	BatchesApplying  int            `json:"batches_applying"`
	BatchesSucceeded int            `json:"batches_succeeded"`
	BatchesFailed    int            `json:"batches_failed"`
}

type ObjectApplyResult struct {
	ObjectID       string       `json:"object_id"`
	ClusterID      string       `json:"cluster_id"`
	Status         string       `json:"status"`
	Attempts       int          `json:"attempts"`
	HTTPStatus     int          `json:"http_status,omitempty"`
	ReadbackFields FieldSet     `json:"readback_fields,omitempty"`
	ReadbackDrift  *DriftReport `json:"readback_drift,omitempty"`
	Error          *Error       `json:"error,omitempty"`
}

type BatchApplyResult struct {
	BatchID       string              `json:"batch_id"`
	Version       int64               `json:"version"`
	ContentHash   string              `json:"content_hash"`
	RequestHash   string              `json:"request_hash"`
	Status        string              `json:"status"`
	Attempts      int                 `json:"attempts"`
	ObjectResults []ObjectApplyResult `json:"object_results"`
	Error         *Error              `json:"error,omitempty"`
	AppliedAt     time.Time           `json:"applied_at,omitempty"`
}

type BatchState struct {
	Batch         Batch               `json:"batch"`
	Status        string              `json:"status"`
	ContentHash   string              `json:"content_hash"`
	RequestHash   string              `json:"request_hash,omitempty"`
	Attempts      int                 `json:"attempts"`
	ObjectResults []ObjectApplyResult `json:"object_results,omitempty"`
	Error         *Error              `json:"error,omitempty"`
	UpdatedAt     time.Time           `json:"updated_at,omitempty"`
}

type CascadeReport struct {
	FromVersion         int64    `json:"from_version"`
	ToVersion           int64    `json:"to_version"`
	ChangedObjectIDs    []string `json:"changed_object_ids"`
	InvalidatedDriftIDs []string `json:"invalidated_drift_ids"`
	InvalidatedBatchIDs []string `json:"invalidated_batch_ids"`
	RebuiltObjectCount  int      `json:"rebuilt_object_count"`
	RebuiltBatchCount   int      `json:"rebuilt_batch_count"`
	Reason              string   `json:"reason"`
	AtomicBoundary      string   `json:"atomic_boundary"`
}

// CommittedVersion is the immutable MVCC root observed by all read queries.
type CommittedVersion struct {
	Snapshot    *Snapshot              `json:"snapshot"`
	DriftList   []DriftReport          `json:"drift_reports"`
	DriftByID   map[string]DriftReport `json:"-"`
	Plan        *Plan                  `json:"plan"`
	Summary     Summary                `json:"summary"`
	BatchStates map[string]*BatchState `json:"batch_states"`
	Cascade     *CascadeReport         `json:"cascade,omitempty"`
}

type ApplyBatchRequest struct {
	BatchID     string `json:"batch_id"`
	RequestHash string `json:"request_hash,omitempty"`
	ContentHash string `json:"content_hash,omitempty"`
	Content     any    `json:"content,omitempty"`
}

type CommitResult struct {
	Version int64             `json:"version"`
	Current *CommittedVersion `json:"-"`
	Plan    *Plan             `json:"plan,omitempty"`
	Cascade *CascadeReport    `json:"cascade,omitempty"`
}
