package engine

import (
	"time"

	"driftengine/internal/batch"
	"driftengine/internal/drift"
	"driftengine/internal/spec"
)

const (
	PhaseObservedInSync = "observed_in_sync"
	PhaseDriftDetected  = "drift_detected"
	PhasePendingWrite   = "pending_write"
	PhaseWriteTimeout   = "write_timeout"
	PhaseConverged      = "converged"
	PhaseRetryExhausted = "retry_exhausted"
	PhaseFailed         = "failed"
)

type ObjectState struct {
	ObjectID        string             `json:"object_id"`
	Phase           string             `json:"phase"`
	PrimaryCategory string             `json:"primary_category"`
	HitCategories   []string           `json:"hit_categories,omitempty"`
	Attempts        int                `json:"attempts"`
	MaxRetries      int                `json:"max_retries"`
	LastErrorCode   string             `json:"last_error_code,omitempty"`
	LastMessage     string             `json:"last_message,omitempty"`
	Differences     []drift.Difference `json:"differences,omitempty"`
	FieldDigest     string             `json:"field_digest,omitempty"`
	ReadbackDigest  string             `json:"readback_fields_digest,omitempty"`
	IntentID        string             `json:"intent_id,omitempty"`
	UpdatedAt       time.Time          `json:"updated_at"`
}

type BatchState struct {
	BatchID      string            `json:"batch_id"`
	BatchNumber  int               `json:"batch_number"`
	RequestHash  string            `json:"request_hash"`
	Status       string            `json:"status"`
	TriggerCount int               `json:"trigger_count"`
	ObjectIDs    []string          `json:"object_ids"`
	ObjectPhases map[string]string `json:"object_phases"`
	ErrorCode    string            `json:"error_code,omitempty"`
	StartedAt    *time.Time        `json:"started_at,omitempty"`
	FinishedAt   *time.Time        `json:"finished_at,omitempty"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

type Summary struct {
	Version               int64          `json:"version"`
	SnapshotDigest        string         `json:"snapshot_digest"`
	RefsDigest            string         `json:"refs_digest"`
	ObjectRefCount        int            `json:"object_ref_count"`
	EdgeRefCount          int            `json:"edge_ref_count"`
	TotalObjects          int            `json:"total_objects"`
	ObservedInSync        int            `json:"observed_in_sync"`
	DriftedObjects        int            `json:"drifted_objects"`
	VerifiedConverged     int            `json:"verified_converged"`
	PendingWrite          int            `json:"pending_write"`
	FailedObjects         int            `json:"failed_objects"`
	PhaseCounts           map[string]int `json:"phase_counts"`
	PrimaryCategoryCounts map[string]int `json:"primary_category_counts"`
	BatchCount            int            `json:"batch_count"`
	BatchStatusCounts     map[string]int `json:"batch_status_counts"`
	DependencyCycle       bool           `json:"dependency_cycle"`
	CommittedAt           time.Time      `json:"committed_at"`
}

type View struct {
	Version      int64
	BaseVersion  int64
	Kind         string
	Snapshot     *spec.Snapshot
	Drift        map[string]drift.Report
	Plan         *batch.Plan
	Summary      Summary
	ObjectStates map[string]*ObjectState
	BatchStates  map[string]*BatchState
	CommittedAt  time.Time
}

type RecoveredIntent struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Version   int64  `json:"version"`
	BatchID   string `json:"batch_id,omitempty"`
	ObjectID  string `json:"object_id,omitempty"`
	OldStatus string `json:"old_status"`
	NewStatus string `json:"new_status"`
	Action    string `json:"action"`
}

type RecoveryObject struct {
	Version                        int64              `json:"version"`
	ObjectID                       string             `json:"object_id"`
	FinalStatusBasedOnDurableFacts string             `json:"final_status_based_on_durable_facts"`
	Attempts                       int                `json:"attempts"`
	ErrorCode                      string             `json:"error_code,omitempty"`
	Differences                    []drift.Difference `json:"differences"`
}

type RecoveryReport struct {
	RecoveredAt             time.Time         `json:"recovered_at"`
	CurrentCommittedVersion int64             `json:"current_committed_version"`
	OpenIntentCount         int               `json:"open_intent_count"`
	RecoveredIntents        []RecoveredIntent `json:"recovered_intents"`
	Objects                 []RecoveryObject  `json:"objects"`
	ExitReportClaimed       bool              `json:"exit_report_claimed"`
	ExternalStateReconciled bool              `json:"external_state_reconciled"`
	Statement               string            `json:"statement"`
}

const recoveryStatement = "Recovery is based only on durable SQLite facts. An interrupted HTTP write may have been accepted remotely; local write intents are therefore pending_write and must be read back before convergence. No exit report is claimed."
