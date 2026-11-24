package domain

import "time"

const (
	StateAvailable = "available"
	StateInUse     = "in_use"
	StateInvalid   = "invalid"
	StateClosed    = "closed"
)

const (
	DispositionIdle    = "idle"
	DispositionClosed  = "closed"
	DispositionHandoff = "handed_off"
	DispositionInvalid = "invalidated"
)

const (
	IntentAcquire    = "acquire"
	IntentRelease    = "release"
	IntentInvalidate = "invalidate"
	IntentClose      = "close"
	IntentConfig     = "config_change"
)

const (
	IntentPending   = "pending"
	IntentCommitted = "committed"
	IntentRecovered = "recovered"
	IntentAborted   = "aborted"
)

// ConnectionRecord is the durable identity of a logical pooled connection.
// It intentionally survives the underlying handle; recovery reports distinguish
// restored metadata from non-restorable handles and FIFO order.
type ConnectionRecord struct {
	ConnectionID      string    `json:"connection_id"`
	State             string    `json:"state"`
	CreatedAt         time.Time `json:"created_at"`
	LastUsedAt        time.Time `json:"last_used_at"`
	UseCount          int64     `json:"use_count"`
	CloseReason       string    `json:"close_reason,omitempty"`
	ReplacedBy        string    `json:"replaced_by,omitempty"`
	ReplacedConnID    string    `json:"replaced_connection_id,omitempty"`
	ReplacementReason string    `json:"replacement_reason,omitempty"`
}

type Lease struct {
	ConnectionID   string    `json:"connection_id"`
	CreatedAt      time.Time `json:"created_at"`
	LastUsedAt     time.Time `json:"last_used_at"`
	AcquiredAt     time.Time `json:"acquired_at"`
	UseCount       int64     `json:"use_count"`
	WaitDurationMS int64     `json:"wait_duration_ms"`
	WaiterID       string    `json:"waiter_id"`
}

type ReleaseResult struct {
	ConnectionID string    `json:"connection_id"`
	State        string    `json:"state"`
	Disposition  string    `json:"disposition"`
	CreatedAt    time.Time `json:"created_at"`
	ReleasedAt   time.Time `json:"released_at"`
	UseCount     int64     `json:"use_count"`
	CloseReason  string    `json:"close_reason,omitempty"`
	HandoffTo    string    `json:"handoff_to,omitempty"`
}

type QueryResult struct {
	Columns      []string `json:"columns,omitempty"`
	Rows         [][]any  `json:"rows,omitempty"`
	RowsAffected int64    `json:"rows_affected,omitempty"`
}

type Event struct {
	Seq          int64          `json:"seq"`
	Type         string         `json:"type"`
	At           time.Time      `json:"at"`
	ConnectionID string         `json:"connection_id,omitempty"`
	WaiterID     string         `json:"waiter_id,omitempty"`
	Detail       map[string]any `json:"detail,omitempty"`
}

type Metrics struct {
	AcquireSuccesses int64 `json:"acquire_successes"`
	AcquireTimeouts  int64 `json:"acquire_timeouts"`
	QueueOverflows   int64 `json:"queue_overflows"`
	DialFailures     int64 `json:"dial_failures"`
	ConnResets       int64 `json:"conn_resets"`
	ReleaseHandoffs  int64 `json:"release_handoffs"`
	IdleClosed       int64 `json:"idle_closed"`
	ExpiredClosed    int64 `json:"expired_closed"`
}

type PoolStats struct {
	SnapshotVersion  int64    `json:"snapshot_version"`
	Config           Config   `json:"config"`
	Active           int      `json:"active"`
	InUse            int      `json:"in_use"`
	Idle             int      `json:"idle"`
	Dialing          int      `json:"dialing"`
	Waiting          int      `json:"waiting"`
	WaitingCapacity  int      `json:"waiting_capacity"`
	KnownConnections int      `json:"known_connections"`
	IdleOrder        []string `json:"idle_order"`
	Metrics          Metrics  `json:"metrics"`
}

type PoolSnapshot struct {
	Stats       PoolStats          `json:"stats"`
	Connections []ConnectionRecord `json:"connections"`
	Events      []Event            `json:"events"`
}

type Intent struct {
	ID           int64          `json:"intent_id"`
	Kind         string         `json:"kind"`
	ConnectionID string         `json:"connection_id,omitempty"`
	RequestID    string         `json:"request_id,omitempty"`
	Status       string         `json:"status"`
	Payload      map[string]any `json:"payload,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
	CommittedAt  *time.Time     `json:"committed_at,omitempty"`
}

type RecoveredIntent struct {
	IntentID     int64  `json:"intent_id"`
	Kind         string `json:"kind"`
	ConnectionID string `json:"connection_id,omitempty"`
	Action       string `json:"action"`
	FromState    string `json:"from_state,omitempty"`
	ToState      string `json:"to_state,omitempty"`
	Detail       string `json:"detail,omitempty"`
}

type ConnectionStateDelta struct {
	ConnectionID string `json:"connection_id"`
	StateBefore  string `json:"state_before"`
	StateAfter   string `json:"state_after"`
}

// RecoveryReport is derived solely from SQLite rows written before the kill.
// It never claims that a process-exit report was produced.
type RecoveryReport struct {
	ReportID                  string                 `json:"report_id"`
	StartedAt                 time.Time              `json:"started_at"`
	FinishedAt                time.Time              `json:"finished_at"`
	PendingIntentsFound       int                    `json:"pending_intents_found"`
	Intents                   []RecoveredIntent      `json:"intents"`
	Connections               []ConnectionStateDelta `json:"connections"`
	MetadataStatesRestored    bool                   `json:"metadata_states_restored"`
	RuntimeHandlesPreserved   bool                   `json:"runtime_handles_preserved"`
	IdleQueueOrderRecoverable bool                   `json:"idle_queue_order_recoverable"`
	FactsOnly                 bool                   `json:"facts_only"`
	Notes                     []string               `json:"notes"`
}

type InjectionRecord struct {
	ID           string         `json:"injection_id"`
	Type         string         `json:"type"`
	Params       map[string]any `json:"params,omitempty"`
	BusinessCode string         `json:"business_code"`
	Observation  map[string]any `json:"observation,omitempty"`
	Status       string         `json:"status"`
	CreatedAt    time.Time      `json:"created_at"`
	CompletedAt  *time.Time     `json:"completed_at,omitempty"`
}
