package core

import "time"

const (
	StatusPending  = "PENDING"
	StatusPushed   = "PUSHED"
	StatusAcked    = "ACKED"
	StatusApplied  = "APPLIED"
	StatusAborted  = "ABORTED"
	PickupLatest   = "LATEST"
	PickupPinned   = "PINNED"
	DispositionOK  = "ACCEPTED"
	DispositionDup = "DUPLICATE"
	DispositionOld = "STALE"
	DispositionBad = "REJECTED"
)

var legalTransitions = map[string]map[string]bool{
	"":            {StatusPending: true},
	StatusPending: {StatusPushed: true, StatusAborted: true},
	StatusPushed:  {StatusAcked: true, StatusAborted: true},
	StatusAcked:   {StatusApplied: true, StatusAborted: true},
	StatusApplied: {},
	StatusAborted: {},
}

func CanTransition(from, to string) bool {
	next, ok := legalTransitions[from]
	return ok && next[to]
}

type Snapshot struct {
	Version     int64              `json:"version"`
	GeneratedAt string             `json:"generated_at"`
	Config      map[string]float64 `json:"config"`
	Digest      string             `json:"digest"`
	CreatedAt   string             `json:"created_at,omitempty"`
}

type Client struct {
	ClientID      string   `json:"client_id"`
	Keys          []string `json:"keys"`
	Endpoint      string   `json:"endpoint"`
	PickupPolicy  string   `json:"pickup_policy"`
	PinnedVersion *int64   `json:"pinned_version,omitempty"`
	CreatedAt     string   `json:"created_at"`
	UpdatedAt     string   `json:"updated_at"`
}

type Batch struct {
	BatchID      string  `json:"batch_id"`
	Version      int64   `json:"version"`
	Status       string  `json:"status"`
	CreatedAt    string  `json:"created_at"`
	FinishedAt   *string `json:"finished_at,omitempty"`
	TimeBudgetMS int64   `json:"time_budget_ms"`
	ErrorCode    string  `json:"error_code,omitempty"`
}

type Push struct {
	PushID          string         `json:"push_id"`
	BatchID         string         `json:"batch_id"`
	ClientID        string         `json:"client_id"`
	Version         int64          `json:"version"`
	Status          string         `json:"status"`
	ContentDigest   string         `json:"content_digest"`
	AttemptToken    string         `json:"attempt_token"`
	AttemptIntentID string         `json:"attempt_intent_id,omitempty"`
	ErrorCode       string         `json:"error_code,omitempty"`
	ErrorMessage    string         `json:"error_message,omitempty"`
	ErrorContext    map[string]any `json:"error_context,omitempty"`
	CreatedAt       string         `json:"created_at"`
	UpdatedAt       string         `json:"updated_at"`
}

type PushTransition struct {
	PushID     string `json:"push_id"`
	FromStatus string `json:"from_status"`
	ToStatus   string `json:"to_status"`
	Reason     string `json:"reason"`
	CreatedAt  string `json:"created_at"`
}

type Ack struct {
	AckID         string         `json:"ack_id"`
	PushID        string         `json:"push_id"`
	ClientID      string         `json:"client_id"`
	Version       int64          `json:"version"`
	ContentDigest string         `json:"content_digest"`
	ResultDigest  string         `json:"result_digest,omitempty"`
	Result        map[string]any `json:"result,omitempty"`
	IngressCount  int            `json:"ingress_count"`
	Source        string         `json:"source"`
	ReceivedAt    string         `json:"received_at"`
}

type AckEvent struct {
	EventID       string         `json:"event_id"`
	AckID         string         `json:"ack_id"`
	PushID        string         `json:"push_id"`
	ClientID      string         `json:"client_id"`
	Version       int64          `json:"version"`
	Disposition   string         `json:"disposition"`
	Reason        string         `json:"reason,omitempty"`
	ContentDigest string         `json:"content_digest,omitempty"`
	ResultDigest  string         `json:"result_digest,omitempty"`
	Result        map[string]any `json:"result,omitempty"`
	IngressCount  int            `json:"ingress_count"`
	ReceivedAt    string         `json:"received_at"`
}

type Result struct {
	ResultID      string      `json:"result_id"`
	ClientID      string      `json:"client_id"`
	PushID        string      `json:"push_id"`
	PickedVersion int64       `json:"picked_version"`
	AckVersion    int64       `json:"ack_version"`
	ResultDigest  string      `json:"result_digest"`
	WeightedSum   float64     `json:"weighted_sum"`
	Normalized    float64     `json:"normalized"`
	Decision      bool        `json:"decision"`
	Seed          int64       `json:"seed"`
	SampleID      string      `json:"sample_id"`
	SampleDigest  string      `json:"sample_digest"`
	Status        string      `json:"status"`
	Reason        string      `json:"reason,omitempty"`
	Mismatches    []FieldDiff `json:"mismatches,omitempty"`
	CreatedAt     string      `json:"created_at"`
}

type ResultReport struct {
	ClientID      string  `json:"client_id"`
	PushID        string  `json:"push_id"`
	PickedVersion int64   `json:"picked_version"`
	Seed          int64   `json:"seed"`
	SampleID      string  `json:"sample_id"`
	SampleDigest  string  `json:"sample_digest"`
	WeightedSum   float64 `json:"weighted_sum"`
	Normalized    float64 `json:"normalized"`
	Decision      bool    `json:"decision"`
	ResultDigest  string  `json:"result_digest"`
}

type FieldDiff struct {
	Field    string `json:"field"`
	Expected any    `json:"expected"`
	Actual   any    `json:"actual"`
}

type Intent struct {
	IntentID    string  `json:"intent_id"`
	Kind        string  `json:"kind"`
	AggregateID string  `json:"aggregate_id"`
	Payload     any     `json:"payload,omitempty"`
	Status      string  `json:"status"`
	CreatedAt   string  `json:"created_at"`
	FinishedAt  *string `json:"finished_at,omitempty"`
	Detail      any     `json:"detail,omitempty"`
}

type RecoveryReport struct {
	ReportID        string           `json:"report_id"`
	StartedAt       string           `json:"started_at"`
	FinishedAt      string           `json:"finished_at"`
	ScannedIntents  int              `json:"scanned_intents"`
	Facts           map[string]any   `json:"facts"`
	Actions         []map[string]any `json:"actions"`
	RestoredClients []ClientVersion  `json:"restored_clients"`
}

type ClientVersion struct {
	ClientID string `json:"client_id"`
	Version  int64  `json:"version"`
	Status   string `json:"status"`
}

type PushEnvelope struct {
	PushID         string             `json:"push_id"`
	BatchID        string             `json:"batch_id"`
	ClientID       string             `json:"client_id"`
	Version        int64              `json:"version"`
	BaseVersion    int64              `json:"base_version"`
	SnapshotDigest string             `json:"snapshot_digest"`
	ContentDigest  string             `json:"content_digest"`
	Config         map[string]float64 `json:"config"`
	Changes        map[string]float64 `json:"changes"`
	Pipeline       PipelineRef        `json:"pipeline"`
}

type PipelineRef struct {
	Seed         int64  `json:"seed"`
	SampleID     string `json:"sample_id"`
	SampleDigest string `json:"sample_digest"`
}

type ClientResponse struct {
	Status  string         `json:"status"`
	Acks    []Ack          `json:"acks"`
	Result  map[string]any `json:"result,omitempty"`
	Message string         `json:"message,omitempty"`
}

func Now() string { return time.Now().UTC().Format(time.RFC3339Nano) }
