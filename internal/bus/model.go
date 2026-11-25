package bus

import (
	"encoding/json"
	"time"

	"local/eventbus/internal/agg"
)

type EventState string

const (
	StateAccepted     EventState = "ACCEPTED"
	StateDispatched   EventState = "DISPATCHED"
	StateAcked        EventState = "ACKED"
	StateRedelivered  EventState = "REDELIVERED"
	StateDeadLettered EventState = "DEAD_LETTERED"
)

// AllowedTransition is the complete event/subscription state machine.
//
// ACKED -> REDELIVERED is only entered by an explicit durable offset rewind.
func AllowedTransition(from, to EventState) bool {
	switch from {
	case StateAccepted:
		return to == StateDispatched || to == StateDeadLettered
	case StateDispatched:
		return to == StateAcked || to == StateRedelivered || to == StateDeadLettered
	case StateRedelivered:
		return to == StateDispatched || to == StateDeadLettered
	case StateAcked:
		return to == StateRedelivered
	case StateDeadLettered:
		return to == StateRedelivered
	default:
		return false
	}
}

type GroupSpec struct {
	Name      string `json:"name"`
	Algorithm string `json:"algorithm"`
}

type CreateTopicRequest struct {
	RequestID  string      `json:"request_id,omitempty"`
	Topic      string      `json:"topic"`
	Partitions int         `json:"partitions"`
	Groups     []GroupSpec `json:"groups"`
}

type TopicGroup struct {
	Name      string        `json:"name"`
	Algorithm agg.Algorithm `json:"algorithm"`
}

type Topic struct {
	Topic      string       `json:"topic"`
	Partitions int          `json:"partitions"`
	Groups     []TopicGroup `json:"groups"`
	CreatedAt  int64        `json:"created_at_ms"`
}

type PublishRequest struct {
	RequestID    string          `json:"request_id,omitempty"`
	EventID      string          `json:"event_id"`
	Topic        string          `json:"topic"`
	PartitionKey string          `json:"partition_key"`
	EventTimeMS  int64           `json:"event_time_ms"`
	BatchID      string          `json:"batch_id"`
	AggregateKey string          `json:"aggregate_key"`
	Metric       float64         `json:"metric"`
	Body         json.RawMessage `json:"body"`
	BodyDigest   string          `json:"body_digest,omitempty"`
}

type PublishResult struct {
	EventID     string `json:"event_id"`
	Topic       string `json:"topic"`
	Partition   int    `json:"partition"`
	Seq         int64  `json:"seq"`
	BodyDigest  string `json:"body_digest"`
	State       string `json:"state"`
	Idempotent  bool   `json:"idempotent"`
	CreatedAtMS int64  `json:"created_at_ms"`
}

type PullRequest struct {
	RequestID  string `json:"request_id,omitempty"`
	Topic      string `json:"topic"`
	Group      string `json:"group"`
	Partition  int    `json:"partition"`
	ConsumerID string `json:"consumer_id"`
	MaxEvents  int    `json:"max_events"`
}

type DeliveredEvent struct {
	EventID       string     `json:"event_id"`
	Topic         string     `json:"topic"`
	Partition     int        `json:"partition"`
	Seq           int64      `json:"seq"`
	PartitionKey  string     `json:"partition_key"`
	EventTimeMS   int64      `json:"event_time_ms"`
	BatchID       string     `json:"batch_id"`
	AggregateKey  string     `json:"aggregate_key"`
	Metric        float64    `json:"metric"`
	BodyDigest    string     `json:"body_digest"`
	State         EventState `json:"state"`
	DeliveryCount int64      `json:"delivery_count"`
}

type PullResult struct {
	LeaseToken  string           `json:"lease_token"`
	ExpiresAtMS int64            `json:"lease_expires_at_ms"`
	Replay      bool             `json:"replay"`
	Events      []DeliveredEvent `json:"events"`
}

type AckRequest struct {
	RequestID  string  `json:"request_id,omitempty"`
	Topic      string  `json:"topic"`
	Group      string  `json:"group"`
	Partition  int     `json:"partition"`
	ConsumerID string  `json:"consumer_id"`
	LeaseToken string  `json:"lease_token"`
	Seqs       []int64 `json:"seqs"`
	AckBatchID string  `json:"ack_batch_id,omitempty"`
}

type KeyPartial struct {
	Count              int64   `json:"count"`
	Sum                float64 `json:"sum"`
	SumAbs             float64 `json:"sum_abs"`
	AbsErrorBound      float64 `json:"absolute_error_bound"`
	RelativeErrorBound float64 `json:"relative_error_bound"`
}

type AggregateBatch struct {
	ID                 int64                 `json:"id"`
	AckBatchID         string                `json:"ack_batch_id"`
	EventBatchIDs      []string              `json:"event_batch_ids"`
	StartSeq           int64                 `json:"start_seq"`
	EndSeq             int64                 `json:"end_seq"`
	Algorithm          agg.Algorithm         `json:"algorithm"`
	EventCount         int64                 `json:"event_count"`
	Partials           map[string]KeyPartial `json:"partials"`
	SumAbs             float64               `json:"sum_abs"`
	AbsErrorBound      float64               `json:"absolute_error_bound"`
	RelativeErrorBound float64               `json:"relative_error_bound"`
	StateDigest        string                `json:"state_digest"`
	PreviousOffset     int64                 `json:"previous_offset"`
	NewOffset          int64                 `json:"new_offset"`
	Version            int64                 `json:"version"`
	Active             bool                  `json:"active"`
	CreatedAtMS        int64                 `json:"created_at_ms"`
	RevertedAtMS       *int64                `json:"reverted_at_ms,omitempty"`
}

type AckResult struct {
	Topic          string         `json:"topic"`
	Group          string         `json:"group"`
	Partition      int            `json:"partition"`
	Offset         int64          `json:"offset"`
	ReplayActive   bool           `json:"replay_active"`
	ReplayComplete bool           `json:"replay_complete"`
	AckedSeqs      []int64        `json:"acked_seqs"`
	Batch          AggregateBatch `json:"aggregate_batch"`
	StateDigest    string         `json:"state_digest"`
}

type DeadLetterRequest struct {
	RequestID  string `json:"request_id,omitempty"`
	Topic      string `json:"topic"`
	Group      string `json:"group"`
	Partition  int    `json:"partition"`
	ConsumerID string `json:"consumer_id"`
	LeaseToken string `json:"lease_token"`
	Seq        int64  `json:"seq"`
	Reason     string `json:"reason,omitempty"`
}

type DeadLetterResult struct {
	Topic          string     `json:"topic"`
	Group          string     `json:"group"`
	Partition      int        `json:"partition"`
	Seq            int64      `json:"seq"`
	State          EventState `json:"state"`
	Offset         int64      `json:"offset"`
	ReplayActive   bool       `json:"replay_active"`
	ReplayComplete bool       `json:"replay_complete"`
}

type RewindRequest struct {
	RequestID string `json:"request_id,omitempty"`
	Topic     string `json:"topic"`
	Group     string `json:"group"`
	Partition int    `json:"partition"`
	TargetSeq int64  `json:"target_seq"`
}

type RewindResult struct {
	Topic           string `json:"topic"`
	Group           string `json:"group"`
	Partition       int    `json:"partition"`
	OldOffset       int64  `json:"old_offset"`
	NewOffset       int64  `json:"new_offset"`
	ReplayUntil     int64  `json:"replay_until"`
	RevertedBatches int    `json:"reverted_batches"`
	StateDigest     string `json:"state_digest"`
}

type OffsetState struct {
	Topic            string `json:"topic"`
	Group            string `json:"group"`
	Partition        int    `json:"partition"`
	AckedOffset      int64  `json:"acked_offset"`
	ReplayUntil      int64  `json:"replay_until"`
	ReplayActive     bool   `json:"replay_active"`
	Version          int64  `json:"version"`
	LeaseHeld        bool   `json:"lease_held"`
	LeaseOwner       string `json:"lease_owner,omitempty"`
	LeaseExpiresAtMS int64  `json:"lease_expires_at_ms,omitempty"`
	UpdatedAtMS      int64  `json:"updated_at_ms"`
}

type EventStateView struct {
	Topic         string     `json:"topic"`
	Group         string     `json:"group"`
	Partition     int        `json:"partition"`
	Seq           int64      `json:"seq"`
	EventID       string     `json:"event_id"`
	State         EventState `json:"state"`
	DeliveryCount int64      `json:"delivery_count"`
	UpdatedAtMS   int64      `json:"updated_at_ms"`
}

type AggregateKeyResult struct {
	Count              int64   `json:"count"`
	Sum                float64 `json:"sum"`
	SumAbs             float64 `json:"sum_abs"`
	AbsErrorBound      float64 `json:"absolute_error_bound"`
	RelativeErrorBound float64 `json:"relative_error_bound"`
}

type AggregateResult struct {
	Topic             string                        `json:"topic"`
	Group             string                        `json:"group"`
	Partition         int                           `json:"partition"`
	Algorithm         agg.Algorithm                 `json:"algorithm"`
	Offset            int64                         `json:"offset"`
	Version           int64                         `json:"version"`
	ReplayActive      bool                          `json:"replay_active"`
	Keys              map[string]AggregateKeyResult `json:"keys"`
	EventCount        int64                         `json:"event_count"`
	SumAbs            float64                       `json:"sum_abs"`
	AbsErrorBound     float64                       `json:"absolute_error_bound"`
	RelativeBound     float64                       `json:"relative_error_bound"`
	RelativeTolerance float64                       `json:"declared_relative_tolerance"`
	StateDigest       string                        `json:"state_digest"`
}

type ErrorLedger struct {
	Topic              string           `json:"topic"`
	Group              string           `json:"group"`
	Partition          int              `json:"partition"`
	Algorithm          agg.Algorithm    `json:"algorithm"`
	Offset             int64            `json:"offset"`
	Batches            []AggregateBatch `json:"batches"`
	Cumulative         AggregateResult  `json:"cumulative"`
	LedgerAbsBound     float64          `json:"ledger_absolute_error_bound"`
	LedgerRelBound     float64          `json:"ledger_relative_error_bound"`
	BatchCount         int              `json:"active_batch_count"`
	RevertedBatchCount int              `json:"reverted_batch_count"`
}

type SetAlgorithmRequest struct {
	RequestID string `json:"request_id,omitempty"`
	Topic     string `json:"topic"`
	Group     string `json:"group"`
	Partition int    `json:"partition"`
	Algorithm string `json:"algorithm"`
}

type SetAlgorithmResult struct {
	Topic     string        `json:"topic"`
	Group     string        `json:"group"`
	Partition int           `json:"partition"`
	Algorithm agg.Algorithm `json:"algorithm"`
}

type SetRetentionRequest struct {
	RequestID      string `json:"request_id,omitempty"`
	Topic          string `json:"topic"`
	Partition      int    `json:"partition"`
	EarliestRetain int64  `json:"earliest_retained_seq"`
}

type SetRetentionResult struct {
	Topic          string `json:"topic"`
	Partition      int    `json:"partition"`
	EarliestRetain int64  `json:"earliest_retained_seq"`
}

type RecoveryIntent struct {
	ID        string `json:"id"`
	Operation string `json:"operation"`
	Topic     string `json:"topic,omitempty"`
	Group     string `json:"group,omitempty"`
	Partition int    `json:"partition"`
	Outcome   string `json:"outcome"`
	Evidence  string `json:"evidence"`
}

type RecoveryReport struct {
	RunID              int64            `json:"run_id"`
	StartedAtMS        int64            `json:"started_at_ms"`
	FinishedAtMS       int64            `json:"finished_at_ms"`
	PendingScanned     int              `json:"pending_scanned"`
	CommittedRecovered int              `json:"committed_recovered"`
	RolledBack         int              `json:"rolled_back"`
	LeasesReleased     int64            `json:"leases_released"`
	DeliveriesReset    int64            `json:"deliveries_reset"`
	Intents            []RecoveryIntent `json:"intents"`
}

type ServiceOptions struct {
	DBPath        string
	LeaseDuration time.Duration
	Now           func() time.Time
	SkipRecovery  bool
}
