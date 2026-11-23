package main

import "time"

const (
	MsgPendingInvisible = "PENDING_INVISIBLE"
	MsgVisible          = "VISIBLE"
	MsgConsumed         = "CONSUMED"
	MsgRolledBack       = "ROLLED_BACK"
	MsgDeadLettered     = "DEAD_LETTERED"

	LeaseFree    = "FREE"
	LeaseLeased  = "LEASED"
	LeaseExpired = "EXPIRED"

	TxnPending    = "PENDING"
	TxnCommitted  = "COMMITTED"
	TxnRolledBack = "ROLLED_BACK"

	TxnDecisionCommit   = "COMMIT"
	TxnDecisionRollback = "ROLLBACK"

	IntentPrepared  = "PREPARED"
	IntentCommitted = "COMMITTED"
	IntentAborted   = "ABORTED"
	IntentRecovered = "RECOVERED"

	RequestRunning   = "RUNNING"
	RequestCommitted = "COMMITTED"
	RequestFailed    = "FAILED"
)

var messageTransitions = map[string]map[string]bool{
	MsgPendingInvisible: {MsgVisible: true, MsgRolledBack: true},
	MsgVisible:          {MsgConsumed: true, MsgDeadLettered: true},
	MsgConsumed:         {MsgVisible: true}, // explicit replay preparation
	MsgRolledBack:       {},
	MsgDeadLettered:     {},
}

var leaseTransitions = map[string]map[string]bool{
	LeaseFree:    {LeaseLeased: true},
	LeaseLeased:  {LeaseFree: true, LeaseExpired: true},
	LeaseExpired: {LeaseLeased: true},
}

func canTransitionMessage(from, to string) bool {
	return messageTransitions[from][to]
}

func canTransitionLease(from, to string) bool {
	return leaseTransitions[from][to]
}

type Topic struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	RoutingRule       string `json:"routing_rule"`
	CreatedAtMS       int64  `json:"created_at_ms"`
	PartitionPriority []int  `json:"partition_priorities,omitempty"`
}

type Message struct {
	MessageID     string `json:"message_id"`
	Topic         string `json:"topic"`
	Partition     int    `json:"partition"`
	PartitionKey  string `json:"partition_key,omitempty"`
	Seq           int64  `json:"seq"`
	Status        string `json:"status"`
	Transactional bool   `json:"transactional"`
	TxnID         string `json:"txn_id,omitempty"`
	BodyDigest    string `json:"body_digest"`
	Body          string `json:"body,omitempty"`
	ProducedAtMS  int64  `json:"produced_at_ms"`
	UpdatedAtMS   int64  `json:"updated_at_ms"`
}

type Transaction struct {
	TxnID             string `json:"txn_id"`
	MessageID         string `json:"message_id"`
	Topic             string `json:"topic"`
	Partition         int    `json:"partition"`
	Seq               int64  `json:"seq"`
	Status            string `json:"status"`
	Decision          string `json:"decision,omitempty"`
	QueryCount        int64  `json:"query_count"`
	CoordinatorResult string `json:"coordinator_result,omitempty"`
	QueriedAtMS       int64  `json:"queried_at_ms,omitempty"`
	DecidedAtMS       int64  `json:"decided_at_ms,omitempty"`
}

type PartitionView struct {
	Topic                 string `json:"topic"`
	Partition             int    `json:"partition"`
	Priority              int    `json:"priority"`
	NextSeq               int64  `json:"next_seq"`
	VisibleFrontier       int64  `json:"visible_frontier"`
	EffectiveFrontier     int64  `json:"effective_frontier"`
	ConsumeCursor         int64  `json:"consume_cursor"`
	EarliestRetainedSeq   int64  `json:"earliest_retained_seq"`
	LeaseState            string `json:"lease_state"`
	LeaseOwner            string `json:"lease_owner,omitempty"`
	LeaseVersion          int64  `json:"lease_version"`
	LeaseExpiresAtMS      int64  `json:"lease_expires_at_ms,omitempty"`
	LeaseCursor           int64  `json:"lease_cursor"`
	LeaseLastDeliveredSeq *int64 `json:"lease_last_delivered_seq,omitempty"`
	ReplayActive          bool   `json:"replay_active"`
	ReplayFrom            *int64 `json:"replay_from,omitempty"`
	ReplayLimit           *int64 `json:"replay_limit,omitempty"`
	PendingTxnCount       int64  `json:"pending_txn_count"`
	Backlog               int64  `json:"backlog"`
	UpdatedAtMS           int64  `json:"updated_at_ms"`
}

type PublishRequest struct {
	RequestID     string `json:"request_id,omitempty"`
	MessageID     string `json:"message_id"`
	Topic         string `json:"topic"`
	PartitionKey  string `json:"partition_key,omitempty"`
	Partition     *int   `json:"partition,omitempty"`
	Body          string `json:"body"`
	Transactional bool   `json:"transactional,omitempty"`
	TxnID         string `json:"txn_id,omitempty"`
}

type PublishResult struct {
	Message   Message `json:"message"`
	Duplicate bool    `json:"duplicate"`
	RequestID string  `json:"request_id,omitempty"`
}

type PullRequest struct {
	RequestID      string `json:"request_id,omitempty"`
	Owner          string `json:"owner"`
	LeaseVersion   *int64 `json:"lease_version,omitempty"`
	Limit          int    `json:"limit,omitempty"`
	LeaseInjection string `json:"lease_injection,omitempty"`
}

type PullResult struct {
	Messages  []Message     `json:"messages"`
	Lease     PartitionView `json:"lease"`
	Injection *Observation  `json:"injection,omitempty"`
	RequestID string        `json:"request_id,omitempty"`
}

type CommitRequest struct {
	RequestID    string `json:"request_id,omitempty"`
	Owner        string `json:"owner"`
	LeaseVersion int64  `json:"lease_version"`
	ThroughSeq   int64  `json:"through_seq"`
}

type CommitResult struct {
	Partition    PartitionView `json:"partition"`
	ConsumedFrom int64         `json:"consumed_from"`
	ConsumedTo   int64         `json:"consumed_to"`
	RequestID    string        `json:"request_id,omitempty"`
}

type ReplayRequest struct {
	RequestID      string `json:"request_id,omitempty"`
	Owner          string `json:"owner"`
	LeaseVersion   int64  `json:"lease_version"`
	FromSeq        int64  `json:"from_seq"`
	LeaseInjection string `json:"lease_injection,omitempty"`
}

type ReplayResult struct {
	Partition   PartitionView `json:"partition"`
	ReplayFrom  int64         `json:"replay_from"`
	ReplayLimit int64         `json:"replay_limit"`
	Injection   *Observation  `json:"injection,omitempty"`
	RequestID   string        `json:"request_id,omitempty"`
}

type RetainRequest struct {
	RequestID string `json:"request_id,omitempty"`
	FromSeq   int64  `json:"from_seq"`
}

type ResolveRequest struct {
	RequestID      string `json:"request_id,omitempty"`
	InjectionMode  string `json:"injection_mode,omitempty"`
	ForceRecheck   bool   `json:"force_recheck,omitempty"`
	QueryTimeoutMS int    `json:"query_timeout_ms,omitempty"`
}

type ResolveResult struct {
	Transaction Transaction   `json:"transaction"`
	Message     Message       `json:"message"`
	Partition   PartitionView `json:"partition"`
	Idempotent  bool          `json:"idempotent"`
	Observation *Observation  `json:"observation,omitempty"`
	RequestID   string        `json:"request_id,omitempty"`
}

type Injection struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Target      string `json:"target"`
	Mode        string `json:"mode"`
	DelayMS     int    `json:"delay_ms,omitempty"`
	Payload     string `json:"payload,omitempty"`
	Active      bool   `json:"active"`
	CreatedAtMS int64  `json:"created_at_ms"`
}

type InjectionRequest struct {
	Kind    string `json:"kind"`
	Target  string `json:"target"`
	Mode    string `json:"mode"`
	DelayMS int    `json:"delay_ms,omitempty"`
	Payload string `json:"payload,omitempty"`
	Active  *bool  `json:"active,omitempty"`
}

type Observation struct {
	ID           int64  `json:"id"`
	InjectionID  string `json:"injection_id"`
	Kind         string `json:"kind"`
	Mode         string `json:"mode"`
	Target       string `json:"target"`
	Phase        string `json:"phase"`
	ObservedCode string `json:"observed_code,omitempty"`
	Detail       string `json:"detail,omitempty"`
	RequestID    string `json:"request_id,omitempty"`
	ObservedAtMS int64  `json:"observed_at_ms"`
}

type ScheduledTask struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"` // txn or lease_expire
	Topic      string `json:"topic"`
	Partition  int    `json:"partition"`
	Priority   int    `json:"priority"`
	TxnID      string `json:"txn_id,omitempty"`
	RequestID  string `json:"request_id,omitempty"`
	EnqueueSeq int64  `json:"enqueue_seq"`
	Status     string `json:"status"`
	Deduped    bool   `json:"deduped,omitempty"`
}

type SchedulerConfig struct {
	GlobalConcurrency      int `json:"global_concurrency"`
	PerPartitionLeaseLimit int `json:"per_partition_lease_limit"`
	QueueCapacity          int `json:"queue_capacity"`
	RoundBudgetMS          int `json:"round_budget_ms"`
	QueryTimeoutMS         int `json:"query_timeout_ms"`
	MinDispatchMS          int `json:"min_dispatch_ms"`
}

type SchedulerTaskRequest struct {
	Kind      string `json:"kind"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	TxnID     string `json:"txn_id,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

type BudgetStep struct {
	TaskID         string `json:"task_id"`
	Kind           string `json:"kind"`
	Topic          string `json:"topic"`
	Partition      int    `json:"partition"`
	Priority       int    `json:"priority"`
	TxnID          string `json:"txn_id,omitempty"`
	Result         string `json:"result"`
	Code           string `json:"code,omitempty"`
	Reason         string `json:"reason,omitempty"`
	DurationMS     int64  `json:"duration_ms"`
	BudgetBeforeMS int64  `json:"budget_before_ms"`
	BudgetAfterMS  int64  `json:"budget_after_ms"`
}

type BudgetRound struct {
	ID          int64        `json:"id"`
	StartedAtMS int64        `json:"started_at_ms"`
	BudgetMS    int64        `json:"budget_ms"`
	ConsumedMS  int64        `json:"consumed_ms"`
	RemainingMS int64        `json:"remaining_ms"`
	Status      string       `json:"status"`
	Steps       []BudgetStep `json:"steps"`
}

type RecoveryDetail struct {
	IntentID      string `json:"intent_id"`
	RequestID     string `json:"request_id"`
	Operation     string `json:"operation"`
	Target        string `json:"target"`
	Action        string `json:"action"`
	Evidence      string `json:"evidence"`
	StateRestored bool   `json:"state_restored"`
}

type RecoveryReport struct {
	ID               int64            `json:"id"`
	StartedAtMS      int64            `json:"started_at_ms"`
	FinishedAtMS     int64            `json:"finished_at_ms"`
	RecoveredIntents int64            `json:"recovered_intents"`
	Details          []RecoveryDetail `json:"details"`
}

type Dashboard struct {
	GeneratedAtMS  int64           `json:"generated_at_ms"`
	Topics         []Topic         `json:"topics"`
	Partitions     []PartitionView `json:"partitions"`
	PendingTxns    []Transaction   `json:"pending_txns"`
	RecentMessages []Message       `json:"recent_messages"`
	Recovery       *RecoveryReport `json:"recovery,omitempty"`
	BudgetRounds   []BudgetRound   `json:"budget_rounds"`
	Scheduler      SchedulerView   `json:"scheduler"`
	Observations   []Observation   `json:"observations"`
}

type SchedulerView struct {
	Config     SchedulerConfig `json:"config"`
	Queue      []ScheduledTask `json:"queue"`
	InFlight   int             `json:"in_flight"`
	LastRounds []BudgetRound   `json:"last_rounds"`
}

type Intent struct {
	IntentID    string
	RequestID   string
	Operation   string
	Target      string
	Payload     string
	Status      string
	Result      string
	CreatedAtMS int64
	UpdatedAtMS int64
}

type requestRecord struct {
	RequestID   string
	Operation   string
	Fingerprint string
	Status      string
	Response    string
	ErrorCode   string
	ErrorMsg    string
}

func nowMS(t time.Time) int64 { return t.UTC().UnixMilli() }
