package store

import (
	"encoding/json"

	"local/windowengine/internal/domain"
)

type SnapshotRecord struct {
	SnapshotID  string `json:"snapshot_id"`
	LoadVersion int64  `json:"load_version"`
	ContentHash string `json:"content_hash"`
	LoadedAtMS  int64  `json:"loaded_at_ms"`
}

type LoadResult struct {
	Snapshot    SnapshotRecord `json:"snapshot"`
	WindowCount int            `json:"window_count"`
	EventCount  int            `json:"event_count"`
}

type Round struct {
	RoundID           string `json:"round_id"`
	SnapshotID        string `json:"snapshot_id"`
	Status            string `json:"status"`
	GlobalConcurrency int    `json:"global_concurrency"`
	TenantConcurrency int    `json:"tenant_concurrency"`
	TimeBudgetMS      int64  `json:"time_budget_ms"`
	QueueCapacity     int    `json:"queue_capacity"`
	TaskDelayMS       int64  `json:"task_delay_ms"`
	ForceRecalc       bool   `json:"force_recalc"`
	ConsideredCount   int    `json:"considered_count"`
	DispatchedCount   int    `json:"dispatched_count"`
	CommittedCount    int    `json:"committed_count"`
	ShedCount         int    `json:"shed_count"`
	AbortedCount      int    `json:"aborted_count"`
	FailedCount       int    `json:"failed_count"`
	ConsumedBudgetMS  int64  `json:"consumed_budget_ms"`
	RemainingBudgetMS int64  `json:"remaining_budget_ms"`
	MaxGlobalRunning  int    `json:"max_global_running"`
	MaxTenantRunning  int    `json:"max_tenant_running"`
	BudgetsJSON       string `json:"budgets_json"`
	ErrorJSON         string `json:"error_json,omitempty"`
	StartedAtMS       int64  `json:"started_at_ms"`
	FinishedAtMS      int64  `json:"finished_at_ms,omitempty"`
}

type Task struct {
	TaskID       string `json:"task_id"`
	RoundID      string `json:"round_id"`
	SnapshotID   string `json:"snapshot_id"`
	TenantID     string `json:"tenant_id"`
	Priority     int    `json:"priority"`
	WindowID     string `json:"window_id"`
	GroupKey     string `json:"group_key"`
	SpecID       string `json:"spec_id"`
	WindowType   string `json:"window_type"`
	StartMS      int64  `json:"window_start_ms"`
	EndMS        int64  `json:"window_end_ms"`
	CostMS       int64  `json:"cost_ms"`
	Status       string `json:"status"`
	Attempt      int    `json:"attempt"`
	ConsumedMS   int64  `json:"consumed_ms"`
	ResultJSON   string `json:"result_json,omitempty"`
	ErrorJSON    string `json:"error_json,omitempty"`
	ShedJSON     string `json:"shed_reason_json,omitempty"`
	CreatedAtMS  int64  `json:"created_at_ms"`
	StartedAtMS  int64  `json:"started_at_ms,omitempty"`
	FinishedAtMS int64  `json:"finished_at_ms,omitempty"`
}

type TraceEvent struct {
	Sequence          int64          `json:"sequence"`
	EventType         string         `json:"event_type"`
	TenantID          string         `json:"tenant_id,omitempty"`
	WindowID          string         `json:"window_id,omitempty"`
	TaskID            string         `json:"task_id,omitempty"`
	GroupKey          string         `json:"group_key,omitempty"`
	Priority          int            `json:"priority,omitempty"`
	CostMS            int64          `json:"cost_ms,omitempty"`
	ConsumedBeforeMS  int64          `json:"consumed_before_ms"`
	ConsumedAfterMS   int64          `json:"consumed_after_ms"`
	RemainingBudgetMS int64          `json:"remaining_budget_ms"`
	Reason            string         `json:"reason,omitempty"`
	Detail            map[string]any `json:"detail,omitempty"`
	AtMS              int64          `json:"at_ms"`
}

type ShedDecision struct {
	RoundID           string         `json:"round_id"`
	TaskID            string         `json:"task_id"`
	SnapshotID        string         `json:"snapshot_id"`
	TenantID          string         `json:"tenant_id"`
	WindowID          string         `json:"window_id"`
	GroupKey          string         `json:"group_key"`
	Priority          int            `json:"priority"`
	WindowStartMS     int64          `json:"window_start_ms"`
	DecisionOrder     int            `json:"decision_order"`
	ReasonCode        string         `json:"reason_code"`
	Basis             map[string]any `json:"basis"`
	ConsumedBudgetMS  int64          `json:"consumed_budget_ms"`
	RemainingBudgetMS int64          `json:"remaining_budget_ms"`
	CreatedAtMS       int64          `json:"created_at_ms"`
}

type Intent struct {
	ID           int64          `json:"id"`
	IntentID     string         `json:"intent_id"`
	IntentType   string         `json:"intent_type"`
	Status       string         `json:"status"`
	RoundID      string         `json:"round_id"`
	TaskID       string         `json:"task_id"`
	TenantID     string         `json:"tenant_id"`
	WindowID     string         `json:"window_id"`
	Payload      map[string]any `json:"payload"`
	PayloadJSON  string         `json:"payload_json"`
	RecoveryNote string         `json:"recovery_note,omitempty"`
	CreatedAtMS  int64          `json:"created_at_ms"`
	UpdatedAtMS  int64          `json:"updated_at_ms"`
}

type GroupIndexEntry struct {
	GroupKey       string  `json:"group_key"`
	Version        int64   `json:"version"`
	WindowCount    int64   `json:"window_count"`
	TotalCount     int64   `json:"total_count"`
	TotalSumScaled string  `json:"total_sum_scaled"`
	TotalSumFixed  string  `json:"total_sum_fixed"`
	MinScaled      *string `json:"min_scaled"`
	MinFixed       *string `json:"min_fixed"`
	MaxScaled      *string `json:"max_scaled"`
	MaxFixed       *string `json:"max_fixed"`
}

type TenantState struct {
	SnapshotID           string            `json:"snapshot_id"`
	TenantID             string            `json:"tenant_id"`
	Version              int64             `json:"version"`
	TotalCount           int64             `json:"total_count"`
	TotalSumScaled       string            `json:"total_sum_scaled"`
	TotalSumFixed        string            `json:"total_sum_fixed"`
	MinScaled            *string           `json:"min_scaled"`
	MinFixed             *string           `json:"min_fixed"`
	MaxScaled            *string           `json:"max_scaled"`
	MaxFixed             *string           `json:"max_fixed"`
	CommittedWindowCount int64             `json:"committed_window_count"`
	GroupCount           int64             `json:"group_count"`
	Groups               []GroupIndexEntry `json:"groups"`
	UpdatedAtMS          int64             `json:"updated_at_ms"`
}

type WindowView struct {
	domain.Window
	Status    string                `json:"status"`
	Version   int64                 `json:"version"`
	Aggregate *domain.AggregateView `json:"aggregate,omitempty"`
}

type AggregateQueryResult struct {
	SnapshotID string       `json:"snapshot_id"`
	TenantID   string       `json:"tenant_id"`
	Version    int64        `json:"version"`
	Windows    []WindowView `json:"windows"`
	State      TenantState  `json:"tenant_state"`
}

type RecoveryReport struct {
	ReportID            string         `json:"report_id"`
	BasedOnDurableFacts bool           `json:"based_on_durable_facts"`
	PendingIntentCount  int            `json:"pending_intent_count"`
	PendingIntents      []Intent       `json:"pending_intents"`
	AbortedTaskIDs      []string       `json:"aborted_task_ids"`
	ResetWindowIDs      []string       `json:"reset_window_ids"`
	FinalizedIntentIDs  []string       `json:"finalized_intent_ids"`
	RunningTaskIDs      []string       `json:"running_task_ids"`
	InterruptedRoundIDs []string       `json:"interrupted_round_ids"`
	Details             map[string]any `json:"details,omitempty"`
	Disclaimer          string         `json:"disclaimer"`
	CreatedAtMS         int64          `json:"created_at_ms"`
}

func mustJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(data)
}
