package feedbackstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db     *sql.DB
	dbPath string
}

type IntentRecord struct {
	ID          string `json:"intent_id"`
	Kind        string `json:"kind"`
	AggregateID string `json:"aggregate_id"`
	PayloadJSON string `json:"payload_json"`
	Status      string `json:"status"`
	CreatedAtMS int64  `json:"created_at_ms"`
	UpdatedAtMS int64  `json:"updated_at_ms"`
}

type BatchRequestRecord struct {
	BatchID     string `json:"batch_id"`
	PayloadHash string `json:"payload_hash"`
	PartitionID int    `json:"partition_id"`
	Status      string `json:"status"`
	ResultJSON  string `json:"result_json,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
	ErrorJSON   string `json:"error_json,omitempty"`
	CreatedAtMS int64  `json:"created_at_ms"`
	CompletedMS int64  `json:"completed_at_ms,omitempty"`
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policies (
  version INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,
  base_version INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  checksum TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  activated_at_ms INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS batch_requests (
  batch_id TEXT PRIMARY KEY,
  payload_hash TEXT NOT NULL,
  partition_id INTEGER NOT NULL,
  status TEXT NOT NULL,
  result_json TEXT NOT NULL DEFAULT '',
  error_code TEXT NOT NULL DEFAULT '',
  error_json TEXT NOT NULL DEFAULT '',
  created_at_ms INTEGER NOT NULL,
  completed_at_ms INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
  partition_id INTEGER NOT NULL,
  sequence INTEGER NOT NULL,
  timestamp_ms INTEGER NOT NULL,
  group_key TEXT NOT NULL,
  metric_fixed INTEGER NOT NULL,
  batch_id TEXT NOT NULL,
  PRIMARY KEY (partition_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_events_batch ON events(batch_id);

CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  partition_id INTEGER NOT NULL,
  window_start_ms INTEGER NOT NULL,
  window_end_ms INTEGER NOT NULL,
  event_count INTEGER NOT NULL,
  bytes INTEGER NOT NULL,
  payload_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  current_group_id TEXT NOT NULL DEFAULT '',
  attempts INTEGER NOT NULL DEFAULT 0,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_status_order ON batches(status, partition_id, window_start_ms, batch_id);

CREATE TABLE IF NOT EXISTS intents (
  intent_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  aggregate_id TEXT NOT NULL DEFAULT '',
  payload_json TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL,
  recovery_action TEXT NOT NULL DEFAULT '',
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, created_at_ms);

CREATE TABLE IF NOT EXISTS buffer_inflight (
  policy_version INTEGER NOT NULL,
  fusion_group_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  bytes INTEGER NOT NULL,
  entered_at_ms INTEGER NOT NULL,
  PRIMARY KEY (policy_version, fusion_group_id, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_buffer_group ON buffer_inflight(policy_version, fusion_group_id);

CREATE TABLE IF NOT EXISTS waiting_batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  policy_version INTEGER NOT NULL,
  batch_id TEXT NOT NULL,
  partition_id INTEGER NOT NULL,
  upstream_group_id TEXT NOT NULL,
  target_group_id TEXT NOT NULL,
  bytes INTEGER NOT NULL,
  status TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_waiting_status ON waiting_batches(policy_version, target_group_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_waiting_active ON waiting_batches(batch_id, target_group_id) WHERE status='WAITING';

CREATE TABLE IF NOT EXISTS backpressure_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  intent_id TEXT NOT NULL DEFAULT '',
  policy_version INTEGER NOT NULL,
  fusion_group_id TEXT NOT NULL,
  level TEXT NOT NULL,
  inflight_batches INTEGER NOT NULL,
  inflight_bytes INTEGER NOT NULL,
  max_batches INTEGER NOT NULL,
  max_bytes INTEGER NOT NULL,
  batch_utilization REAL NOT NULL,
  byte_utilization REAL NOT NULL,
  watermark_ratio REAL NOT NULL,
  blocked_upstream_batches INTEGER NOT NULL DEFAULT 0,
  created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bp_group_time ON backpressure_events(policy_version, fusion_group_id, id);

CREATE TABLE IF NOT EXISTS rounds (
  round_id TEXT PRIMARY KEY,
  policy_version INTEGER NOT NULL,
  budget_ms INTEGER NOT NULL,
  consumed_budget_ms INTEGER NOT NULL,
  remaining_budget_ms INTEGER NOT NULL,
  partition_concurrency INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS shed_decisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  round_id TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  batch_id TEXT NOT NULL,
  partition_id INTEGER NOT NULL,
  window_start_ms INTEGER NOT NULL,
  rank_no INTEGER NOT NULL,
  estimated_cost_ms INTEGER NOT NULL,
  consumed_budget_before_ms INTEGER NOT NULL,
  remaining_budget_ms INTEGER NOT NULL,
  reason_code TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shed_batch ON shed_decisions(batch_id);

CREATE TABLE IF NOT EXISTS group_outputs (
  batch_id TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  fusion_group_id TEXT NOT NULL,
  signature_hash TEXT NOT NULL,
  output_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (batch_id, policy_version, fusion_group_id)
);

CREATE TABLE IF NOT EXISTS window_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  policy_version INTEGER NOT NULL,
  fusion_group_id TEXT NOT NULL,
  signature_hash TEXT NOT NULL,
  partition_id INTEGER NOT NULL,
  window_start_ms INTEGER NOT NULL,
  window_end_ms INTEGER NOT NULL,
  group_key TEXT NOT NULL,
  count INTEGER NOT NULL,
  sum_fixed INTEGER NOT NULL,
  min_fixed INTEGER NOT NULL,
  max_fixed INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'COMMITTED',
  source_batch_ids TEXT NOT NULL DEFAULT '[]',
  committed_intent_id TEXT NOT NULL DEFAULT '',
  committed_at_ms INTEGER NOT NULL,
  UNIQUE(policy_version, fusion_group_id, signature_hash, partition_id, window_start_ms, window_end_ms, group_key)
);
CREATE INDEX IF NOT EXISTS idx_window_active ON window_results(policy_version, status, partition_id, window_start_ms, group_key);

CREATE TABLE IF NOT EXISTS group_completions (
  batch_id TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  fusion_group_id TEXT NOT NULL,
  signature_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  window_result_count INTEGER NOT NULL DEFAULT 0,
  committed_intent_id TEXT NOT NULL DEFAULT '',
  completed_at_ms INTEGER NOT NULL,
  PRIMARY KEY (batch_id, policy_version, fusion_group_id)
);

CREATE TABLE IF NOT EXISTS late_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  policy_version INTEGER NOT NULL,
  batch_id TEXT NOT NULL,
  partition_id INTEGER NOT NULL,
  sequence INTEGER NOT NULL,
  window_start_ms INTEGER NOT NULL,
  window_end_ms INTEGER NOT NULL,
  reason TEXT NOT NULL,
  recorded_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cascade_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  intent_id TEXT NOT NULL,
  old_policy_version INTEGER NOT NULL,
  new_policy_version INTEGER NOT NULL,
  affected_groups TEXT NOT NULL,
  invalidated_window_rows INTEGER NOT NULL,
  rebuilt_window_rows INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER NOT NULL,
  report_json TEXT NOT NULL
);
`

func nowMS() int64 { return time.Now().UnixMilli() }

func OpenStore(ctx context.Context, dbPath string) (*Store, error) {
	if dir := filepath.Dir(dbPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	dsn := "file:" + dbPath + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)&_pragma=synchronous(NORMAL)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	store := &Store{db: db, dbPath: dbPath}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate sqlite schema: %w", err)
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Path() string { return s.dbPath }

func (s *Store) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func CreateIntent(ctx context.Context, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, intent IntentRecord) (*IntentRecord, error) {
	if intent.ID == "" {
		return nil, fmt.Errorf("intent id is required")
	}
	if intent.Status == "" {
		intent.Status = IntentPending
	}
	ts := nowMS()
	if intent.CreatedAtMS == 0 {
		intent.CreatedAtMS = ts
	}
	intent.UpdatedAtMS = ts
	payload := intent.PayloadJSON
	if payload == "" {
		payload = "{}"
	}
	_, err := q.ExecContext(ctx, `
		INSERT INTO intents(intent_id, kind, aggregate_id, payload_json, status, created_at_ms, updated_at_ms)
		VALUES(?,?,?,?,?,?,?)
		ON CONFLICT(intent_id) DO NOTHING`,
		intent.ID, intent.Kind, intent.AggregateID, payload, intent.Status, intent.CreatedAtMS, intent.UpdatedAtMS)
	if err != nil {
		return nil, err
	}
	return &intent, nil
}

func (s *Store) CreateIntent(ctx context.Context, intent IntentRecord) (*IntentRecord, error) {
	return CreateIntent(ctx, s.db, intent)
}

func (s *Store) GetIntent(ctx context.Context, id string) (*IntentRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT intent_id,kind,aggregate_id,payload_json,status,created_at_ms,updated_at_ms FROM intents WHERE intent_id=?`, id)
	var rec IntentRecord
	err := row.Scan(&rec.ID, &rec.Kind, &rec.AggregateID, &rec.PayloadJSON, &rec.Status, &rec.CreatedAtMS, &rec.UpdatedAtMS)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *Store) ListPendingIntents(ctx context.Context) ([]IntentRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT intent_id,kind,aggregate_id,payload_json,status,created_at_ms,updated_at_ms FROM intents WHERE status=? ORDER BY created_at_ms,intent_id`, IntentPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []IntentRecord{}
	for rows.Next() {
		var rec IntentRecord
		if err := rows.Scan(&rec.ID, &rec.Kind, &rec.AggregateID, &rec.PayloadJSON, &rec.Status, &rec.CreatedAtMS, &rec.UpdatedAtMS); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func CompleteIntentTx(ctx context.Context, tx *sql.Tx, id, status string) error {
	if status == "" {
		status = IntentCommitted
	}
	_, err := tx.ExecContext(ctx, `UPDATE intents SET status=?, updated_at_ms=? WHERE intent_id=?`, status, nowMS(), id)
	return err
}

func (s *Store) SaveRecoveryReport(ctx context.Context, report RecoveryReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO recovery_reports(started_at_ms,finished_at_ms,report_json) VALUES(?,?,?)`,
		nowMS(), nowMS(), string(data))
	return err
}
