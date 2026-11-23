package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"local/configmiddleware/internal/core"

	_ "modernc.org/sqlite"
)

type Store struct {
	db      *sql.DB
	path    string
	writeMu sync.Mutex
}

const schema = `
CREATE TABLE IF NOT EXISTS snapshots (
  version INTEGER PRIMARY KEY,
  generated_at TEXT NOT NULL,
  config_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clients (
  client_id TEXT PRIMARY KEY,
  keys_json TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  pickup_policy TEXT NOT NULL,
  pinned_version INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  finished_at TEXT,
  time_budget_ms INTEGER NOT NULL,
  error_code TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS pushes (
  push_id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  client_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  status TEXT NOT NULL,
  content_digest TEXT NOT NULL,
  attempt_token TEXT NOT NULL,
  attempt_intent_id TEXT NOT NULL DEFAULT '',
  payload_json TEXT NOT NULL DEFAULT '{}',
  target_endpoint TEXT NOT NULL DEFAULT '',
  error_code TEXT NOT NULL DEFAULT '',
  error_message TEXT NOT NULL DEFAULT '',
  error_context_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(client_id, version),
  FOREIGN KEY(batch_id) REFERENCES batches(batch_id),
  FOREIGN KEY(client_id) REFERENCES clients(client_id)
);
CREATE INDEX IF NOT EXISTS idx_pushes_client_status ON pushes(client_id, status, version);
CREATE INDEX IF NOT EXISTS idx_pushes_batch ON pushes(batch_id);

CREATE TABLE IF NOT EXISTS push_transitions (
  transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
  push_id TEXT NOT NULL,
  from_status TEXT NOT NULL,
  to_status TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(push_id) REFERENCES pushes(push_id)
);
CREATE INDEX IF NOT EXISTS idx_transitions_push ON push_transitions(push_id, transition_id);

CREATE TABLE IF NOT EXISTS acks (
  ack_id TEXT PRIMARY KEY,
  push_id TEXT NOT NULL UNIQUE,
  client_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  content_digest TEXT NOT NULL,
  result_digest TEXT NOT NULL DEFAULT '',
  result_json TEXT NOT NULL DEFAULT '{}',
  ingress_count INTEGER NOT NULL,
  source TEXT NOT NULL,
  received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_acks_client_version ON acks(client_id, version);

CREATE TABLE IF NOT EXISTS ack_events (
  event_id TEXT PRIMARY KEY,
  ack_id TEXT NOT NULL,
  push_id TEXT NOT NULL,
  client_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  disposition TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  content_digest TEXT NOT NULL DEFAULT '',
  result_digest TEXT NOT NULL DEFAULT '',
  result_json TEXT NOT NULL DEFAULT '{}',
  ingress_count INTEGER NOT NULL,
  received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ack_events_client_version ON ack_events(client_id, version);

CREATE TABLE IF NOT EXISTS results (
  result_id TEXT PRIMARY KEY,
  client_id TEXT NOT NULL,
  push_id TEXT NOT NULL,
  picked_version INTEGER NOT NULL,
  ack_version INTEGER NOT NULL,
  result_digest TEXT NOT NULL,
  weighted_sum REAL NOT NULL,
  normalized REAL NOT NULL,
  decision INTEGER NOT NULL,
  seed INTEGER NOT NULL,
  sample_id TEXT NOT NULL,
  sample_digest TEXT NOT NULL,
  status TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  mismatches_json TEXT NOT NULL DEFAULT '[]',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_results_client_version ON results(client_id, picked_version);

CREATE TABLE IF NOT EXISTS intents (
  intent_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  payload_json TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  finished_at TEXT,
  detail_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, created_at);

CREATE TABLE IF NOT EXISTS recovery_reports (
  report_id TEXT PRIMARY KEY,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,
  report_json TEXT NOT NULL
);
`

func Open(path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	dsn := "file:" + filepath.ToSlash(abs) + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db, path: abs}, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) DB() *sql.DB  { return s.db }
func (s *Store) Path() string { return s.path }

func (s *Store) ReadTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	return fn(tx)
}

// IntentWrite serializes all business writes. The intent row is committed
// before the mutation transaction starts. commit=true commits even when err is
// non-nil, which is used for durable stale-ack history plus an E_STALE_ACK.
func (s *Store) IntentWrite(ctx context.Context, kind, aggregate string, payload any, fn func(*sql.Tx) (bool, error)) (string, error) {
	return s.IntentWriteWithID(ctx, kind, aggregate, payload, func(tx *sql.Tx, _ string) (bool, error) {
		return fn(tx)
	})
}

// IntentWriteWithID is the recovery-aware variant. The callback receives the
// already-persisted PREPARED intent id so a recovery scan can exclude its own
// operation from the facts it reports.
func (s *Store) IntentWriteWithID(ctx context.Context, kind, aggregate string, payload any, fn func(*sql.Tx, string) (bool, error)) (string, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	intentID := core.NewID("intent")
	payloadJSON := marshal(payload)
	now := core.Now()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO intents(intent_id,kind,aggregate_id,payload_json,status,created_at,detail_json) VALUES(?,?,?,?,?,?,?)`,
		intentID, kind, aggregate, payloadJSON, "PREPARED", now, "{}"); err != nil {
		return "", err
	}

	crash := os.Getenv("MW_FAULT_CRASH")
	if crash == kind+"_AFTER_INTENT" {
		fmt.Fprintf(os.Stderr, "FAULT_INJECTION crash after intent %s kind=%s\n", intentID, kind)
		os.Exit(137)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.finishIntent(intentID, "FAILED", map[string]any{"error": err.Error()})
		return intentID, err
	}
	commit, fnErr := fn(tx, intentID)
	if fnErr != nil && !commit {
		_ = tx.Rollback()
		s.finishIntent(intentID, "FAILED", map[string]any{"error": fnErr.Error()})
		return intentID, fnErr
	}
	if !commit {
		_ = tx.Rollback()
		s.finishIntent(intentID, "ABANDONED", map[string]any{"reason": "mutation requested rollback"})
		return intentID, nil
	}
	if crash == kind+"_BEFORE_COMMIT" {
		fmt.Fprintf(os.Stderr, "FAULT_INJECTION crash before commit intent %s kind=%s\n", intentID, kind)
		os.Exit(137)
	}
	detail := map[string]any{"outcome": "committed"}
	if fnErr != nil {
		detail["outcome"] = "committed_with_business_error"
		detail["error"] = fnErr.Error()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='COMMITTED', finished_at=?, detail_json=? WHERE intent_id=?`,
		core.Now(), marshal(detail), intentID); err != nil {
		_ = tx.Rollback()
		s.finishIntent(intentID, "FAILED", map[string]any{"error": err.Error()})
		return intentID, err
	}
	if err := tx.Commit(); err != nil {
		return intentID, err
	}
	return intentID, fnErr
}

func (s *Store) RawWrite(ctx context.Context, fn func(*sql.Tx) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) finishIntent(intentID, status string, detail any) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = s.db.ExecContext(ctx, `UPDATE intents SET status=?, finished_at=?, detail_json=? WHERE intent_id=?`,
		status, core.Now(), marshal(detail), intentID)
}

func marshal(v any) string {
	if v == nil {
		return "{}"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func UnmarshalMap(raw string) map[string]any {
	if strings.TrimSpace(raw) == "" {
		return map[string]any{}
	}
	var value map[string]any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return map[string]any{"_raw": raw}
	}
	return value
}
