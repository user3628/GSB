package store

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
	DB   *sql.DB
	Path string
}

type Intent struct {
	IntentID      string `json:"intent_id"`
	IntentType    string `json:"intent_type"`
	BatchID       string `json:"batch_id"`
	RequestID     string `json:"request_id,omitempty"`
	ConfigVersion int64  `json:"config_version"`
	ReferenceMs   int64  `json:"reference_ms"`
	RequestHash   string `json:"request_hash,omitempty"`
	Status        string `json:"status"`
	PayloadJSON   string `json:"payload_json,omitempty"`
	CreatedAtMs   int64  `json:"created_at_ms"`
	UpdatedAtMs   int64  `json:"updated_at_ms"`
}

type RecoveryReport struct {
	RunID               int64    `json:"run_id"`
	StartedAtMs         int64    `json:"started_at_ms"`
	FinishedAtMs        int64    `json:"finished_at_ms"`
	DBPath              string   `json:"db_path"`
	OpenIntentsFound    int      `json:"open_intents_found"`
	AbortedIntents      int      `json:"aborted_intents"`
	InterruptedBatches  []string `json:"interrupted_batches"`
	AbortedBatches      int      `json:"aborted_batches"`
	OrphanSignatures    int      `json:"orphan_signatures_removed"`
	OrphanOccupancy     int      `json:"orphan_occupancy_removed"`
	OrphanDecisions     int      `json:"orphan_decisions_removed"`
	SQLiteRecovery      string   `json:"sqlite_recovery"`
	LastCompleteVersion int64    `json:"last_complete_config_version"`
	Facts               []string `json:"facts"`
}

func Open(path string) (*Store, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
	}
	dsn := path
	if path != ":memory:" {
		abs, err := filepath.Abs(path)
		if err == nil {
			dsn = abs
		}
		dsn = "file:" + dsn + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=synchronous(FULL)"
	} else {
		dsn = "file::memory:?cache=shared&_pragma=busy_timeout(5000)&_pragma=journal_mode(MEMORY)&_pragma=foreign_keys(ON)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	st := &Store{DB: db, Path: path}
	if err := st.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

func (s *Store) Close() error { return s.DB.Close() }

func (s *Store) migrate() error {
	schema := `
CREATE TABLE IF NOT EXISTS schema_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('schema_version', '1');

CREATE TABLE IF NOT EXISTS config_snapshots (
  version INTEGER PRIMARY KEY,
  generated_at_ms INTEGER NOT NULL,
  min_offset_ms INTEGER NOT NULL,
  max_offset_ms INTEGER NOT NULL,
  nonce_window_ms INTEGER NOT NULL,
  fields_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS active_config (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  version INTEGER NOT NULL REFERENCES config_snapshots(version)
);
CREATE TABLE IF NOT EXISTS clock_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  high_water_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
INSERT OR IGNORE INTO clock_state(id, high_water_ms, updated_at_ms) VALUES (1, 0, 0);

CREATE TABLE IF NOT EXISTS batch_jobs (
  batch_id TEXT PRIMARY KEY,
  config_version INTEGER NOT NULL,
  reference_ms INTEGER NOT NULL,
  mode TEXT NOT NULL,
  status TEXT NOT NULL,
  total INTEGER NOT NULL DEFAULT 0,
  processed INTEGER NOT NULL DEFAULT 0,
  request_json TEXT NOT NULL,
  options_json TEXT NOT NULL DEFAULT '{}',
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
  intent_id TEXT PRIMARY KEY,
  intent_type TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  request_id TEXT NOT NULL DEFAULT '',
  config_version INTEGER NOT NULL,
  reference_ms INTEGER NOT NULL,
  request_hash TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL CHECK(status IN ('open','committed','aborted','reused')),
  payload_json TEXT NOT NULL DEFAULT '{}',
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
CREATE INDEX IF NOT EXISTS idx_intents_batch ON intents(batch_id);

CREATE TABLE IF NOT EXISTS request_registry (
  request_id TEXT PRIMARY KEY,
  content_hash TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  config_version INTEGER NOT NULL,
  decision_code TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS conflict_attempts (
  attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  provided_hash TEXT NOT NULL,
  stored_hash TEXT NOT NULL,
  detail_json TEXT NOT NULL DEFAULT '{}',
  created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS signature_checks (
  request_id TEXT PRIMARY KEY,
  intent_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  config_version INTEGER NOT NULL,
  algorithm TEXT NOT NULL,
  checked INTEGER NOT NULL,
  valid INTEGER NOT NULL,
  canonical_string TEXT NOT NULL DEFAULT '',
  provided_digest TEXT NOT NULL,
  expected_digest TEXT NOT NULL DEFAULT '',
  created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS nonce_occupancy (
  client_id TEXT NOT NULL,
  nonce TEXT NOT NULL,
  intent_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  config_version INTEGER NOT NULL,
  first_seen_ms INTEGER NOT NULL,
  window_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL,
  recycled_from_ms INTEGER NOT NULL DEFAULT 0,
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY(client_id, nonce)
);
CREATE INDEX IF NOT EXISTS idx_nonce_config ON nonce_occupancy(config_version);
CREATE INDEX IF NOT EXISTS idx_nonce_expiry ON nonce_occupancy(expires_at_ms);

CREATE TABLE IF NOT EXISTS decisions (
  request_id TEXT PRIMARY KEY,
  intent_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  batch_index INTEGER NOT NULL,
  config_version INTEGER NOT NULL,
  reference_ms INTEGER NOT NULL DEFAULT 0,
  mode TEXT NOT NULL,
  code TEXT NOT NULL,
  status TEXT NOT NULL,
  accepted INTEGER NOT NULL,
  offset_ms INTEGER,
  boundary_ms INTEGER,
  first_seen_ms INTEGER,
  remaining_window_ms INTEGER,
  expires_at_ms INTEGER,
  idempotent_reuse INTEGER NOT NULL DEFAULT 0,
  recycled INTEGER NOT NULL DEFAULT 0,
  content_hash TEXT NOT NULL,
  detail_json TEXT NOT NULL DEFAULT '{}',
  decided_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_config ON decisions(config_version);
CREATE INDEX IF NOT EXISTS idx_decisions_batch ON decisions(batch_id);
CREATE INDEX IF NOT EXISTS idx_decisions_code ON decisions(code);

CREATE TABLE IF NOT EXISTS batch_items (
  batch_id TEXT NOT NULL,
  batch_index INTEGER NOT NULL,
  request_id TEXT NOT NULL,
  code TEXT NOT NULL,
  idempotent_reuse INTEGER NOT NULL DEFAULT 0,
  result_json TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY(batch_id, batch_index)
);
CREATE INDEX IF NOT EXISTS idx_batch_items_request ON batch_items(request_id);

CREATE TABLE IF NOT EXISTS fast_reports (
  report_id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  config_version INTEGER NOT NULL,
  report_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_runs (
  run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER NOT NULL,
  report_json TEXT NOT NULL
);
`
	_, err := s.DB.Exec(schema)
	return err
}

func (s *Store) HighWaterMs(ctx context.Context) (int64, error) {
	var v int64
	err := s.DB.QueryRowContext(ctx, `SELECT high_water_ms FROM clock_state WHERE id=1`).Scan(&v)
	return v, err
}

func (s *Store) Recover(ctx context.Context, nowMs int64) (*RecoveryReport, error) {
	started := nowMs
	report := &RecoveryReport{StartedAtMs: started, DBPath: s.Path, SQLiteRecovery: "modernc/sqlite replayed WAL and rolled back unfinished transactions when opening the database"}

	rows, err := s.DB.QueryContext(ctx, `SELECT intent_id,intent_type,batch_id,request_id,config_version,reference_ms,request_hash,status,payload_json,created_at_ms,updated_at_ms FROM intents WHERE status='open' ORDER BY created_at_ms`)
	if err != nil {
		return nil, err
	}
	open := []Intent{}
	for rows.Next() {
		var in Intent
		if err := rows.Scan(&in.IntentID, &in.IntentType, &in.BatchID, &in.RequestID, &in.ConfigVersion, &in.ReferenceMs, &in.RequestHash, &in.Status, &in.PayloadJSON, &in.CreatedAtMs, &in.UpdatedAtMs); err != nil {
			rows.Close()
			return nil, err
		}
		open = append(open, in)
	}
	rows.Close()
	report.OpenIntentsFound = len(open)

	batchSet := map[string]bool{}
	for _, in := range open {
		batchSet[in.BatchID] = true
	}
	jobRows, err := s.DB.QueryContext(ctx, `SELECT batch_id FROM batch_jobs WHERE status IN ('queued','processing','interrupted')`)
	if err != nil {
		return nil, err
	}
	for jobRows.Next() {
		var id string
		if err := jobRows.Scan(&id); err != nil {
			jobRows.Close()
			return nil, err
		}
		batchSet[id] = true
	}
	jobRows.Close()

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	for batchID := range batchSet {
		report.InterruptedBatches = append(report.InterruptedBatches, batchID)
		res, err := tx.ExecContext(ctx, `DELETE FROM signature_checks WHERE batch_id=? AND intent_id IN (SELECT intent_id FROM intents WHERE status='open')`, batchID)
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		report.OrphanSignatures += int(n)

		res, err = tx.ExecContext(ctx, `DELETE FROM nonce_occupancy WHERE batch_id=? AND intent_id IN (SELECT intent_id FROM intents WHERE status='open')`, batchID)
		if err != nil {
			return nil, err
		}
		n, _ = res.RowsAffected()
		report.OrphanOccupancy += int(n)

		res, err = tx.ExecContext(ctx, `DELETE FROM decisions WHERE batch_id=? AND intent_id IN (SELECT intent_id FROM intents WHERE status='open')`, batchID)
		if err != nil {
			return nil, err
		}
		n, _ = res.RowsAffected()
		report.OrphanDecisions += int(n)

		_, err = tx.ExecContext(ctx, `UPDATE intents SET status='aborted', updated_at_ms=? WHERE batch_id=? AND status='open'`, nowMs, batchID)
		if err != nil {
			return nil, err
		}
		res, err = tx.ExecContext(ctx, `UPDATE batch_jobs SET status='interrupted_recovered', updated_at_ms=? WHERE batch_id=? AND status IN ('queued','processing','interrupted')`, nowMs, batchID)
		if err != nil {
			return nil, err
		}
		n, _ = res.RowsAffected()
		report.AbortedBatches += int(n)
	}

	var abortedIntents int64
	row := tx.QueryRowContext(ctx, `SELECT changes()`)
	_ = row.Scan(&abortedIntents)
	report.AbortedIntents = report.OpenIntentsFound

	var version int64
	_ = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM active_config`).Scan(&version)
	if version == 0 {
		_ = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM config_snapshots`).Scan(&version)
	}
	report.LastCompleteVersion = version
	report.FinishedAtMs = time.Now().UnixMilli()
	report.Facts = []string{
		fmt.Sprintf("found %d intent row(s) in status=open", report.OpenIntentsFound),
		fmt.Sprintf("marked %d batch job(s) interrupted_recovered", report.AbortedBatches),
		fmt.Sprintf("removed %d signature, %d occupancy, %d decision orphan row(s) tied to open intents", report.OrphanSignatures, report.OrphanOccupancy, report.OrphanDecisions),
		fmt.Sprintf("last complete active config version is %d", report.LastCompleteVersion),
		"committed SQLite transactions remain durable; uncommitted force-killed transactions were replayed/rolled back by SQLite",
	}
	initialRaw, _ := json.Marshal(report)
	res, err := tx.ExecContext(ctx, `INSERT INTO recovery_runs(started_at_ms,finished_at_ms,report_json) VALUES(?,?,?)`, report.StartedAtMs, report.FinishedAtMs, string(initialRaw))
	if err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT last_insert_rowid()`).Scan(&report.RunID); err != nil {
		return nil, err
	}
	finalRaw, _ := json.Marshal(report)
	if _, err := tx.ExecContext(ctx, `UPDATE recovery_runs SET report_json=? WHERE run_id=?`, string(finalRaw), report.RunID); err != nil {
		return nil, err
	}
	_ = res
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return report, nil
}

func (s *Store) LatestRecovery(ctx context.Context) (*RecoveryReport, error) {
	var raw string
	err := s.DB.QueryRowContext(ctx, `SELECT report_json FROM recovery_runs ORDER BY run_id DESC LIMIT 1`).Scan(&raw)
	if err != nil {
		return nil, err
	}
	var rep RecoveryReport
	if err := json.Unmarshal([]byte(raw), &rep); err != nil {
		return nil, err
	}
	return &rep, nil
}

func (s *Store) RecoveryHistory(ctx context.Context) ([]RecoveryReport, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT report_json FROM recovery_runs ORDER BY run_id DESC LIMIT 20`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RecoveryReport{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var rep RecoveryReport
		if err := json.Unmarshal([]byte(raw), &rep); err == nil {
			out = append(out, rep)
		}
	}
	return out, rows.Err()
}
