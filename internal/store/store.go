package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	DB *sql.DB
}

type CommittedVersion struct {
	Version      int64
	BaseVersion  int64
	Kind         string
	Digest       string
	SnapshotJSON string
	DriftJSON    string
	PlanJSON     string
	SummaryJSON  string
	CreatedAt    time.Time
}

type CommitBundle struct {
	Version      CommittedVersion
	ObjectStates map[string]string
	BatchStates  map[string]string
}

type Intent struct {
	ID          string
	Type        string
	Version     int64
	BatchID     string
	ObjectID    string
	PayloadJSON string
	Status      string
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	slashPath := filepath.ToSlash(absPath)
	if !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	dsnURL := url.URL{
		Scheme: "file",
		Path:   slashPath,
		RawQuery: "_pragma=busy_timeout(5000)" +
			"&_pragma=journal_mode(WAL)" +
			"&_pragma=foreign_keys(ON)",
	}
	dsn := dsnURL.String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{DB: db}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.DB.Close() }

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, schemaSQL)
	return err
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS committed_versions (
  version INTEGER PRIMARY KEY,
  base_version INTEGER NOT NULL DEFAULT 0,
  kind TEXT NOT NULL,
  digest TEXT NOT NULL,
  snapshot_json TEXT NOT NULL,
  drift_json TEXT NOT NULL,
  plan_json TEXT NOT NULL,
  summary_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS intent (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  version INTEGER NOT NULL,
  batch_id TEXT NOT NULL DEFAULT '',
  object_id TEXT NOT NULL DEFAULT '',
  payload_json TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  last_error TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS intent_status_idx ON intent(status, updated_at);
CREATE TABLE IF NOT EXISTS object_state (
  version INTEGER NOT NULL,
  object_id TEXT NOT NULL,
  state_json TEXT NOT NULL,
  PRIMARY KEY(version, object_id)
);
CREATE TABLE IF NOT EXISTS batch_state (
  version INTEGER NOT NULL,
  batch_id TEXT NOT NULL,
  state_json TEXT NOT NULL,
  PRIMARY KEY(version, batch_id)
);
CREATE TABLE IF NOT EXISTS write_attempt (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  intent_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  batch_id TEXT NOT NULL DEFAULT '',
  object_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  status TEXT NOT NULL,
  error_code TEXT NOT NULL DEFAULT '',
  detail_json TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recovery_report (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  report_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
`

func (s *Store) CurrentVersion(ctx context.Context) (int64, error) {
	var value int64
	err := s.DB.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_version'`).Scan(&value)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (s *Store) SaveCommittedVersion(ctx context.Context, v CommittedVersion) error {
	return s.SaveCommitBundle(ctx, CommitBundle{Version: v})
}

func (s *Store) SaveCommitBundle(ctx context.Context, bundle CommitBundle) error {
	v := bundle.Version
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO committed_versions
		(version,base_version,kind,digest,snapshot_json,drift_json,plan_json,summary_json,created_at)
		VALUES (?,?,?,?,?,?,?,?,?)
		ON CONFLICT(version) DO UPDATE SET
		base_version=excluded.base_version,kind=excluded.kind,digest=excluded.digest,
		snapshot_json=excluded.snapshot_json,drift_json=excluded.drift_json,
		plan_json=excluded.plan_json,summary_json=excluded.summary_json,created_at=excluded.created_at`,
		v.Version, v.BaseVersion, v.Kind, v.Digest, v.SnapshotJSON, v.DriftJSON, v.PlanJSON, v.SummaryJSON, now)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('current_version',?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmt.Sprint(v.Version))
	if err != nil {
		return err
	}
	for objectID, stateJSON := range bundle.ObjectStates {
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_state(version,object_id,state_json) VALUES(?,?,?)
			ON CONFLICT(version,object_id) DO UPDATE SET state_json=excluded.state_json`,
			v.Version, objectID, stateJSON); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	for batchID, stateJSON := range bundle.BatchStates {
		if _, err := tx.ExecContext(ctx, `INSERT INTO batch_state(version,batch_id,state_json) VALUES(?,?,?)
			ON CONFLICT(version,batch_id) DO UPDATE SET state_json=excluded.state_json`,
			v.Version, batchID, stateJSON); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListObjectStates(ctx context.Context, version int64) (map[string]string, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT object_id,state_json FROM object_state WHERE version=?`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		out[id] = raw
	}
	return out, rows.Err()
}

func (s *Store) ListBatchStates(ctx context.Context, version int64) (map[string]string, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT batch_id,state_json FROM batch_state WHERE version=?`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		out[id] = raw
	}
	return out, rows.Err()
}

func (s *Store) LoadCommittedVersion(ctx context.Context, version int64) (CommittedVersion, error) {
	var v CommittedVersion
	var createdAt string
	err := s.DB.QueryRowContext(ctx, `SELECT version,base_version,kind,digest,snapshot_json,drift_json,plan_json,summary_json,created_at
		FROM committed_versions WHERE version=?`, version).Scan(
		&v.Version, &v.BaseVersion, &v.Kind, &v.Digest, &v.SnapshotJSON, &v.DriftJSON, &v.PlanJSON, &v.SummaryJSON, &createdAt)
	if err != nil {
		return v, err
	}
	v.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
	return v, nil
}

func (s *Store) UpsertIntent(ctx context.Context, intent Intent) error {
	now := time.Now().UTC()
	if intent.CreatedAt.IsZero() {
		intent.CreatedAt = now
	}
	intent.UpdatedAt = now
	_, err := s.DB.ExecContext(ctx, `INSERT INTO intent
		(id,type,version,batch_id,object_id,payload_json,status,last_error,created_at,updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET
		type=excluded.type,version=excluded.version,batch_id=excluded.batch_id,object_id=excluded.object_id,
		payload_json=excluded.payload_json,status=excluded.status,last_error=excluded.last_error,updated_at=excluded.updated_at`,
		intent.ID, intent.Type, intent.Version, intent.BatchID, intent.ObjectID, intent.PayloadJSON,
		intent.Status, intent.LastError, intent.CreatedAt.Format(time.RFC3339Nano), intent.UpdatedAt.Format(time.RFC3339Nano))
	return err
}

func (s *Store) SetIntentStatus(ctx context.Context, id, status, lastError string) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE intent SET status=?,last_error=?,updated_at=? WHERE id=?`,
		status, lastError, time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

func (s *Store) ListIntentsByStatus(ctx context.Context, statuses ...string) ([]Intent, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	query := `SELECT id,type,version,batch_id,object_id,payload_json,status,last_error,created_at,updated_at FROM intent WHERE status IN (`
	args := make([]any, len(statuses))
	for i, status := range statuses {
		if i > 0 {
			query += ","
		}
		query += "?"
		args[i] = status
	}
	query += `) ORDER BY created_at,id`
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Intent{}
	for rows.Next() {
		var in Intent
		var created, updated string
		if err := rows.Scan(&in.ID, &in.Type, &in.Version, &in.BatchID, &in.ObjectID, &in.PayloadJSON,
			&in.Status, &in.LastError, &created, &updated); err != nil {
			return nil, err
		}
		in.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		in.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
		out = append(out, in)
	}
	return out, rows.Err()
}

func (s *Store) PutObjectState(ctx context.Context, version int64, objectID, stateJSON string) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO object_state(version,object_id,state_json) VALUES(?,?,?)
		ON CONFLICT(version,object_id) DO UPDATE SET state_json=excluded.state_json`, version, objectID, stateJSON)
	return err
}

func (s *Store) PutBatchState(ctx context.Context, version int64, batchID, stateJSON string) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO batch_state(version,batch_id,state_json) VALUES(?,?,?)
		ON CONFLICT(version,batch_id) DO UPDATE SET state_json=excluded.state_json`, version, batchID, stateJSON)
	return err
}

func (s *Store) InsertWriteAttempt(ctx context.Context, intentID string, version int64, batchID, objectID string, attempt int, status, code, detailJSON string) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO write_attempt
		(intent_id,version,batch_id,object_id,attempt,status,error_code,detail_json,created_at)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		intentID, version, batchID, objectID, attempt, status, code, detailJSON, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) SaveRecoveryReport(ctx context.Context, reportJSON string) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO recovery_report(report_json,created_at) VALUES(?,?)`,
		reportJSON, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) SaveWriteProgress(ctx context.Context, version int64, objectState *string, batchState *string, summaryJSON string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if objectState != nil {
		var partial struct {
			ObjectID string `json:"object_id"`
		}
		if err := json.Unmarshal([]byte(*objectState), &partial); err != nil {
			return err
		}
		objectID := partial.ObjectID
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_state(version,object_id,state_json) VALUES(?,?,?)
			ON CONFLICT(version,object_id) DO UPDATE SET state_json=excluded.state_json`,
			version, objectID, *objectState); err != nil {
			return err
		}
	}
	if batchState != nil {
		var partial struct {
			BatchID string `json:"batch_id"`
		}
		if err := json.Unmarshal([]byte(*batchState), &partial); err != nil {
			return err
		}
		batchID := partial.BatchID
		if _, err := tx.ExecContext(ctx, `INSERT INTO batch_state(version,batch_id,state_json) VALUES(?,?,?)
			ON CONFLICT(version,batch_id) DO UPDATE SET state_json=excluded.state_json`,
			version, batchID, *batchState); err != nil {
			return err
		}
	}
	if summaryJSON != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE committed_versions SET summary_json=? WHERE version=?`, summaryJSON, version); err != nil {
			return err
		}
	}
	return tx.Commit()
}
