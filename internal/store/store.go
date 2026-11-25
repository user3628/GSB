package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"csrouter/internal/domain"

	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	db       *sql.DB
	path     string
	lockPath string
}

type TicketRow struct {
	Ticket           domain.Ticket
	Tier             string
	ShardPath        string
	Ordinal          int
	InputFingerprint string
}

type ArtifactRecord struct {
	TicketID          string
	Stage             string
	StageSeq          int64
	Revision          int64
	InputFingerprint  string
	ConfigVersion     int64
	ConfigFingerprint string
	Digest            string
	Payload           string
	Status            string
	ErrorCode         string
}

type PublishInput struct {
	TicketID          string
	Stage             string
	InputFingerprint  string
	ConfigVersion     int64
	ConfigFingerprint string
	Artifact          any
	Status            string
	ErrorCode         string
	ExpectedHead      *int64
	ClientStageSeq    *int64
}

type PublishResult struct {
	StageSeq  int64 `json:"stage_seq"`
	Revision  int64 `json:"revision"`
	GlobalSeq int64 `json:"global_seq"`
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shards (
  path TEXT PRIMARY KEY,
  tier TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  row_count INTEGER NOT NULL,
  column_count INTEGER NOT NULL,
  declared_rows INTEGER NOT NULL,
  declared_columns INTEGER NOT NULL,
  config_version INTEGER NOT NULL DEFAULT 1,
  updated_at_logical INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tickets (
  ticket_id TEXT PRIMARY KEY,
  tier TEXT NOT NULL,
  shard_path TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  raw_json TEXT NOT NULL,
  input_fingerprint TEXT NOT NULL,
  intent_label TEXT NOT NULL,
  priority_label INTEGER,
  confidence REAL,
  current_config_version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_tickets_tier ON tickets(tier, ticket_id);
CREATE INDEX IF NOT EXISTS idx_tickets_shard ON tickets(shard_path);
CREATE TABLE IF NOT EXISTS policies (
  tier TEXT NOT NULL,
  version INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  change_code TEXT NOT NULL,
  note TEXT NOT NULL DEFAULT '',
  based_on_version INTEGER NOT NULL DEFAULT 0,
  rollback_of_version INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(tier, version)
);
CREATE TABLE IF NOT EXISTS stage_cursors (
  stage TEXT PRIMARY KEY,
  last_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS artifacts (
  ticket_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  stage_seq INTEGER NOT NULL,
  revision INTEGER NOT NULL DEFAULT 1,
  input_fingerprint TEXT NOT NULL,
  config_version INTEGER NOT NULL,
  config_fingerprint TEXT NOT NULL,
  digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL,
  error_code TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(ticket_id, stage)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_artifacts_stage_seq ON artifacts(stage, stage_seq);
CREATE TABLE IF NOT EXISTS artifact_ledger (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  stage TEXT NOT NULL,
  stage_seq INTEGER NOT NULL,
  ticket_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  digest TEXT NOT NULL,
  config_version INTEGER NOT NULL,
  status TEXT NOT NULL,
  error_code TEXT NOT NULL DEFAULT '',
  operation TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ledger_stage_seq ON artifact_ledger(stage, stage_seq);
CREATE TABLE IF NOT EXISTS stop_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  requested INTEGER NOT NULL DEFAULT 0
);
INSERT INTO stop_state(id, requested) VALUES(1, 0) ON CONFLICT(id) DO NOTHING;
CREATE TABLE IF NOT EXISTS reports (
  tier TEXT PRIMARY KEY,
  policy_version INTEGER NOT NULL,
  fingerprint TEXT NOT NULL,
  payload_json TEXT NOT NULL
);
`

func Open(path string) (*Store, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
	}
	dsn := path
	if path != ":memory:" {
		dsn = path + "?_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=on&_txlock=immediate"
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize sqlite schema: %w", err)
	}
	lockPath := path + ".lock"
	if path == ":memory:" {
		lockPath = filepath.Join(os.TempDir(), "csrouter-memory.lock")
	}
	return &Store{db: db, path: path, lockPath: lockPath}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) WithFileLock(fn func() error) error {
	f, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return err
	}
	defer func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}()
	return fn()
}

func (s *Store) SetMeta(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO meta(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

func (s *Store) GetMeta(key string) (string, error) {
	var value string
	err := s.db.QueryRow(`SELECT value FROM meta WHERE key=?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

func (s *Store) RequestStop() error {
	_, err := s.db.Exec(`UPDATE stop_state SET requested=1 WHERE id=1`)
	return err
}

func (s *Store) ClearStop() error {
	_, err := s.db.Exec(`UPDATE stop_state SET requested=0 WHERE id=1`)
	return err
}

func (s *Store) StopRequested() (bool, error) {
	var requested int
	if err := s.db.QueryRow(`SELECT requested FROM stop_state WHERE id=1`).Scan(&requested); err != nil {
		return false, err
	}
	return requested == 1, nil
}

func (s *Store) SavePolicy(policy domain.Policy) error {
	payload, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO policies(tier,version,payload_json,fingerprint,change_code,note,based_on_version,rollback_of_version)
		VALUES(?,?,?,?,?,?,?,?)
		ON CONFLICT(tier,version) DO UPDATE SET
			payload_json=excluded.payload_json,
			fingerprint=excluded.fingerprint,
			change_code=excluded.change_code,
			note=excluded.note,
			based_on_version=excluded.based_on_version,
			rollback_of_version=excluded.rollback_of_version`,
		policy.Tier, policy.EffectiveVersion(), string(payload), policy.Fingerprint(),
		policy.ChangeCode, policy.ChangeNote, policy.BasedOnVersion, policy.RollbackOfVersion)
	return err
}

func (s *Store) LatestPolicy(tier string) (*domain.Policy, error) {
	var payload string
	err := s.db.QueryRow(`SELECT payload_json FROM policies WHERE tier=? ORDER BY version DESC LIMIT 1`, tier).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var policy domain.Policy
	if err := json.Unmarshal([]byte(payload), &policy); err != nil {
		return nil, err
	}
	return &policy, nil
}

func (s *Store) GetPolicy(tier string, version int64) (*domain.Policy, error) {
	var payload string
	err := s.db.QueryRow(`SELECT payload_json FROM policies WHERE tier=? AND version=?`, tier, version).Scan(&payload)
	if err != nil {
		return nil, err
	}
	var policy domain.Policy
	if err := json.Unmarshal([]byte(payload), &policy); err != nil {
		return nil, err
	}
	return &policy, nil
}

func (s *Store) ListPolicyVersions(tier string) ([]domain.Policy, error) {
	rows, err := s.db.Query(`SELECT payload_json FROM policies WHERE tier=? ORDER BY version ASC`, tier)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Policy
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var policy domain.Policy
		if err := json.Unmarshal([]byte(payload), &policy); err != nil {
			return nil, err
		}
		out = append(out, policy)
	}
	return out, rows.Err()
}
