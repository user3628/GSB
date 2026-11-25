package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type StageRecord struct {
	Seq              int64
	Stage            string
	QueryID          string
	InputFingerprint string
	ConfigVersion    int64
	OutputDigest     string
	OutputJSON       []byte
	Status           string
	CreatedAtNS      int64
}

type CheckpointView struct {
	Seq              int64  `json:"seq"`
	Stage            string `json:"stage"`
	QueryID          string `json:"query_id"`
	InputFingerprint string `json:"input_fingerprint"`
	ConfigVersion    int64  `json:"config_version"`
	OutputDigest     string `json:"output_digest"`
	Status           string `json:"status"`
	CreatedAtNS      int64  `json:"created_at_ns"`
}

type Store struct {
	db          *sql.DB
	path        string
	AfterCommit func(seq int64, queryID, stage string)
}

func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	dsn := path + "?_busy_timeout=5000&_synchronous=FULL&_foreign_keys=on"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA synchronous=FULL;`); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db, path: path}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS checkpoint (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  query_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  input_fingerprint TEXT NOT NULL,
  config_version INTEGER NOT NULL,
  output_digest TEXT NOT NULL,
  output_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'valid' CHECK (status IN ('valid','invalid')),
  created_at_ns INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_checkpoint_one_valid
  ON checkpoint(query_id, stage) WHERE status = 'valid';
CREATE INDEX IF NOT EXISTS idx_checkpoint_query ON checkpoint(query_id, seq);
CREATE TABLE IF NOT EXISTS config_revision (
  version INTEGER PRIMARY KEY,
  base_version INTEGER NOT NULL,
  kind TEXT NOT NULL,
  digest TEXT NOT NULL,
  snapshot_json TEXT NOT NULL,
  created_at_ns INTEGER NOT NULL
);
`)
	return err
}

func (s *Store) CommitStage(ctx context.Context, rec StageRecord) (int64, error) {
	if rec.Status == "" {
		rec.Status = "valid"
	}
	if rec.CreatedAtNS == 0 {
		rec.CreatedAtNS = time.Now().UnixNano()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
UPDATE checkpoint SET status='invalid'
WHERE query_id=? AND stage=? AND status='valid'`, rec.QueryID, rec.Stage); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `
INSERT INTO checkpoint
 (query_id, stage, input_fingerprint, config_version, output_digest, output_json, status, created_at_ns)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.QueryID, rec.Stage, rec.InputFingerprint, rec.ConfigVersion, rec.OutputDigest,
		string(rec.OutputJSON), rec.Status, rec.CreatedAtNS)
	if err != nil {
		return 0, err
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	if s.AfterCommit != nil {
		s.AfterCommit(seq, rec.QueryID, rec.Stage)
	}
	return seq, nil
}

func (s *Store) GetValid(ctx context.Context, queryID, stage string) (*StageRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT seq, query_id, stage, input_fingerprint, config_version, output_digest, output_json, status, created_at_ns
FROM checkpoint
WHERE query_id=? AND stage=? AND status='valid'`, queryID, stage)
	var rec StageRecord
	var output string
	err := row.Scan(&rec.Seq, &rec.QueryID, &rec.Stage, &rec.InputFingerprint, &rec.ConfigVersion,
		&rec.OutputDigest, &output, &rec.Status, &rec.CreatedAtNS)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec.OutputJSON = []byte(output)
	return &rec, nil
}

func (s *Store) ListCheckpoints(ctx context.Context) ([]CheckpointView, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT seq, stage, query_id, input_fingerprint, config_version, output_digest, status, created_at_ns
FROM checkpoint ORDER BY seq ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CheckpointView
	for rows.Next() {
		var v CheckpointView
		if err := rows.Scan(&v.Seq, &v.Stage, &v.QueryID, &v.InputFingerprint, &v.ConfigVersion,
			&v.OutputDigest, &v.Status, &v.CreatedAtNS); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) InvalidateFromStage(ctx context.Context, stage string, order []string) (int64, error) {
	idx := -1
	for i, candidate := range order {
		if candidate == stage {
			idx = i
			break
		}
	}
	if idx < 0 {
		return 0, fmt.Errorf("unknown stage %q", stage)
	}
	stages := order[idx:]
	query := `UPDATE checkpoint SET status='invalid' WHERE status='valid' AND stage IN (`
	args := make([]any, 0, len(stages))
	for i, st := range stages {
		if i > 0 {
			query += ","
		}
		query += "?"
		args = append(args, st)
	}
	query += ")"
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) SaveConfigRevision(ctx context.Context, version, baseVersion int64, kind, digest string, raw []byte) error {
	_, err := s.db.ExecContext(ctx, `
INSERT OR REPLACE INTO config_revision(version, base_version, kind, digest, snapshot_json, created_at_ns)
VALUES (?, ?, ?, ?, ?, ?)`, version, baseVersion, kind, digest, string(raw), time.Now().UnixNano())
	return err
}
