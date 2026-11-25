// Package store 用纯 Go SQLite 持久化版本化分块状态并提供条件发布保护。
package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"flashattention/internal/attn"
	"flashattention/internal/filelock"
	"flashattention/internal/tensor"
	"flashattention/internal/versioning"

	_ "modernc.org/sqlite"
)

type Store struct {
	db       *sql.DB
	path     string
	lockPath string
}

type Checkpoint struct {
	ShardID       string
	BH            int
	RowBlock      int
	ColStart      int
	ColEnd        int
	RowsCount     int
	HeadDim       int
	Seq           int64
	Strategy      string
	Snapshots     []attn.Snapshot
	InputFP       string
	VersionFP     string
	PrevHash      string
	StateHash     string
	AccHash       string
	Contributions []attn.BlockContribution
	Events        []attn.NumericEvent
}

type Record struct {
	ID            int64  `json:"id"`
	ShardID       string `json:"shard_id"`
	BH            int    `json:"bh"`
	RowBlock      int    `json:"row_block"`
	ColStart      int    `json:"col_start"`
	ColEnd        int    `json:"col_end"`
	RowsCount     int    `json:"rows_count"`
	HeadDim       int    `json:"head_dim"`
	Seq           int64  `json:"seq"`
	Strategy      string `json:"strategy"`
	InputFP       string `json:"input_fp"`
	VersionFP     string `json:"version_fp"`
	StateHash     string `json:"state_hash"`
	AccHash       string `json:"acc_hash"`
	Active        bool   `json:"active"`
	InvalidReason string `json:"invalid_reason,omitempty"`
}

type IgnoredRecord struct {
	ID     int64  `json:"id,omitempty"`
	Reason string `json:"reason"`
	Detail string `json:"detail,omitempty"`
}

type RecoveryResult struct {
	States  map[string]attn.ResumeState `json:"states"`
	HeadSeq map[string]int64            `json:"head_seq"`
	Ignored []IgnoredRecord             `json:"ignored"`
}

func Open(path string) (*Store, error) {
	if path == ":memory:" {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			return nil, err
		}
		s := &Store{db: db, path: path, lockPath: ":memory:.lock"}
		db.SetMaxOpenConns(1)
		if err := s.migrate(context.Background()); err != nil {
			db.Close()
			return nil, err
		}
		return s, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && filepath.Dir(path) != "." {
		return nil, err
	}
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	s := &Store{db: db, path: path, lockPath: path + ".lock"}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) lock(ctx context.Context) (*filelock.FileLock, error) {
	if s.path == ":memory:" {
		return nil, nil
	}
	return filelock.Acquire(ctx, s.lockPath)
}

func (s *Store) migrate(ctx context.Context) error {
	lock, err := s.lock(ctx)
	if err != nil {
		return err
	}
	defer lock.Release()

	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS versions (
  shard_id TEXT PRIMARY KEY,
  fingerprint TEXT NOT NULL,
  stages_json TEXT NOT NULL,
  revision INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS invalidations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  shard_id TEXT NOT NULL,
  trigger TEXT NOT NULL,
  old_fp TEXT NOT NULL,
  new_fp TEXT NOT NULL,
  first_row_block INTEGER NOT NULL,
  last_row_block INTEGER NOT NULL,
  row_start INTEGER NOT NULL,
  row_end INTEGER NOT NULL,
  first_col_block INTEGER NOT NULL,
  last_col_block INTEGER NOT NULL,
  col_start INTEGER NOT NULL,
  col_end INTEGER NOT NULL,
  all_rows INTEGER NOT NULL,
  all_cols INTEGER NOT NULL,
  reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoints (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  shard_id TEXT NOT NULL,
  bh INTEGER NOT NULL,
  row_block INTEGER NOT NULL,
  col_start INTEGER NOT NULL,
  col_end INTEGER NOT NULL,
  rows_count INTEGER NOT NULL,
  head_dim INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  strategy TEXT NOT NULL,
  row_max_blob BLOB NOT NULL,
  exp_sum_blob BLOB NOT NULL,
  lse_blob BLOB NOT NULL,
  acc_blob BLOB NOT NULL,
  acc_hash TEXT NOT NULL,
  input_fp TEXT NOT NULL,
  version_fp TEXT NOT NULL,
  prev_hash TEXT NOT NULL DEFAULT '',
  state_hash TEXT NOT NULL,
  events_json TEXT NOT NULL DEFAULT '[]',
  active INTEGER NOT NULL DEFAULT 1,
  invalid_reason TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_key_range
  ON checkpoints(shard_id,bh,row_block,col_start,col_end) WHERE active=1;
CREATE INDEX IF NOT EXISTS ix_checkpoint_lookup
  ON checkpoints(shard_id,bh,row_block,seq);
CREATE TABLE IF NOT EXISTS row_heads (
  shard_id TEXT NOT NULL,
  bh INTEGER NOT NULL,
  row_block INTEGER NOT NULL,
  current_seq INTEGER NOT NULL,
  checkpoint_id INTEGER NOT NULL,
  version_fp TEXT NOT NULL,
  state_hash TEXT NOT NULL,
  valid INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY(shard_id,bh,row_block)
);
CREATE TABLE IF NOT EXISTS block_contributions (
  shard_id TEXT NOT NULL,
  bh INTEGER NOT NULL,
  row_block INTEGER NOT NULL,
  col_block INTEGER NOT NULL,
  row_start INTEGER NOT NULL DEFAULT 0,
  row_end INTEGER NOT NULL DEFAULT 0,
  col_start INTEGER NOT NULL,
  col_end INTEGER NOT NULL,
  head_dim INTEGER NOT NULL,
  row_max_blob BLOB NOT NULL,
  exp_sum_blob BLOB NOT NULL,
  pv_blob BLOB NOT NULL,
  hash TEXT NOT NULL,
  version_fp TEXT NOT NULL,
  PRIMARY KEY(shard_id,bh,row_block,col_block)
);
`)
	if err != nil {
		return err
	}
	return s.addColumnIfMissing(ctx, "checkpoints", "events_json", "TEXT NOT NULL DEFAULT '[]'")
}

func (s *Store) addColumnIfMissing(ctx context.Context, table, column, declaration string) error {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, table))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return err
		}
		if name == column {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table, column, declaration))
	return err
}

func (s *Store) SaveVersion(ctx context.Context, fp versioning.Fingerprint) error {
	lock, err := s.lock(ctx)
	if err != nil {
		return err
	}
	defer lock.Release()
	raw, err := json.Marshal(fp.Stages)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO versions(shard_id,fingerprint,stages_json,revision)
VALUES(?,?,?,1)
ON CONFLICT(shard_id) DO UPDATE SET fingerprint=excluded.fingerprint, stages_json=excluded.stages_json, revision=versions.revision+1`,
		fp.Stages.Config.ShardID, fp.Hash, string(raw))
	return err
}

func (s *Store) CurrentFingerprint(ctx context.Context, shardID string) (string, error) {
	var fp string
	err := s.db.QueryRowContext(ctx, `SELECT fingerprint FROM versions WHERE shard_id=?`, shardID).Scan(&fp)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return fp, err
}

func HashCheckpoint(c *Checkpoint, mBlob, sumBlob, lseBlob, accBlob []byte) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%d|%d|%d|%d|%d|%d|%d|%s|%s|%s|%s|",
		c.ShardID, c.BH, c.RowBlock, c.ColStart, c.ColEnd, c.RowsCount, c.HeadDim, c.Seq,
		c.Strategy, c.InputFP, c.VersionFP, c.PrevHash)
	h.Write(mBlob)
	h.Write(sumBlob)
	h.Write(lseBlob)
	h.Write(accBlob)
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func AccHash(accBlob []byte) string {
	sum := sha256.Sum256(accBlob)
	return hex.EncodeToString(sum[:])[:32]
}

func blobsFromSnapshots(snaps []attn.Snapshot) (m, sum, lse, acc []byte, rows, dim int) {
	rows = len(snaps)
	if rows == 0 {
		return nil, nil, nil, nil, 0, 0
	}
	dim = len(snaps[0].Acc)
	ms := make([]float64, rows)
	ss := make([]float64, rows)
	ls := make([]float64, rows)
	accs := make([]float64, 0, rows*dim)
	for i, sn := range snaps {
		ms[i], ss[i], ls[i] = sn.M, sn.Sum, sn.LSE
		accs = append(accs, sn.Acc...)
	}
	return tensor.EncodeFloat64s(ms), tensor.EncodeFloat64s(ss), tensor.EncodeFloat64s(ls), tensor.EncodeFloat64s(accs), rows, dim
}
