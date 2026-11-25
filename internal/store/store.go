package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	_ "modernc.org/sqlite"

	"imretrace/internal/model"
)

type Store struct {
	db       *sql.DB
	lockPath string
}

func Open(ctx context.Context, paths model.Paths) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(paths.DBPath), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(paths.LockPath), 0o755); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_txlock=immediate", paths.DBPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(6)
	db.SetMaxIdleConns(3)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db, lockPath: paths.LockPath}
	if err := s.initSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) initSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS policies (
  version INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,
  content_hash TEXT NOT NULL UNIQUE,
  canonical_json TEXT NOT NULL,
  min_acceptable_version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS items (
  item_id TEXT PRIMARY KEY,
  shard_id TEXT NOT NULL,
  image_file TEXT NOT NULL,
  caption TEXT NOT NULL,
  lang TEXT NOT NULL,
  source TEXT NOT NULL,
  input_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shards (
  shard_id TEXT PRIMARY KEY,
  current_seq INTEGER NOT NULL DEFAULT 0,
  current_stage TEXT NOT NULL DEFAULT '',
  policy_version INTEGER NOT NULL DEFAULT 0,
  input_hash TEXT NOT NULL DEFAULT '',
  artifact_digest TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS checkpoints (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  shard_id TEXT NOT NULL,
  item_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  seq INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  policy_version INTEGER NOT NULL,
  policy_hash TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  quant_param_hash TEXT NOT NULL DEFAULT '',
  artifact_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_checkpoint_active_shard_stage
  ON checkpoints(shard_id, stage) WHERE status='active';
CREATE UNIQUE INDEX IF NOT EXISTS uq_checkpoint_active_shard_seq
  ON checkpoints(shard_id, seq) WHERE status='active';
CREATE INDEX IF NOT EXISTS idx_checkpoints_policy ON checkpoints(policy_version, status);
CREATE INDEX IF NOT EXISTS idx_items_shard ON items(shard_id);
`)
	return err
}

type fileLock struct {
	f *os.File
}

func acquireFileLock(path string) (*fileLock, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	return l.f.Close()
}

func (s *Store) withLock(fn func() error) error {
	lock, err := acquireFileLock(s.lockPath)
	if err != nil {
		return fmt.Errorf("acquire publish file lock: %w", err)
	}
	defer lock.release()
	return fn()
}

func (s *Store) UpsertItems(ctx context.Context, items []model.ManifestItem, inputHashes map[string]string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, item := range items {
		inputHash := inputHashes[item.ItemID]
		if _, err := tx.ExecContext(ctx, `
INSERT INTO items(item_id,shard_id,image_file,caption,lang,source,input_hash)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT(item_id) DO UPDATE SET
 shard_id=excluded.shard_id,image_file=excluded.image_file,caption=excluded.caption,
 lang=excluded.lang,source=excluded.source,input_hash=excluded.input_hash`,
			item.ItemID, item.ShardID, item.ImageFile, item.Caption, item.Lang, item.Source, inputHash); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListItems(ctx context.Context) ([]model.ManifestItem, map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT item_id,shard_id,image_file,caption,lang,source,input_hash FROM items ORDER BY item_id`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	items := []model.ManifestItem{}
	hashes := map[string]string{}
	for rows.Next() {
		var item model.ManifestItem
		var hash string
		if err := rows.Scan(&item.ItemID, &item.ShardID, &item.ImageFile, &item.Caption, &item.Lang, &item.Source, &hash); err != nil {
			return nil, nil, err
		}
		items = append(items, item)
		hashes[item.ItemID] = hash
	}
	return items, hashes, rows.Err()
}

func (s *Store) GetItem(ctx context.Context, itemID string) (model.ManifestItem, string, error) {
	var item model.ManifestItem
	var hash string
	err := s.db.QueryRowContext(ctx, `SELECT item_id,shard_id,image_file,caption,lang,source,input_hash FROM items WHERE item_id=?`, itemID).
		Scan(&item.ItemID, &item.ShardID, &item.ImageFile, &item.Caption, &item.Lang, &item.Source, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return item, "", model.NewBiz(model.CodeQueryInvalid, "store", itemID, "item not found")
	}
	return item, hash, err
}

func (s *Store) CurrentPolicy(ctx context.Context) (*model.Policy, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_policy'`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p, err := model.ParsePolicyJSON([]byte(raw))
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) ActivatePolicy(ctx context.Context, p model.Policy) (model.Policy, bool, error) {
	if err := p.Validate(); err != nil {
		return model.Policy{}, false, err
	}
	var changed bool
	err := s.withLock(func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var raw string
		var current *model.Policy
		err = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_policy'`).Scan(&raw)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			cp, perr := model.ParsePolicyJSON([]byte(raw))
			if perr != nil {
				return perr
			}
			current = &cp
		}
		accepted, didChange, aerr := model.AcceptIncomingPolicy(current, p)
		if aerr != nil {
			return aerr
		}
		p = accepted
		changed = didChange
		canonical, err := model.CanonicalBytes(p)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO policies(version,kind,content_hash,canonical_json,min_acceptable_version)
VALUES(?,?,?,?,?)
ON CONFLICT(version) DO UPDATE SET kind=excluded.kind,content_hash=excluded.content_hash,
 canonical_json=excluded.canonical_json,min_acceptable_version=excluded.min_acceptable_version`,
			p.Version, p.Kind, p.ContentHash, string(canonical), p.MinAcceptableVersion); err != nil {
			return err
		}
		if changed && current != nil && current.Version != p.Version {
			if _, err := tx.ExecContext(ctx, `UPDATE checkpoints SET status='superseded' WHERE status='active'`); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE shards SET current_seq=0,current_stage='',policy_version=?`, p.Version); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO meta(key,value) VALUES('current_policy',?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value`, string(canonical)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO meta(key,value) VALUES('current_version',?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmt.Sprint(p.Version)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO meta(key,value) VALUES('min_acceptable_version',?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmt.Sprint(p.MinAcceptableVersion)); err != nil {
			return err
		}
		return tx.Commit()
	})
	return p, changed, err
}

type Checkpoint struct {
	ID             int64  `json:"id"`
	ShardID        string `json:"shard_id"`
	ItemID         string `json:"item_id"`
	Stage          string `json:"stage"`
	Seq            int    `json:"seq"`
	Status         string `json:"status"`
	PolicyVersion  int    `json:"policy_version"`
	PolicyHash     string `json:"policy_hash"`
	InputHash      string `json:"input_hash"`
	QuantParamHash string `json:"quant_param_hash"`
	ArtifactDigest string `json:"artifact_digest"`
	PayloadJSON    string `json:"payload_json,omitempty"`
}

type PublishInput struct {
	ShardID        string
	ItemID         string
	Stage          string
	Seq            int
	PolicyVersion  int
	PolicyHash     string
	InputHash      string
	QuantParamHash string
	ArtifactDigest string
	PayloadJSON    string
}

func (s *Store) PublishCheckpoint(ctx context.Context, in PublishInput) (*Checkpoint, error) {
	var saved *Checkpoint
	err := s.withLock(func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		var itemShard string
		err = tx.QueryRowContext(ctx, `SELECT shard_id FROM items WHERE item_id=?`, in.ItemID).Scan(&itemShard)
		if errors.Is(err, sql.ErrNoRows) {
			return model.NewBiz(model.CodeStaleCheckpoint, in.Stage, in.ItemID, "cannot publish unknown item")
		}
		if err != nil {
			return err
		}
		if itemShard != in.ShardID {
			biz := model.NewBiz(model.CodeStaleCheckpoint, in.Stage, in.ItemID, "cross-shard publish rejected: payload shard %s but item belongs to %s", in.ShardID, itemShard)
			biz.Reason = "cross_shard_mismatch"
			return biz
		}

		if _, err := tx.ExecContext(ctx, `
INSERT INTO shards(shard_id,current_seq,current_stage,policy_version)
VALUES(?,0,'',?) ON CONFLICT(shard_id) DO NOTHING`, in.ShardID, in.PolicyVersion); err != nil {
			return err
		}
		var currentSeq, currentPolicy int
		var currentStage string
		if err := tx.QueryRowContext(ctx, `SELECT current_seq,current_stage,policy_version FROM shards WHERE shard_id=?`, in.ShardID).
			Scan(&currentSeq, &currentStage, &currentPolicy); err != nil {
			return err
		}
		if currentPolicy != 0 && currentPolicy != in.PolicyVersion {
			biz := model.NewBiz(model.CodeStaleVersion, in.Stage, in.ItemID, "publish policy version %d does not match active shard generation %d", in.PolicyVersion, currentPolicy)
			biz.CurrentVersion = currentPolicy
			return biz
		}
		expectedSeq := currentSeq + 1
		if in.Seq != expectedSeq || in.Seq < 1 || in.Seq > len(model.StageNames) {
			biz := model.NewBiz(model.CodeStaleCheckpoint, in.Stage, in.ItemID,
				"stale/duplicate/out-of-order publish seq=%d expected=%d after current_seq=%d", in.Seq, expectedSeq, currentSeq)
			biz.CurrentSeq = currentSeq
			biz.ExpectedSeq = expectedSeq
			biz.Reason = "sequence_not_contiguous"
			return biz
		}
		res, err := tx.ExecContext(ctx, `
UPDATE shards SET current_seq=?, current_stage=?, policy_version=?, input_hash=?, artifact_digest=?
WHERE shard_id=? AND current_seq=?`,
			in.Seq, in.Stage, in.PolicyVersion, in.InputHash, in.ArtifactDigest, in.ShardID, currentSeq)
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			biz := model.NewBiz(model.CodeStaleCheckpoint, in.Stage, in.ItemID, "conditional checkpoint update lost race")
			biz.CurrentSeq = currentSeq
			biz.ExpectedSeq = expectedSeq
			return biz
		}
		insert, err := tx.ExecContext(ctx, `
INSERT INTO checkpoints(shard_id,item_id,stage,seq,status,policy_version,policy_hash,input_hash,quant_param_hash,artifact_digest,payload_json)
VALUES(?,?,?,?,'active',?,?,?,?,?,?)`,
			in.ShardID, in.ItemID, in.Stage, in.Seq, in.PolicyVersion, in.PolicyHash, in.InputHash, in.QuantParamHash, in.ArtifactDigest, in.PayloadJSON)
		if err != nil {
			return err
		}
		id, err := insert.LastInsertId()
		if err != nil {
			return err
		}
		saved = &Checkpoint{ID: id, ShardID: in.ShardID, ItemID: in.ItemID, Stage: in.Stage, Seq: in.Seq, Status: "active", PolicyVersion: in.PolicyVersion, PolicyHash: in.PolicyHash, InputHash: in.InputHash, QuantParamHash: in.QuantParamHash, ArtifactDigest: in.ArtifactDigest, PayloadJSON: in.PayloadJSON}
		return tx.Commit()
	})
	return saved, err
}

func (s *Store) ActiveCheckpoints(ctx context.Context, policyVersion int) ([]Checkpoint, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id,shard_id,item_id,stage,seq,status,policy_version,policy_hash,input_hash,quant_param_hash,artifact_digest,payload_json
FROM checkpoints WHERE status='active' AND policy_version=? ORDER BY shard_id,seq`, policyVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCheckpoints(rows)
}

func (s *Store) AllCheckpoints(ctx context.Context) ([]Checkpoint, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id,shard_id,item_id,stage,seq,status,policy_version,policy_hash,input_hash,quant_param_hash,artifact_digest,payload_json
FROM checkpoints ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCheckpoints(rows)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanCheckpoints(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]Checkpoint, error) {
	out := []Checkpoint{}
	for rows.Next() {
		var c Checkpoint
		if err := rows.Scan(&c.ID, &c.ShardID, &c.ItemID, &c.Stage, &c.Seq, &c.Status, &c.PolicyVersion, &c.PolicyHash, &c.InputHash, &c.QuantParamHash, &c.ArtifactDigest, &c.PayloadJSON); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) ListPolicies(ctx context.Context) ([]model.Policy, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT canonical_json FROM policies ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Policy{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		p, err := model.ParsePolicyJSON([]byte(raw))
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) ShardStates(ctx context.Context) ([]map[string]any, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT shard_id,current_seq,current_stage,policy_version,input_hash,artifact_digest FROM shards ORDER BY shard_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var shard, stage, input, digest string
		var seq, version int
		if err := rows.Scan(&shard, &seq, &stage, &version, &input, &digest); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"shard_id": shard, "current_seq": seq, "current_stage": stage, "policy_version": version, "input_hash": input, "artifact_digest": digest})
	}
	return out, rows.Err()
}
