package pipeline

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// Store is the durable checkpoint metadata database. A single connection with
// busy handling serializes writers; cross-process mutual exclusion additionally
// uses a real file lock (see lock.go).
type Store struct {
	db *sql.DB
}

func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", path+"?_busy_timeout=5000&_foreign_keys=on")
	if err != nil {
		return nil, bizError(CodeInternal, "open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) init() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS checkpoints(
			seq INTEGER NOT NULL,
			shard_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			view_id TEXT NOT NULL DEFAULT '',
			input_digest TEXT NOT NULL,
			config_version TEXT NOT NULL,
			artifact_hash TEXT NOT NULL,
			artifact_path TEXT NOT NULL,
			strategy TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL,
			PRIMARY KEY(shard_id, stage, view_id)
		)`,
		`CREATE TABLE IF NOT EXISTS seq_cursor(
			shard_id TEXT PRIMARY KEY,
			next_seq INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jobs(
			job_id TEXT PRIMARY KEY,
			strategy TEXT NOT NULL,
			status TEXT NOT NULL,
			config_version TEXT NOT NULL,
			scene_fingerprint TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			detail TEXT NOT NULL DEFAULT ''
		)`,
	}
	for _, st := range stmts {
		if _, err := s.db.Exec(st); err != nil {
			return bizError(CodeInternal, "schema init: %v", err)
		}
	}
	return nil
}

// LastSeq returns the last committed sequence for a shard, or 0.
func (s *Store) LastSeq(shardID string) (int64, error) {
	var next int64
	err := s.db.QueryRow(`SELECT next_seq FROM seq_cursor WHERE shard_id=?`, shardID).Scan(&next)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, bizError(CodeInternal, "seq query: %v", err)
	}
	return next - 1, nil
}

// PublishResult explains how a conditional publish attempt was resolved.
type PublishResult struct {
	Accepted bool
	Kind     string // "accepted" | "duplicate" | "stale" | "rollback" | "tampered"
	Existing *Checkpoint
}

// PublishCheckpoint conditionally inserts/advances a checkpoint. The sequence
// must advance by exactly one within a shard. Repeated, lagging, and
// cross-shard-misaligned publishes are rejected as E_STALE_CHECKPOINT; a
// whole-record replacement is classified as rollback, tamper or normal
// advance (requirement 7).
func (s *Store) PublishCheckpoint(cp Checkpoint) (*PublishResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, bizError(CodeInternal, "begin tx: %v", err)
	}
	defer tx.Rollback()

	var nextSeq int64
	err = tx.QueryRow(`SELECT next_seq FROM seq_cursor WHERE shard_id=?`, cp.ShardID).Scan(&nextSeq)
	switch err {
	case nil:
	case sql.ErrNoRows:
		nextSeq = 1
	default:
		return nil, bizError(CodeInternal, "cursor query: %v", err)
	}

	existing, err := queryCheckpoint(tx, cp.ShardID, cp.Stage, cp.ViewID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		kind := classifyReplacement(existing, &cp)
		if kind != "advance" {
			code := CodeStaleCheckpoint
			if kind == "rollback" {
				code = CodeCheckpointRollback
			}
			if kind == "tampered" {
				code = CodeCheckpointTampered
			}
			return &PublishResult{Accepted: false, Kind: kind, Existing: existing},
				&BizError{Code: code, Message: "checkpoint publish rejected: " + kind,
					ShardID: cp.ShardID, Field: cp.Stage, Details: map[string]string{
						"existing_seq": fmt.Sprintf("%d", existing.Seq),
						"incoming_seq": fmt.Sprintf("%d", cp.Seq),
					}}
		}
		// Normal advance must still follow the contiguous per-shard sequence.
		if cp.Seq != nextSeq {
			return &PublishResult{Accepted: false, Kind: "stale", Existing: existing},
				&BizError{Code: CodeStaleCheckpoint, Message: "non-contiguous checkpoint sequence",
					ShardID: cp.ShardID, Field: cp.Stage, Details: map[string]string{
						"expected_seq": fmt.Sprintf("%d", nextSeq),
						"incoming_seq": fmt.Sprintf("%d", cp.Seq),
					}}
		}
		if _, err := tx.Exec(`UPDATE checkpoints SET seq=?, input_digest=?, config_version=?,
			artifact_hash=?, artifact_path=?, strategy=?, updated_at=?
			WHERE shard_id=? AND stage=? AND view_id=?`,
			cp.Seq, cp.InputDigest, cp.ConfigVer, cp.ArtifactHash, cp.ArtifactPath,
			cp.Strategy, cp.UpdatedAt, cp.ShardID, cp.Stage, cp.ViewID); err != nil {
			return nil, bizError(CodeInternal, "update checkpoint: %v", err)
		}
	} else {
		// New row: sequence must be exactly the next contiguous one.
		if cp.Seq != nextSeq {
			return &PublishResult{Accepted: false, Kind: "stale"},
				&BizError{Code: CodeStaleCheckpoint, Message: "checkpoint sequence gap or misaligned shard publish",
					ShardID: cp.ShardID, Field: cp.Stage, Details: map[string]string{
						"expected_seq": fmt.Sprintf("%d", nextSeq),
						"incoming_seq": fmt.Sprintf("%d", cp.Seq),
					}}
		}
		if _, err := tx.Exec(`INSERT INTO checkpoints
			(seq, shard_id, stage, view_id, input_digest, config_version, artifact_hash,
			 artifact_path, strategy, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)`,
			cp.Seq, cp.ShardID, cp.Stage, cp.ViewID, cp.InputDigest, cp.ConfigVer,
			cp.ArtifactHash, cp.ArtifactPath, cp.Strategy, cp.UpdatedAt); err != nil {
			return nil, bizError(CodeInternal, "insert checkpoint: %v", err)
		}
	}

	if _, err := tx.Exec(`INSERT INTO seq_cursor(shard_id, next_seq) VALUES(?,?)
		ON CONFLICT(shard_id) DO UPDATE SET next_seq=?`,
		cp.ShardID, cp.Seq+1, cp.Seq+1); err != nil {
		return nil, bizError(CodeInternal, "cursor update: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, bizError(CodeInternal, "commit: %v", err)
	}
	return &PublishResult{Accepted: true, Kind: "accepted", Existing: existing}, nil
}

// classifyReplacement distinguishes rollback, tampering and normal advance when
// a row for the same (shard, stage, view) already exists.
func classifyReplacement(old, new *Checkpoint) string {
	switch {
	case new.Seq < old.Seq:
		return "rollback"
	case new.Seq == old.Seq:
		// Same sequence with a changed artifact hash or input digest means the
		// historical record was tampered with.
		if new.ArtifactHash != old.ArtifactHash || new.InputDigest != old.InputDigest {
			return "tampered"
		}
		return "duplicate"
	default:
		return "advance"
	}
}

func queryCheckpoint(q interface {
	QueryRow(string, ...any) *sql.Row
}, shardID, stage, viewID string) (*Checkpoint, error) {
	cp := &Checkpoint{}
	err := q.QueryRow(`SELECT seq, shard_id, stage, view_id, input_digest, config_version,
		artifact_hash, artifact_path, strategy, updated_at
		FROM checkpoints WHERE shard_id=? AND stage=? AND view_id=?`,
		shardID, stage, viewID).Scan(&cp.Seq, &cp.ShardID, &cp.Stage, &cp.ViewID,
		&cp.InputDigest, &cp.ConfigVer, &cp.ArtifactHash, &cp.ArtifactPath,
		&cp.Strategy, &cp.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, bizError(CodeInternal, "checkpoint query: %v", err)
	}
	return cp, nil
}

// GetCheckpoint loads a checkpoint row.
func (s *Store) GetCheckpoint(shardID, stage, viewID string) (*Checkpoint, error) {
	return queryCheckpoint(s.db, shardID, stage, viewID)
}

// ListCheckpoints returns all checkpoint rows ordered by shard and sequence.
func (s *Store) ListCheckpoints() ([]Checkpoint, error) {
	rows, err := s.db.Query(`SELECT seq, shard_id, stage, view_id, input_digest, config_version,
		artifact_hash, artifact_path, strategy, updated_at
		FROM checkpoints ORDER BY shard_id, seq, stage, view_id`)
	if err != nil {
		return nil, bizError(CodeInternal, "list checkpoints: %v", err)
	}
	defer rows.Close()
	var out []Checkpoint
	for rows.Next() {
		var cp Checkpoint
		if err := rows.Scan(&cp.Seq, &cp.ShardID, &cp.Stage, &cp.ViewID, &cp.InputDigest,
			&cp.ConfigVer, &cp.ArtifactHash, &cp.ArtifactPath, &cp.Strategy, &cp.UpdatedAt); err != nil {
			return nil, bizError(CodeInternal, "scan checkpoint: %v", err)
		}
		out = append(out, cp)
	}
	return out, rows.Err()
}

// UpsertJob records a job row.
func (s *Store) UpsertJob(jobID, strategy, status, cfgVer, sceneFP, createdAt, updatedAt, detail string) error {
	_, err := s.db.Exec(`INSERT INTO jobs(job_id, strategy, status, config_version, scene_fingerprint,
		created_at, updated_at, detail) VALUES(?,?,?,?,?,?,?,?)
		ON CONFLICT(job_id) DO UPDATE SET status=?, updated_at=?, detail=?`,
		jobID, strategy, status, cfgVer, sceneFP, createdAt, updatedAt, detail,
		status, updatedAt, detail)
	if err != nil {
		return bizError(CodeInternal, "upsert job: %v", err)
	}
	return nil
}

// GetJob loads a job row.
func (s *Store) GetJob(jobID string) (map[string]string, error) {
	var jobID2, strategy, status, cfgVer, sceneFP, created, updated, detail string
	err := s.db.QueryRow(`SELECT job_id, strategy, status, config_version, scene_fingerprint,
		created_at, updated_at, detail FROM jobs WHERE job_id=?`, jobID).Scan(
		&jobID2, &strategy, &status, &cfgVer, &sceneFP, &created, &updated, &detail)
	if err == sql.ErrNoRows {
		return nil, bizError(CodeNotFound, "job %s not found", jobID)
	}
	if err != nil {
		return nil, bizError(CodeInternal, "get job: %v", err)
	}
	return map[string]string{
		"job_id": jobID2, "strategy": strategy, "status": status,
		"config_version": cfgVer, "scene_fingerprint": sceneFP,
		"created_at": created, "updated_at": updated, "detail": detail,
	}, nil
}
