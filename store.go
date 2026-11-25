package speakerdiar

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Checkpoint is one committed, currently valid chunk/stage fact.
type Checkpoint struct {
	Sequence           int64  `json:"sequence"`
	ChunkSequence      int64  `json:"chunk_sequence"`
	SessionID          string `json:"session_id"`
	ChunkID            string `json:"chunk_id"`
	ChunkIndex         int    `json:"chunk_index"`
	Stage              string `json:"stage"`
	StageOrder         int    `json:"stage_order"`
	ContentFingerprint string `json:"content_fingerprint"`
	Fingerprint        string `json:"fingerprint"`
	ConfigVersion      string `json:"config_version"`
	ArtifactDigest     string `json:"artifact_digest"`
	ProducerID         string `json:"producer_id"`
}

// PublishRequest is a conditional checkpoint publication.
type PublishRequest struct {
	SessionID             string
	ProducerID            string
	ChunkID               string
	ChunkIndex            int
	Stage                 string
	ContentFingerprint    string
	Fingerprint           string
	ConfigVersion         string
	ArtifactDigest        string
	ExpectedChunkSequence int64
	InvalidateDownstream  bool
}

// MutationClass describes a checkpoint record transition.
type MutationClass string

const (
	MutationUnchanged MutationClass = "unchanged"
	MutationNormal    MutationClass = "normal_progression"
	MutationReplaced  MutationClass = "whole_replacement"
	MutationRollback  MutationClass = "rollback"
	MutationDuplicate MutationClass = "duplicate"
)

// Store is the transactional SQLite checkpoint and sequence gateway.
type Store struct {
	db       *sql.DB
	path     string
	lockPath string
}

// OpenStore initializes schema and SQLite pragmas.
func OpenStore(workDir string) (*Store, error) {
	path := filepath.Join(workDir, "state.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`
PRAGMA busy_timeout=5000;
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS global_sequence (
  id INTEGER PRIMARY KEY CHECK(id=1),
  next_value INTEGER NOT NULL
);
INSERT OR IGNORE INTO global_sequence(id, next_value) VALUES(1, 0);
CREATE TABLE IF NOT EXISTS chunks (
  chunk_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  high_water INTEGER NOT NULL DEFAULT 0,
  content_fingerprint TEXT NOT NULL DEFAULT '',
  updated_sequence INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS checkpoints (
  chunk_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  session_id TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  stage_order INTEGER NOT NULL,
  content_fingerprint TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  config_version TEXT NOT NULL,
  artifact_digest TEXT NOT NULL,
  sequence INTEGER NOT NULL UNIQUE,
  chunk_sequence INTEGER NOT NULL,
  producer_id TEXT NOT NULL,
  PRIMARY KEY(chunk_id, stage)
);
CREATE TABLE IF NOT EXISTS checkpoint_revisions (
  revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
  sequence INTEGER NOT NULL UNIQUE,
  chunk_sequence INTEGER NOT NULL,
  session_id TEXT NOT NULL,
  chunk_id TEXT NOT NULL,
  chunk_index INTEGER NOT NULL,
  stage TEXT NOT NULL,
  stage_order INTEGER NOT NULL,
  content_fingerprint TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  config_version TEXT NOT NULL,
  artifact_digest TEXT NOT NULL,
  producer_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'committed'
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_session ON checkpoints(session_id, chunk_index, stage_order);
`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db, path: path, lockPath: filepath.Join(workDir, "locks", "publish.lock")}, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// Lock acquires the workspace-wide publication file lock.
func (s *Store) Lock() (*FileLock, func() error, error) {
	return AcquireFileLock(s.lockPath)
}

// HighWater returns the committed per-chunk sequence high-water mark.
func (s *Store) HighWater(chunkID string) (int64, error) {
	var v int64
	err := s.db.QueryRow(`SELECT high_water FROM chunks WHERE chunk_id=?`, chunkID).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return v, err
}

// GetCheckpoint returns the valid checkpoint for one chunk/stage.
func (s *Store) GetCheckpoint(chunkID, stage string) (*Checkpoint, error) {
	row := s.db.QueryRow(`SELECT sequence, chunk_sequence, session_id, chunk_id, chunk_index, stage, stage_order,
		content_fingerprint, fingerprint, config_version, artifact_digest, producer_id
		FROM checkpoints WHERE chunk_id=? AND stage=?`, chunkID, stage)
	c, err := scanCheckpoint(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return c, err
}

func scanCheckpoint(scan func(dest ...any) error) (*Checkpoint, error) {
	var c Checkpoint
	err := scan(&c.Sequence, &c.ChunkSequence, &c.SessionID, &c.ChunkID, &c.ChunkIndex, &c.Stage, &c.StageOrder,
		&c.ContentFingerprint, &c.Fingerprint, &c.ConfigVersion, &c.ArtifactDigest, &c.ProducerID)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// ListCheckpoints lists valid checkpoints in session/chunk/stage order.
func (s *Store) ListCheckpoints(sessionID string) ([]Checkpoint, error) {
	rows, err := s.db.Query(`SELECT sequence, chunk_sequence, session_id, chunk_id, chunk_index, stage, stage_order,
		content_fingerprint, fingerprint, config_version, artifact_digest, producer_id
		FROM checkpoints WHERE session_id=? ORDER BY chunk_index, stage_order`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Checkpoint{}
	for rows.Next() {
		c, err := scanCheckpoint(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

// PublishCheckpoint conditionally advances the chunk sequence and commits one valid record.
func (s *Store) PublishCheckpoint(req PublishRequest) (*Checkpoint, error) {
	if req.SessionID == "" || req.ChunkID == "" || req.Stage == "" {
		return nil, newError(CodeInvalidParam, "checkpoint publication is missing identity fields")
	}
	order, ok := StageOrder[req.Stage]
	if !ok {
		return nil, newError(CodeInvalidParam, "unknown pipeline stage "+req.Stage)
	}
	reqStageOrder := order

	_, release, err := AcquireFileLock(s.lockPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = release() }()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var currentHigh int64
	err = tx.QueryRow(`SELECT high_water FROM chunks WHERE chunk_id=?`, req.ChunkID).Scan(&currentHigh)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.Exec(`INSERT INTO chunks(chunk_id,session_id,chunk_index,high_water) VALUES(?,?,?,0)`,
			req.ChunkID, req.SessionID, req.ChunkIndex); err != nil {
			return nil, err
		}
		currentHigh = 0
	} else if err != nil {
		return nil, err
	}
	if currentHigh != req.ExpectedChunkSequence {
		existing, _ := s.getCheckpointTx(tx, req.ChunkID, req.Stage)
		class := MutationNormal
		switch {
		case req.ExpectedChunkSequence < currentHigh:
			class = MutationRollback
			if existing != nil && existing.ArtifactDigest == req.ArtifactDigest && existing.Fingerprint == req.Fingerprint {
				class = MutationDuplicate
			}
		}
		de := staleError("chunk sequence is not the current contiguous predecessor", class)
		de.ChunkID = req.ChunkID
		de.Stage = req.Stage
		de.Details = map[string]any{"expected_base": req.ExpectedChunkSequence, "current_high_water": currentHigh, "request_stage_order": reqStageOrder}
		return nil, de
	}

	if reqStageOrder == 1 {
		if req.ChunkIndex > 0 {
			var previousDone int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM checkpoints WHERE session_id=? AND chunk_index=? AND stage=?`,
				req.SessionID, req.ChunkIndex-1, StageSummary).Scan(&previousDone); err != nil {
				return nil, err
			}
			if previousDone != 1 {
				de := staleError("cross-chunk publication before preceding chunk summary", "cross_chunk")
				de.ChunkID = req.ChunkID
				de.Stage = req.Stage
				de.Details = map[string]any{"chunk_index": req.ChunkIndex, "required_previous_chunk": req.ChunkIndex - 1}
				return nil, de
			}
		}
	} else {
		var prior int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM checkpoints WHERE chunk_id=? AND stage_order=?`,
			req.ChunkID, reqStageOrder-1).Scan(&prior); err != nil {
			return nil, err
		}
		if prior != 1 {
			de := staleError("stage publication is out of order within chunk", "missing_previous_stage")
			de.ChunkID = req.ChunkID
			de.Stage = req.Stage
			de.Details = map[string]any{"required_stage_order": reqStageOrder - 1}
			return nil, de
		}
	}

	if _, err := tx.Exec(`UPDATE global_sequence SET next_value=next_value+1 WHERE id=1`); err != nil {
		return nil, err
	}
	var globalSeq int64
	if err := tx.QueryRow(`SELECT next_value FROM global_sequence WHERE id=1`).Scan(&globalSeq); err != nil {
		return nil, err
	}
	newChunkSeq := currentHigh + 1

	if _, err := tx.Exec(`INSERT INTO checkpoints
		(chunk_id,stage,session_id,chunk_index,stage_order,content_fingerprint,fingerprint,config_version,artifact_digest,sequence,chunk_sequence,producer_id)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(chunk_id,stage) DO UPDATE SET
		  session_id=excluded.session_id, chunk_index=excluded.chunk_index,
		  stage_order=excluded.stage_order, content_fingerprint=excluded.content_fingerprint,
		  fingerprint=excluded.fingerprint, config_version=excluded.config_version,
		  artifact_digest=excluded.artifact_digest, sequence=excluded.sequence,
		  chunk_sequence=excluded.chunk_sequence, producer_id=excluded.producer_id`,
		req.ChunkID, req.Stage, req.SessionID, req.ChunkIndex, reqStageOrder,
		req.ContentFingerprint, req.Fingerprint, req.ConfigVersion, req.ArtifactDigest,
		globalSeq, newChunkSeq, req.ProducerID); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`INSERT INTO checkpoint_revisions
		(sequence,chunk_sequence,session_id,chunk_id,chunk_index,stage,stage_order,content_fingerprint,fingerprint,config_version,artifact_digest,producer_id,status)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?, 'committed')`,
		globalSeq, newChunkSeq, req.SessionID, req.ChunkID, req.ChunkIndex, req.Stage, reqStageOrder,
		req.ContentFingerprint, req.Fingerprint, req.ConfigVersion, req.ArtifactDigest, req.ProducerID); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`UPDATE chunks SET high_water=?, content_fingerprint=?, updated_sequence=? WHERE chunk_id=?`,
		newChunkSeq, req.ContentFingerprint, globalSeq, req.ChunkID); err != nil {
		return nil, err
	}
	if req.InvalidateDownstream {
		if _, err := tx.Exec(`DELETE FROM checkpoints WHERE chunk_id=? AND stage_order>?`, req.ChunkID, reqStageOrder); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Checkpoint{
		Sequence: globalSeq, ChunkSequence: newChunkSeq, SessionID: req.SessionID,
		ChunkID: req.ChunkID, ChunkIndex: req.ChunkIndex, Stage: req.Stage, StageOrder: reqStageOrder,
		ContentFingerprint: req.ContentFingerprint, Fingerprint: req.Fingerprint,
		ConfigVersion: req.ConfigVersion, ArtifactDigest: req.ArtifactDigest, ProducerID: req.ProducerID,
	}, nil
}

func (s *Store) getCheckpointTx(tx *sql.Tx, chunkID, stage string) (*Checkpoint, error) {
	row := tx.QueryRow(`SELECT sequence, chunk_sequence, session_id, chunk_id, chunk_index, stage, stage_order,
		content_fingerprint, fingerprint, config_version, artifact_digest, producer_id
		FROM checkpoints WHERE chunk_id=? AND stage=?`, chunkID, stage)
	return scanCheckpoint(row.Scan)
}

func staleError(msg string, class MutationClass) *Error {
	de := newError(CodeStaleCheckpoint, msg)
	de.Details = map[string]any{"mutation_class": string(class)}
	return de
}

// ClassifyRecordChange distinguishes normal progression, same-sequence replacement, and rollback.
func ClassifyRecordChange(current, candidate Checkpoint) MutationClass {
	switch {
	case candidate.Sequence == current.Sequence:
		if candidate.Fingerprint == current.Fingerprint && candidate.ArtifactDigest == current.ArtifactDigest &&
			candidate.ContentFingerprint == current.ContentFingerprint {
			return MutationUnchanged
		}
		return MutationReplaced
	case candidate.Sequence > current.Sequence:
		return MutationNormal
	default:
		return MutationRollback
	}
}

// HandleObservedRecord applies the replacement/rollback policy to an externally observed row.
// Replacement deletes that stage and downstream valid rows; rollback returns E_STALE_CHECKPOINT.
func (s *Store) HandleObservedRecord(observed Checkpoint) (MutationClass, error) {
	_, release, err := AcquireFileLock(s.lockPath)
	if err != nil {
		return "", err
	}
	defer func() { _ = release() }()
	current, err := s.GetCheckpoint(observed.ChunkID, observed.Stage)
	if err != nil {
		return "", err
	}
	if current == nil {
		return MutationNormal, nil
	}
	class := ClassifyRecordChange(*current, observed)
	switch class {
	case MutationReplaced:
		tx, err := s.db.Begin()
		if err != nil {
			return class, err
		}
		if _, err := tx.Exec(`DELETE FROM checkpoints WHERE chunk_id=? AND stage_order>=?`, observed.ChunkID, observed.StageOrder); err != nil {
			_ = tx.Rollback()
			return class, err
		}
		if err := tx.Commit(); err != nil {
			return class, err
		}
		return class, nil
	case MutationRollback:
		de := staleError("observed checkpoint sequence rolls back committed state", class)
		de.ChunkID = observed.ChunkID
		de.Stage = observed.Stage
		de.Details["current_sequence"] = current.Sequence
		de.Details["observed_sequence"] = observed.Sequence
		return class, de
	default:
		return class, nil
	}
}

// VerifySequenceContinuity ensures committed global sequences have no gaps.
func (s *Store) VerifySequenceContinuity() error {
	rows, err := s.db.Query(`SELECT sequence FROM checkpoint_revisions ORDER BY revision_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var expected int64 = 1
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return err
		}
		if seq != expected {
			return newError(CodeStaleCheckpoint, fmt.Sprintf("checkpoint sequence gap: expected %d got %d", expected, seq))
		}
		expected++
	}
	return rows.Err()
}
