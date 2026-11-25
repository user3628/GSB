package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"voiceline/internal/model"
)

type Store struct {
	db       *sql.DB
	lockPath string
}

type Checkpoint struct {
	SegmentID        string
	Stage            string
	InputFingerprint string
	PolicyVersion    int64
	ArtifactDigest   string
	ArtifactBlob     []byte
	MonotonicSeq     int64
	Revision         int
	UpdatedAt        string
}

type BlockPublication struct {
	SegmentID     string
	BlockIndex    int
	WorkerID      string
	PolicyVersion int64
	PCM           []byte
	Digest        string
	FrameDigest   string
	ClampCount    int
}

type BlockArtifact struct {
	BlockPublication
	CreatedAt string
}

type AssignedPublication struct {
	GlobalSeq int64
	BlockPublication
}

func Open(ctx context.Context, path string) (*Store, error) {
	if path != ":memory:" {
		if dir := filepath.Dir(path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
		}
	}
	dsn := path
	if path != ":memory:" {
		dsn = path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &Store{db: db, lockPath: path + ".flock"}
	if path == ":memory:" {
		store.lockPath = ":memory:.flock"
	}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	schema := `
CREATE TABLE IF NOT EXISTS global_sequence (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	next_seq INTEGER NOT NULL
);
INSERT INTO global_sequence(id, next_seq) VALUES (1, 0) ON CONFLICT(id) DO NOTHING;

CREATE TABLE IF NOT EXISTS stage_checkpoint (
	segment_id TEXT NOT NULL,
	stage TEXT NOT NULL,
	monotonic_seq INTEGER NOT NULL,
	input_fingerprint TEXT NOT NULL,
	policy_version INTEGER NOT NULL,
	artifact_digest TEXT NOT NULL,
	artifact_blob BLOB,
	revision INTEGER NOT NULL DEFAULT 1,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	PRIMARY KEY(segment_id, stage)
);

CREATE TABLE IF NOT EXISTS block_cursor (
	segment_id TEXT PRIMARY KEY,
	next_block_index INTEGER NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS block_artifact (
	segment_id TEXT NOT NULL,
	block_index INTEGER NOT NULL,
	policy_version INTEGER NOT NULL,
	worker_id TEXT NOT NULL,
	pcm BLOB NOT NULL,
	digest TEXT NOT NULL,
	frame_digest TEXT NOT NULL,
	clamp_count INTEGER NOT NULL,
	created_at TEXT NOT NULL,
	PRIMARY KEY(segment_id, block_index)
);

CREATE TABLE IF NOT EXISTS block_assignment (
	global_seq INTEGER PRIMARY KEY,
	segment_id TEXT NOT NULL,
	block_index INTEGER NOT NULL,
	status TEXT NOT NULL DEFAULT 'assigned',
	worker_id TEXT,
	digest TEXT,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	UNIQUE(segment_id, block_index)
);
`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *Store) nextSequence(ctx context.Context, tx *sql.Tx) (int64, error) {
	if _, err := tx.ExecContext(ctx, `UPDATE global_sequence SET next_seq = next_seq + 1 WHERE id = 1`); err != nil {
		return 0, err
	}
	var seq int64
	if err := tx.QueryRowContext(ctx, `SELECT next_seq FROM global_sequence WHERE id = 1`).Scan(&seq); err != nil {
		return 0, err
	}
	return seq, nil
}

func (s *Store) PutCheckpoint(ctx context.Context, cp Checkpoint) (*Checkpoint, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	seq, err := s.nextSequence(ctx, tx)
	if err != nil {
		return nil, err
	}
	var revision int
	err = tx.QueryRowContext(ctx, `
INSERT INTO stage_checkpoint(
	segment_id, stage, monotonic_seq, input_fingerprint, policy_version,
	artifact_digest, artifact_blob, revision, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
ON CONFLICT(segment_id, stage) DO UPDATE SET
	monotonic_seq = excluded.monotonic_seq,
	input_fingerprint = excluded.input_fingerprint,
	policy_version = excluded.policy_version,
	artifact_digest = excluded.artifact_digest,
	artifact_blob = excluded.artifact_blob,
	revision = stage_checkpoint.revision + 1,
	updated_at = excluded.updated_at
RETURNING monotonic_seq, revision`,
		cp.SegmentID, cp.Stage, seq, cp.InputFingerprint, cp.PolicyVersion,
		cp.ArtifactDigest, cp.ArtifactBlob, now, now,
	).Scan(&seq, &revision)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	cp.MonotonicSeq = seq
	cp.Revision = revision
	cp.UpdatedAt = now
	return &cp, nil
}

func (s *Store) GetCheckpoint(ctx context.Context, segmentID, stage string) (*Checkpoint, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT segment_id, stage, monotonic_seq, input_fingerprint, policy_version,
       artifact_digest, artifact_blob, revision, updated_at
FROM stage_checkpoint WHERE segment_id = ? AND stage = ?`, segmentID, stage)
	var cp Checkpoint
	var blob []byte
	err := row.Scan(&cp.SegmentID, &cp.Stage, &cp.MonotonicSeq, &cp.InputFingerprint,
		&cp.PolicyVersion, &cp.ArtifactDigest, &blob, &cp.Revision, &cp.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cp.ArtifactBlob = blob
	return &cp, nil
}

func (s *Store) ListCheckpoints(ctx context.Context, segmentID string) ([]Checkpoint, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT segment_id, stage, monotonic_seq, input_fingerprint, policy_version,
       artifact_digest, revision, updated_at
FROM stage_checkpoint WHERE segment_id = ? ORDER BY monotonic_seq`, segmentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Checkpoint, 0)
	for rows.Next() {
		var cp Checkpoint
		if err := rows.Scan(&cp.SegmentID, &cp.Stage, &cp.MonotonicSeq, &cp.InputFingerprint,
			&cp.PolicyVersion, &cp.ArtifactDigest, &cp.Revision, &cp.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, cp)
	}
	return out, rows.Err()
}

func (s *Store) EnsureShard(ctx context.Context, segmentID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO block_cursor(segment_id, next_block_index, updated_at)
		 VALUES(?, 0, ?) ON CONFLICT(segment_id) DO NOTHING`, segmentID, now)
	return err
}

func (s *Store) NextBlockIndex(ctx context.Context, segmentID string) (int, error) {
	if err := s.EnsureShard(ctx, segmentID); err != nil {
		return 0, err
	}
	var next int
	err := s.db.QueryRowContext(ctx, `SELECT next_block_index FROM block_cursor WHERE segment_id=?`, segmentID).Scan(&next)
	return next, err
}

// PublishBlock uses both an OS file lock and a conditional SQLite cursor update.
// Only block_index == current next_block_index is accepted.
func (s *Store) PublishBlock(ctx context.Context, pub BlockPublication) (*BlockArtifact, error) {
	lock, err := AcquireFileLock(s.lockPath)
	if err != nil {
		return nil, fmt.Errorf("acquire publication lock: %w", err)
	}
	defer lock.Unlock()

	if err := s.EnsureShard(ctx, pub.SegmentID); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var next int
	if err := tx.QueryRowContext(ctx, `SELECT next_block_index FROM block_cursor WHERE segment_id=?`, pub.SegmentID).Scan(&next); err != nil {
		return nil, err
	}
	if pub.BlockIndex != next {
		var existingDigest string
		exists := tx.QueryRowContext(ctx, `SELECT digest FROM block_artifact WHERE segment_id=? AND block_index=?`,
			pub.SegmentID, pub.BlockIndex).Scan(&existingDigest)
		reason := "out_of_order"
		if exists == nil {
			reason = "duplicate"
		} else if pub.BlockIndex < next {
			reason = "stale"
		}
		return nil, staleCheckpointError(pub.SegmentID, pub.BlockIndex, next, reason, nil)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := tx.ExecContext(ctx,
		`UPDATE block_cursor SET next_block_index=?, updated_at=?
		 WHERE segment_id=? AND next_block_index=?`,
		pub.BlockIndex+1, now, pub.SegmentID, pub.BlockIndex)
	if err != nil {
		return nil, err
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		var current int
		_ = tx.QueryRowContext(ctx, `SELECT next_block_index FROM block_cursor WHERE segment_id=?`, pub.SegmentID).Scan(&current)
		return nil, staleCheckpointError(pub.SegmentID, pub.BlockIndex, current, "conditional_update_failed", nil)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO block_artifact(
	segment_id, block_index, policy_version, worker_id, pcm, digest,
	frame_digest, clamp_count, created_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		pub.SegmentID, pub.BlockIndex, pub.PolicyVersion, pub.WorkerID, pub.PCM,
		pub.Digest, pub.FrameDigest, pub.ClampCount, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &BlockArtifact{BlockPublication: pub, CreatedAt: now}, nil
}

func (s *Store) RegisterAssignment(ctx context.Context, globalSeq int64, segmentID string, blockIndex int) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO block_assignment(global_seq, segment_id, block_index, status, created_at, updated_at)
VALUES(?, ?, ?, 'assigned', ?, ?)
ON CONFLICT(global_seq) DO NOTHING`, globalSeq, segmentID, blockIndex, now, now)
	return err
}

func (s *Store) PublishAssignedBlock(ctx context.Context, pub AssignedPublication) (*BlockArtifact, error) {
	lock, err := AcquireFileLock(s.lockPath)
	if err != nil {
		return nil, err
	}
	defer lock.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var assignedSegment string
	var assignedBlock int
	var status string
	err = tx.QueryRowContext(ctx,
		`SELECT segment_id, block_index, status FROM block_assignment WHERE global_seq=?`,
		pub.GlobalSeq).Scan(&assignedSegment, &assignedBlock, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, staleCheckpointError(pub.SegmentID, pub.BlockIndex, -1, "unknown_global_sequence", map[string]any{"global_seq": pub.GlobalSeq})
	}
	if err != nil {
		return nil, err
	}
	if assignedSegment != pub.SegmentID {
		return nil, staleCheckpointError(pub.SegmentID, pub.BlockIndex, assignedBlock, "cross_shard_misplacement", map[string]any{
			"global_seq":        pub.GlobalSeq,
			"assigned_segment":  assignedSegment,
			"published_segment": pub.SegmentID,
		})
	}
	if assignedBlock != pub.BlockIndex {
		return nil, staleCheckpointError(pub.SegmentID, pub.BlockIndex, assignedBlock, "frame_block_sequence_mismatch", map[string]any{"global_seq": pub.GlobalSeq})
	}
	if status == "published" {
		return nil, staleCheckpointError(pub.SegmentID, pub.BlockIndex, assignedBlock+1, "duplicate_assignment", map[string]any{"global_seq": pub.GlobalSeq})
	}

	var next int
	if err := tx.QueryRowContext(ctx, `SELECT next_block_index FROM block_cursor WHERE segment_id=?`, pub.SegmentID).Scan(&next); err != nil {
		return nil, err
	}
	if pub.BlockIndex != next {
		return nil, staleCheckpointError(pub.SegmentID, pub.BlockIndex, next, "non_contiguous_block", map[string]any{"global_seq": pub.GlobalSeq})
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := tx.ExecContext(ctx,
		`UPDATE block_assignment SET status='published', worker_id=?, digest=?, updated_at=?
		 WHERE global_seq=? AND status='assigned'`,
		pub.WorkerID, pub.Digest, now, pub.GlobalSeq)
	if err != nil {
		return nil, err
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return nil, staleCheckpointError(pub.SegmentID, pub.BlockIndex, next, "assignment_already_changed", nil)
	}
	res, err = tx.ExecContext(ctx,
		`UPDATE block_cursor SET next_block_index=?, updated_at=? WHERE segment_id=? AND next_block_index=?`,
		pub.BlockIndex+1, now, pub.SegmentID, pub.BlockIndex)
	if err != nil {
		return nil, err
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return nil, staleCheckpointError(pub.SegmentID, pub.BlockIndex, next, "conditional_cursor_update_failed", nil)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO block_artifact(
	segment_id, block_index, policy_version, worker_id, pcm, digest,
	frame_digest, clamp_count, created_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		pub.SegmentID, pub.BlockIndex, pub.PolicyVersion, pub.WorkerID, pub.PCM,
		pub.Digest, pub.FrameDigest, pub.ClampCount, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &BlockArtifact{BlockPublication: pub.BlockPublication, CreatedAt: now}, nil
}

func (s *Store) GetBlockArtifact(ctx context.Context, segmentID string, blockIndex int) (*BlockArtifact, error) {
	var artifact BlockArtifact
	err := s.db.QueryRowContext(ctx, `
SELECT segment_id, block_index, policy_version, worker_id, pcm, digest,
       frame_digest, clamp_count, created_at
FROM block_artifact WHERE segment_id=? AND block_index=?`, segmentID, blockIndex).Scan(
		&artifact.SegmentID, &artifact.BlockIndex, &artifact.PolicyVersion, &artifact.WorkerID,
		&artifact.PCM, &artifact.Digest, &artifact.FrameDigest, &artifact.ClampCount, &artifact.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &artifact, nil
}

func (s *Store) ListBlockArtifacts(ctx context.Context, segmentID string) ([]BlockArtifact, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT segment_id, block_index, policy_version, worker_id, pcm, digest,
       frame_digest, clamp_count, created_at
FROM block_artifact WHERE segment_id=? ORDER BY block_index`, segmentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]BlockArtifact, 0)
	for rows.Next() {
		var artifact BlockArtifact
		if err := rows.Scan(&artifact.SegmentID, &artifact.BlockIndex, &artifact.PolicyVersion,
			&artifact.WorkerID, &artifact.PCM, &artifact.Digest, &artifact.FrameDigest,
			&artifact.ClampCount, &artifact.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, artifact)
	}
	return out, rows.Err()
}

func staleCheckpointError(segmentID string, blockIndex, expected int, reason string, extra map[string]any) error {
	frame := blockIndex
	details := map[string]any{
		"reason":         reason,
		"expected_block": expected,
		"actual_block":   blockIndex,
	}
	for k, v := range extra {
		details[k] = v
	}
	return &model.BusinessError{
		Code:       model.CodeStaleCheckpoint,
		SegmentID:  segmentID,
		FrameIndex: &frame,
		Message:    "frame block publication is stale, duplicate, out of order, or cross-shard",
		Details:    details,
	}
}
