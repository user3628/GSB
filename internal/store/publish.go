package store

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"time"

	"maskrevision/internal/model"
)

func (s *Store) PublicationState(shardID string) (PublicationState, bool, error) {
	var st PublicationState
	err := s.db.QueryRow(`SELECT shard_id, revision, snapshot_digest, input_fp, log_prefix_fp, last_seq, row_version, updated_at
		FROM publish_state WHERE shard_id = ?`, shardID).Scan(&st.ShardID, &st.Revision, &st.SnapshotDigest, &st.InputFP,
		&st.LogPrefixFP, &st.LastSeq, &st.RowVersion, &st.UpdatedAt)
	if err == sql.ErrNoRows {
		return PublicationState{ShardID: shardID}, false, nil
	}
	return st, err == nil, err
}

func (s *Store) SeedPublication(shardID string, revision int) error {
	cp, ok, err := s.At(shardID, revision)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("checkpoint %s@%d does not exist", shardID, revision)
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM publications WHERE shard_id = ?`, shardID); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO publications(shard_id, revision, snapshot_digest, revision_fp, input_fp, log_prefix_fp, publisher, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, shardID, cp.Revision, cp.SnapshotDigest, cp.RevisionFP, cp.InputFP, cp.LogPrefixFP, "seed", nowString()); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO publish_state(shard_id, revision, snapshot_digest, input_fp, log_prefix_fp, last_seq, row_version, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, 1, ?)
		ON CONFLICT(shard_id) DO UPDATE SET revision=excluded.revision, snapshot_digest=excluded.snapshot_digest,
		input_fp=excluded.input_fp, log_prefix_fp=excluded.log_prefix_fp, last_seq=excluded.last_seq,
		row_version=publish_state.row_version+1, updated_at=excluded.updated_at`,
		shardID, cp.Revision, cp.SnapshotDigest, cp.InputFP, cp.LogPrefixFP, cp.Seq, nowString()); err != nil {
		return err
	}
	return tx.Commit()
}

// Publish gate is serialized by both an OS-level file lock and a SQLite
// conditional update. It only accepts a contiguous next revision.
func (s *Store) Publish(lockPath string, req PublishRequest) (PublishResult, *model.BusinessError) {
	lock, err := AcquireFileLock(lockPath, 10*time.Second)
	if err != nil {
		return PublishResult{Code: "E_LOCK_TIMEOUT", Publisher: req.Publisher}, model.WithContext(
			model.NewBusinessError("E_LOCK_TIMEOUT", err.Error(), nil), req.ShardID, req.Revision)
	}
	defer lock.Close()

	tx, err := s.db.Begin()
	if err != nil {
		return PublishResult{Code: "E_STALE_CHECKPOINT", Publisher: req.Publisher}, model.WithContext(
			model.NewBusinessError("E_CHECKPOINT_TX", err.Error(), nil), req.ShardID, req.Revision)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO publish_state(shard_id, revision, snapshot_digest, input_fp, log_prefix_fp, last_seq, row_version, updated_at)
		VALUES (?, 0, '', '', '', 0, 0, ?) ON CONFLICT(shard_id) DO NOTHING`, req.ShardID, nowString()); err != nil {
		return PublishResult{Code: "E_STALE_CHECKPOINT", Publisher: req.Publisher}, model.WithContext(
			model.NewBusinessError("E_CHECKPOINT_TX", err.Error(), nil), req.ShardID, req.Revision)
	}
	var st PublicationState
	if err := tx.QueryRow(`SELECT shard_id, revision, snapshot_digest, input_fp, log_prefix_fp, last_seq, row_version, updated_at
		FROM publish_state WHERE shard_id = ?`, req.ShardID).Scan(&st.ShardID, &st.Revision, &st.SnapshotDigest,
		&st.InputFP, &st.LogPrefixFP, &st.LastSeq, &st.RowVersion, &st.UpdatedAt); err != nil {
		return PublishResult{Code: "E_STALE_CHECKPOINT", Publisher: req.Publisher}, model.WithContext(
			model.NewBusinessError("E_CHECKPOINT_TX", err.Error(), nil), req.ShardID, req.Revision)
	}
	stale := func(reason string) (PublishResult, *model.BusinessError) {
		code := "E_STALE_CHECKPOINT"
		return PublishResult{Code: code, State: st, Publisher: req.Publisher}, model.WithContext(
			model.NewBusinessError(code, reason, map[string]any{
				"current_revision": st.Revision, "expected_revision": req.ExpectedRevision,
				"requested_revision": req.Revision, "current_row_version": st.RowVersion,
			}), req.ShardID, req.Revision)
	}
	if req.ExpectedRevision != st.Revision {
		return stale("expected revision no longer matches the published head")
	}
	if req.Revision != st.Revision+1 {
		return stale("publication must advance exactly one revision")
	}
	var cpSnap, cpInput, cpLog, cpRevFP string
	err = tx.QueryRow(`SELECT snapshot_digest, input_fp, log_prefix_fp, revision_fp FROM checkpoints WHERE shard_id = ? AND revision = ?`,
		req.ShardID, req.Revision).Scan(&cpSnap, &cpInput, &cpLog, &cpRevFP)
	if err != nil {
		return stale("requested revision checkpoint is not committed for this shard")
	}
	if req.SnapshotDigest != "" && req.SnapshotDigest != cpSnap {
		return stale("snapshot digest does not match the committed checkpoint")
	}
	res, err := tx.Exec(`INSERT INTO publications(shard_id, revision, snapshot_digest, revision_fp, input_fp, log_prefix_fp, publisher, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, req.ShardID, req.Revision, cpSnap, cpRevFP, cpInput, cpLog, req.Publisher, nowString())
	if err != nil {
		return stale("revision was already published: " + err.Error())
	}
	seq, _ := res.LastInsertId()
	upd, err := tx.Exec(`UPDATE publish_state SET revision = ?, snapshot_digest = ?, input_fp = ?, log_prefix_fp = ?,
		last_seq = ?, row_version = row_version + 1, updated_at = ?
		WHERE shard_id = ? AND revision = ? AND row_version = ?`,
		req.Revision, cpSnap, cpInput, cpLog, seq, nowString(), req.ShardID, req.ExpectedRevision, st.RowVersion)
	if err != nil {
		return stale("conditional publish update failed: " + err.Error())
	}
	n, err := upd.RowsAffected()
	if err != nil || n != 1 {
		return stale("conditional publish update lost the revision race")
	}
	if err := tx.Commit(); err != nil {
		return stale("publish transaction could not commit: " + err.Error())
	}
	st.Revision = req.Revision
	st.SnapshotDigest = cpSnap
	st.InputFP = cpInput
	st.LogPrefixFP = cpLog
	st.LastSeq = seq
	st.RowVersion++
	st.UpdatedAt = nowString()
	return PublishResult{Accepted: true, Seq: seq, State: st, Code: "OK", Publisher: req.Publisher}, nil
}

func PublishLockPath(root, shardID string) string {
	return filepath.Join(root, "locks", shardID+".publish.lock")
}
