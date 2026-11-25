package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"example.com/gsb-router/internal/model"
)

func fingerprint(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func newIntentID() (string, error) {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return "intent-" + hex.EncodeToString(raw[:]), nil
}

func (s *Store) persistIntent(ctx context.Context, commitID, kind string, baseVersion, targetVersion int64, payload any) (string, error) {
	id, err := newIntentID()
	if err != nil {
		return "", err
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	now := nowString(s.now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO intents(id, commit_id, kind, base_version, target_version, phase, status, payload_json, created_at) VALUES(?,?,?,?,?,?,?,?,?)`,
		id, commitID, kind, baseVersion, targetVersion, "intent_persisted", "pending", string(payloadJSON), now)
	if err != nil {
		_ = tx.Rollback()
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return id, nil
}

func (s *Store) markIntent(ctx context.Context, id, status, phase string) {
	markCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = s.db.ExecContext(markCtx, `UPDATE intents SET status=?, phase=?, finished_at=? WHERE id=? AND status='pending'`, status, phase, nowString(s.now()), id)
}

func (s *Store) existingCommit(ctx context.Context, commitID, contentHash string) (*CommitResult, error) {
	var storedHash, response string
	err := s.db.QueryRowContext(ctx, `SELECT content_hash, response_json FROM commits WHERE commit_id=?`, commitID).Scan(&storedHash, &response)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if storedHash != contentHash {
		return nil, conflicting(commitID, storedHash, contentHash)
	}
	var result CommitResult
	if err := json.Unmarshal([]byte(response), &result); err != nil {
		return nil, fmt.Errorf("decode commit receipt: %w", err)
	}
	result.Reused = true
	return &result, nil
}

func conflicting(commitID, storedHash, incomingHash string) error {
	return errsConflict(commitID, storedHash, incomingHash)
}

func persistSnapshotTx(ctx context.Context, tx *sql.Tx, snapshot model.Snapshot, createdAt string) error {
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(version, generated_at, checksum, snapshot_json, created_at) VALUES(?,?,?,?,?)`,
		snapshot.Version, snapshot.GeneratedAt, snapshot.Checksum, string(raw), createdAt); err != nil {
		return err
	}
	for _, table := range snapshot.LogicalTables {
		algorithmJSON, err := json.Marshal(table.Algorithm)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO logical_tables(version, table_id, logical_db, shard_key, shard_key_type, algorithm_json, primary_key) VALUES(?,?,?,?,?,?,?)`,
			snapshot.Version, table.ID, table.LogicalDB, table.ShardKey, table.ShardKeyType, string(algorithmJSON), table.PrimaryKey); err != nil {
			return err
		}
		for _, shard := range table.Ranges {
			if _, err := tx.ExecContext(ctx, `INSERT INTO shard_ranges(version, table_id, range_id, lower_bound, upper_bound, lower_inclusive, upper_inclusive, physical_table, weight) VALUES(?,?,?,?,?,?,?,?,?)`,
				snapshot.Version, table.ID, shard.ID, shard.Lower, shard.Upper, boolInt(shard.LowerInclusive), boolInt(shard.UpperInclusive), shard.PhysicalTable, shard.Weight); err != nil {
				return err
			}
		}
	}
	return nil
}

func insertCommitTx(ctx context.Context, tx *sql.Tx, result CommitResult, contentHash string) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO commits(commit_id, content_hash, kind, status, version, response_json, created_at) VALUES(?,?,?,?,?,?,?)`,
		result.CommitID, contentHash, result.Kind, "success", result.Version, string(raw), result.CommittedAt)
	return err
}

func markIntentCommittedTx(ctx context.Context, tx *sql.Tx, id, finishedAt string) error {
	_, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', phase='committed', finished_at=? WHERE id=? AND status='pending'`, finishedAt, id)
	return err
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
