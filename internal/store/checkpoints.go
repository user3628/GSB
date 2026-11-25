package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"ropechunk/internal/model"
)

type CheckpointRecord struct {
	model.Checkpoint
	ResultPayload []byte `json:"result_payload,omitempty"`
}

type CheckpointKey struct {
	Batch    int
	Head     int
	RowBlock int
}

func (s *Store) PublishCheckpoint(ctx context.Context, req model.PublishRequest) (int64, error) {
	if req.Stage != model.StageRotation && req.Stage != model.StageScore {
		return 0, &model.AppError{Code: model.CodeInvalidConfig, Message: "invalid stage"}
	}
	if req.AssignedShardID != "" && req.AssignedShardID != req.ShardID {
		return 0, staleError(req, "cross-shard checkpoint publication")
	}
	if req.AssignedShardID != "" && req.AssignedRowBlock != req.RowBlock {
		return 0, staleError(req, "cross-row-block checkpoint publication")
	}
	var sequence int64
	err := s.withCheckpointLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var currentSeq int64
		var currentVersion, currentInput string
		var complete int
		err = tx.QueryRowContext(ctx, `SELECT sequence, version_fingerprint, input_fingerprint, complete
			FROM checkpoints WHERE shard_id=? AND stage=? AND batch=? AND head=? AND row_block=?`,
			req.ShardID, req.Stage, req.Batch, req.Head, req.RowBlock).Scan(&currentSeq, &currentVersion, &currentInput, &complete)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if req.ExpectedSequence != 1 {
				return staleError(req, fmt.Sprintf("new checkpoint expects sequence 1, got %d", req.ExpectedSequence))
			}
			partialJSON, err := json.Marshal(nonNilFloats(req.PartialOutput))
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO checkpoints(
				shard_id,stage,batch,head,row_block,row_start,row_end,covered_key_start,covered_key_end,
				running_max,exp_sum,lse,partial_output,partial_digest,input_fingerprint,version_fingerprint,
				sequence,complete,config_version,run_id,result_payload,updated_at)
				VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
				req.ShardID, req.Stage, req.Batch, req.Head, req.RowBlock, req.RowStart, req.RowEnd,
				req.CoveredKeyStart, req.CoveredKeyEnd, req.RunningMax, req.ExpSum, req.LSE,
				string(partialJSON), req.PartialDigest, req.InputFingerprint, req.VersionFingerprint,
				1, boolInt(req.Complete), req.ConfigVersion, req.RunID, []byte(req.ResultPayload), timestamp(req.UpdatedAt))
			if err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "constraint") {
					return staleError(req, "duplicate checkpoint publication")
				}
				return err
			}
			sequence = 1
		case err != nil:
			return err
		default:
			if req.ExpectedSequence != currentSeq+1 {
				return staleError(req, fmt.Sprintf("expected sequence %d, current sequence %d", req.ExpectedSequence, currentSeq))
			}
			if currentVersion != req.VersionFingerprint {
				return staleError(req, "version fingerprint changed without coordinated invalidation")
			}
			if currentInput != req.InputFingerprint {
				return staleError(req, "input fingerprint mismatch")
			}
			if complete != 0 {
				return staleError(req, "checkpoint is already complete")
			}
			partialJSON, err := json.Marshal(nonNilFloats(req.PartialOutput))
			if err != nil {
				return err
			}
			res, err := tx.ExecContext(ctx, `UPDATE checkpoints SET
				row_start=?,row_end=?,covered_key_start=?,covered_key_end=?,running_max=?,exp_sum=?,lse=?,
				partial_output=?,partial_digest=?,input_fingerprint=?,version_fingerprint=?,sequence=?,
				complete=?,config_version=?,run_id=?,result_payload=?,updated_at=?
				WHERE shard_id=? AND stage=? AND batch=? AND head=? AND row_block=?
				AND sequence=? AND version_fingerprint=? AND input_fingerprint=?`,
				req.RowStart, req.RowEnd, req.CoveredKeyStart, req.CoveredKeyEnd, req.RunningMax, req.ExpSum, req.LSE,
				string(partialJSON), req.PartialDigest, req.InputFingerprint, req.VersionFingerprint, currentSeq+1,
				boolInt(req.Complete), req.ConfigVersion, req.RunID, []byte(req.ResultPayload), timestamp(req.UpdatedAt),
				req.ShardID, req.Stage, req.Batch, req.Head, req.RowBlock,
				currentSeq, currentVersion, currentInput)
			if err != nil {
				return err
			}
			affected, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if affected != 1 {
				return staleError(req, "conditional checkpoint update lost the race")
			}
			sequence = currentSeq + 1
		}
		return tx.Commit()
	})
	return sequence, err
}

func (s *Store) GetCheckpoint(ctx context.Context, shardID, stage string, batch, head, rowBlock int) (*CheckpointRecord, error) {
	row := s.db.QueryRowContext(ctx, checkpointSelect+` WHERE shard_id=? AND stage=? AND batch=? AND head=? AND row_block=?`,
		shardID, stage, batch, head, rowBlock)
	rec, err := scanCheckpoint(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &model.AppError{Code: model.CodeNotFound, Message: "checkpoint not found"}
	}
	return rec, err
}

func (s *Store) ListCheckpoints(ctx context.Context, shardID, stage string) ([]CheckpointRecord, error) {
	rows, err := s.db.QueryContext(ctx, checkpointSelect+` WHERE shard_id=? AND stage=? ORDER BY batch,head,row_block`, shardID, stage)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CheckpointRecord
	for rows.Next() {
		rec, err := scanCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rec)
	}
	return out, rows.Err()
}

func (s *Store) ListAllCheckpoints(ctx context.Context) ([]CheckpointRecord, error) {
	rows, err := s.db.QueryContext(ctx, checkpointSelect+` ORDER BY shard_id,stage,batch,head,row_block`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CheckpointRecord
	for rows.Next() {
		rec, err := scanCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rec)
	}
	return out, rows.Err()
}

func (s *Store) InvalidateCheckpoints(ctx context.Context, shardID, stage string, keys []CheckpointKey, event model.InvalidationEvent) (model.InvalidationEvent, error) {
	err := s.withCheckpointLock(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, key := range keys {
			res, err := tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE shard_id=? AND stage=? AND batch=? AND head=? AND row_block=?`,
				shardID, stage, key.Batch, key.Head, key.RowBlock)
			if err != nil {
				return err
			}
			affected, err := res.RowsAffected()
			if err != nil {
				return err
			}
			event.AffectedRows += int(affected)
		}
		event.ShardID = shardID
		event.Stage = stage
		event.CreatedAt = timestamp(event.CreatedAt)
		res, err := tx.ExecContext(ctx, `INSERT INTO invalidations(config_version,stage,shard_id,earliest_row_block,
			col_block_start,col_block_end,reason,affected_rows,reused_rows,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
			event.ConfigVersion, event.Stage, event.ShardID, event.EarliestRowBlock, event.ColBlockStart,
			event.ColBlockEnd, event.Reason, event.AffectedRows, event.ReusedRows, event.CreatedAt)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		event.ID = id
		return tx.Commit()
	})
	return event, err
}

func (s *Store) ListInvalidations(ctx context.Context, shardID string) ([]model.InvalidationEvent, error) {
	query := `SELECT id,config_version,stage,shard_id,earliest_row_block,col_block_start,col_block_end,
		reason,affected_rows,reused_rows,created_at FROM invalidations`
	args := []any{}
	if shardID != "" {
		query += ` WHERE shard_id=?`
		args = append(args, shardID)
	}
	query += ` ORDER BY id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.InvalidationEvent
	for rows.Next() {
		var e model.InvalidationEvent
		if err := rows.Scan(&e.ID, &e.ConfigVersion, &e.Stage, &e.ShardID, &e.EarliestRowBlock,
			&e.ColBlockStart, &e.ColBlockEnd, &e.Reason, &e.AffectedRows, &e.ReusedRows, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

const checkpointSelect = `SELECT shard_id,stage,batch,head,row_block,row_start,row_end,covered_key_start,
	covered_key_end,running_max,exp_sum,lse,partial_output,partial_digest,input_fingerprint,version_fingerprint,
	sequence,complete,config_version,run_id,updated_at,result_payload FROM checkpoints`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanCheckpoint(scanner rowScanner) (*CheckpointRecord, error) {
	var rec CheckpointRecord
	var partialJSON string
	var complete int
	if err := scanner.Scan(&rec.ShardID, &rec.Stage, &rec.Batch, &rec.Head, &rec.RowBlock,
		&rec.RowStart, &rec.RowEnd, &rec.CoveredKeyStart, &rec.CoveredKeyEnd, &rec.RunningMax,
		&rec.ExpSum, &rec.LSE, &partialJSON, &rec.PartialDigest, &rec.InputFingerprint,
		&rec.VersionFingerprint, &rec.Sequence, &complete, &rec.ConfigVersion, &rec.RunID,
		&rec.UpdatedAt, &rec.ResultPayload); err != nil {
		return nil, err
	}
	rec.Complete = complete != 0
	if partialJSON != "" {
		if err := json.Unmarshal([]byte(partialJSON), &rec.PartialOutput); err != nil {
			return nil, err
		}
	}
	return &rec, nil
}

func (s *Store) withCheckpointLock(ctx context.Context, fn func() error) error {
	f, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
	defer unix.Flock(int(f.Fd()), unix.LOCK_UN)
	return fn()
}

func staleError(req model.PublishRequest, detail string) *model.AppError {
	return &model.AppError{
		Code:     model.CodeStaleCheckpoint,
		ShardID:  req.ShardID,
		Stage:    req.Stage,
		Batch:    req.Batch,
		Head:     req.Head,
		RowBlock: req.RowBlock,
		Message:  detail,
		Details: map[string]any{
			"expected_sequence": req.ExpectedSequence,
			"assigned_shard":    req.AssignedShardID,
			"assigned_row":      req.AssignedRowBlock,
		},
	}
}

func timestamp(value string) string {
	if value != "" {
		return value
	}
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func nonNilFloats(values []float64) []float64 {
	if values == nil {
		return []float64{}
	}
	return values
}
