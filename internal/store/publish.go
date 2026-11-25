package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"flashattention/internal/attn"
	"flashattention/internal/errs"
	"flashattention/internal/tensor"
	"flashattention/internal/versioning"
)

type Head struct {
	Seq          int64
	CheckpointID int64
	VersionFP    string
	StateHash    string
	Valid        bool
}

func (s *Store) Publish(ctx context.Context, c Checkpoint) (Record, error) {
	lock, err := s.lock(ctx)
	if err != nil {
		return Record{}, err
	}
	defer lock.Release()

	mBlob, sumBlob, lseBlob, accBlob, rows, dim := blobsFromSnapshots(c.Snapshots)
	if rows == 0 || dim == 0 || len(accBlob) != rows*dim*8 {
		return Record{}, errs.New(errs.EState, "incomplete checkpoint snapshots at bh=%d rb=%d", c.BH, c.RowBlock)
	}
	c.RowsCount, c.HeadDim = rows, dim
	c.AccHash = AccHash(accBlob)
	c.StateHash = HashCheckpoint(&c, mBlob, sumBlob, lseBlob, accBlob)
	eventsJSON, err := json.Marshal(c.Events)
	if err != nil {
		return Record{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, err
	}
	defer tx.Rollback()

	head, err := getHead(ctx, tx, c.ShardID, c.BH, c.RowBlock)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Record{}, err
	}
	noHead := errors.Is(err, sql.ErrNoRows)

	if noHead {
		if c.Seq != 1 {
			return Record{}, stale(c.ShardID, c.BH, c.RowBlock, 0, c.Seq, "gap_without_head", "first checkpoint must have seq=1")
		}
	} else {
		switch {
		case c.Seq < head.Seq:
			return Record{}, stale(c.ShardID, c.BH, c.RowBlock, head.Seq, c.Seq, "sequence_rollback", "publish sequence is behind current effective head")
		case c.Seq == head.Seq:
			kind := "duplicate_sequence"
			if c.StateHash != head.StateHash || c.VersionFP != head.VersionFP {
				kind = "overall_replacement_or_digest_mismatch"
			}
			return Record{}, stale(c.ShardID, c.BH, c.RowBlock, head.Seq, c.Seq, kind, "same sequence cannot replace effective checkpoint")
		case c.Seq != head.Seq+1:
			return Record{}, stale(c.ShardID, c.BH, c.RowBlock, head.Seq, c.Seq, "sequence_gap", "sequence must advance by exactly one")
		}
		if head.Valid && c.PrevHash != "" && c.PrevHash != head.StateHash {
			return Record{}, stale(c.ShardID, c.BH, c.RowBlock, head.Seq, c.Seq, "prev_digest_mismatch", "PrevHash does not match current effective state")
		}
		if c.VersionFP != head.VersionFP {
			ok, err := hasInvalidation(ctx, tx, c.ShardID, head.VersionFP, c.VersionFP, c.RowBlock)
			if err != nil {
				return Record{}, err
			}
			if !ok {
				return Record{}, stale(c.ShardID, c.BH, c.RowBlock, head.Seq, c.Seq, "unregistered_fingerprint_change", "version fingerprint changed without registered block invalidation")
			}
		}

		if head.Valid {
			res, err := tx.ExecContext(ctx,
				`UPDATE checkpoints SET active=0 WHERE id=? AND seq=? AND state_hash=? AND active=1`,
				head.CheckpointID, head.Seq, head.StateHash)
			if err != nil {
				return Record{}, err
			}
			n, _ := res.RowsAffected()
			if n != 1 {
				return Record{}, stale(c.ShardID, c.BH, c.RowBlock, head.Seq, c.Seq, "conditional_update_failed", "current checkpoint was replaced or deactivated")
			}
		}
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE checkpoints SET active=0
WHERE shard_id=? AND bh=? AND row_block=? AND col_start=? AND col_end=? AND active=1`,
		c.ShardID, c.BH, c.RowBlock, c.ColStart, c.ColEnd); err != nil {
		return Record{}, err
	}

	res, err := tx.ExecContext(ctx, `
INSERT INTO checkpoints(
 shard_id,bh,row_block,col_start,col_end,rows_count,head_dim,seq,strategy,
 row_max_blob,exp_sum_blob,lse_blob,acc_blob,acc_hash,input_fp,version_fp,prev_hash,state_hash,events_json,active)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)`,
		c.ShardID, c.BH, c.RowBlock, c.ColStart, c.ColEnd, rows, dim, c.Seq, c.Strategy,
		mBlob, sumBlob, lseBlob, accBlob, c.AccHash, c.InputFP, c.VersionFP, c.PrevHash, c.StateHash, string(eventsJSON))
	if err != nil {
		return Record{}, errs.New(errs.EStaleCheckpoint, "insert checkpoint failed: %v", err).
			With("shard", c.ShardID).With("bh", c.BH).With("row_block", c.RowBlock)
	}
	id, _ := res.LastInsertId()

	if noHead {
		_, err = tx.ExecContext(ctx, `
INSERT INTO row_heads(shard_id,bh,row_block,current_seq,checkpoint_id,version_fp,state_hash,valid)
VALUES(?,?,?,?,?,?,?,1)`, c.ShardID, c.BH, c.RowBlock, c.Seq, id, c.VersionFP, c.StateHash)
	} else {
		res, err = tx.ExecContext(ctx, `
UPDATE row_heads SET current_seq=?, checkpoint_id=?, version_fp=?, state_hash=?, valid=1
WHERE shard_id=? AND bh=? AND row_block=? AND current_seq=?`,
			c.Seq, id, c.VersionFP, c.StateHash, c.ShardID, c.BH, c.RowBlock, head.Seq)
		if err == nil {
			n, _ := res.RowsAffected()
			if n != 1 {
				err = errs.New(errs.EStaleCheckpoint, "conditional row head update affected %d rows", n)
			}
		}
	}
	if err != nil {
		return Record{}, err
	}

	for _, contrib := range c.Contributions {
		if err := upsertContributionTx(ctx, tx, c.ShardID, c.VersionFP, contrib); err != nil {
			return Record{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Record{}, err
	}

	return Record{
		ID: id, ShardID: c.ShardID, BH: c.BH, RowBlock: c.RowBlock,
		ColStart: c.ColStart, ColEnd: c.ColEnd, RowsCount: rows, HeadDim: dim,
		Seq: c.Seq, Strategy: c.Strategy, InputFP: c.InputFP, VersionFP: c.VersionFP,
		StateHash: c.StateHash, AccHash: c.AccHash, Active: true,
	}, nil
}

func getHead(ctx context.Context, q querier, shard string, bh, rb int) (Head, error) {
	var h Head
	var valid int
	err := q.QueryRowContext(ctx,
		`SELECT current_seq,checkpoint_id,version_fp,state_hash,valid FROM row_heads WHERE shard_id=? AND bh=? AND row_block=?`,
		shard, bh, rb).Scan(&h.Seq, &h.CheckpointID, &h.VersionFP, &h.StateHash, &valid)
	h.Valid = valid == 1
	return h, err
}

type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func stale(shard string, bh, rb int, current, attempted int64, kind, msg string) *errs.Error {
	return errs.New(errs.EStaleCheckpoint, "%s: %s", kind, msg).
		With("shard", shard).With("bh", bh).With("row_block", rb).
		With("current_seq", current).With("attempted_seq", attempted).
		With("kind", kind)
}

func hasInvalidation(ctx context.Context, q querier, shard, oldFP, newFP string, rb int) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx, `
SELECT COUNT(1) FROM invalidations
WHERE shard_id=? AND old_fp=? AND new_fp=? AND ? BETWEEN first_row_block AND last_row_block`,
		shard, oldFP, newFP, rb).Scan(&n)
	return n > 0, err
}

func (s *Store) ApplyInvalidation(ctx context.Context, r versioning.Range) error {
	lock, err := s.lock(ctx)
	if err != nil {
		return err
	}
	defer lock.Release()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	allRows := 0
	if r.AllRowBlocks {
		allRows = 1
	}
	allCols := 0
	if r.AllColBlocks {
		allCols = 1
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO invalidations(shard_id,trigger,old_fp,new_fp,first_row_block,last_row_block,row_start,row_end,
 first_col_block,last_col_block,col_start,col_end,all_rows,all_cols,reason)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		r.ShardID, r.Trigger, r.OldFingerprint, r.NewFingerprint, r.FirstRowBlock, r.LastRowBlock,
		r.RowStart, r.RowEnd, r.FirstColBlock, r.LastColBlock, r.ColStart, r.ColEnd, allRows, allCols, r.Reason)
	if err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx, `
UPDATE checkpoints SET active=0, invalid_reason=?
WHERE shard_id=? AND active=1
  AND row_block BETWEEN ? AND ?
  AND col_end > ?`,
		fmt.Sprintf("%s:%s->%s", r.Trigger, r.OldFingerprint, r.NewFingerprint),
		r.ShardID, r.FirstRowBlock, r.LastRowBlock, r.ColStart)
	if err != nil {
		return err
	}
	_, err = res.RowsAffected()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
UPDATE row_heads SET valid=0
WHERE shard_id=? AND row_block BETWEEN ? AND ?`,
		r.ShardID, r.FirstRowBlock, r.LastRowBlock)
	if err != nil {
		return err
	}
	if r.AllColBlocks || r.Trigger == versioning.TriggerTiling || r.Trigger == versioning.TriggerCausal || r.Trigger == versioning.TriggerStrategy {
		_, err = tx.ExecContext(ctx, `DELETE FROM block_contributions WHERE shard_id=?`, r.ShardID)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListInvalidations(ctx context.Context, shardID string) ([]versioning.Range, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT shard_id,trigger,old_fp,new_fp,first_row_block,last_row_block,row_start,row_end,
 first_col_block,last_col_block,col_start,col_end,all_rows,all_cols,reason
FROM invalidations WHERE shard_id=? ORDER BY id`, shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []versioning.Range
	for rows.Next() {
		var r versioning.Range
		var allRows, allCols int
		if err := rows.Scan(&r.ShardID, &r.Trigger, &r.OldFingerprint, &r.NewFingerprint,
			&r.FirstRowBlock, &r.LastRowBlock, &r.RowStart, &r.RowEnd, &r.FirstColBlock, &r.LastColBlock,
			&r.ColStart, &r.ColEnd, &allRows, &allCols, &r.Reason); err != nil {
			return nil, err
		}
		r.AllRowBlocks = allRows == 1
		r.AllColBlocks = allCols == 1
		out = append(out, r)
	}
	return out, rows.Err()
}

func upsertContributionTx(ctx context.Context, tx *sql.Tx, shard, fp string, c attn.BlockContribution) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO block_contributions(shard_id,bh,row_block,col_block,col_start,col_end,head_dim,
 row_start,row_end,row_max_blob,exp_sum_blob,pv_blob,hash,version_fp)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(shard_id,bh,row_block,col_block) DO UPDATE SET
 col_start=excluded.col_start,col_end=excluded.col_end,head_dim=excluded.head_dim,
 row_start=excluded.row_start,row_end=excluded.row_end,
 row_max_blob=excluded.row_max_blob,exp_sum_blob=excluded.exp_sum_blob,pv_blob=excluded.pv_blob,
 hash=excluded.hash,version_fp=excluded.version_fp`,
		shard, c.BH, c.RowBlock, c.ColBlock, c.ColStart, c.ColEnd, c.HeadDim, c.RowStart, c.RowEnd,
		tensor.EncodeFloat64s(c.M), tensor.EncodeFloat64s(c.Sum), tensor.EncodeFloat64s(c.PV), c.Hash, fp)
	return err
}
