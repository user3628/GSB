package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"checkpointengine/internal/model"
)

type intentSpec struct {
	Kind         string
	SubmissionID string
	Before       any
	After        any
}

func (e *Engine) insertIntents(ctx context.Context, specs []intentSpec) ([]int64, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	tx, err := e.store.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ids := make([]int64, 0, len(specs))
	now := e.nowText()
	for _, spec := range specs {
		before := ""
		after := ""
		if spec.Before != nil {
			b, err := json.Marshal(spec.Before)
			if err != nil {
				return nil, err
			}
			before = string(b)
		}
		if spec.After != nil {
			b, err := json.Marshal(spec.After)
			if err != nil {
				return nil, err
			}
			after = string(b)
		}
		res, err := tx.ExecContext(ctx, `INSERT INTO intents(kind, submission_id, state, before_json, after_json, created_at, applied_at)
			VALUES(?, ?, 'PENDING', ?, ?, ?, '')`, spec.Kind, spec.SubmissionID, before, after, now)
		if err != nil {
			return nil, err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

func markIntentsApplied(ctx context.Context, tx *sql.Tx, ids []int64, now string) error {
	for _, id := range ids {
		res, err := tx.ExecContext(ctx, `UPDATE intents SET state='APPLIED', applied_at=? WHERE intent_id=? AND state='PENDING'`, now, id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("intent %d was not pending", id)
		}
	}
	return nil
}

func (e *Engine) abortIntents(ctx context.Context, ids []int64) {
	if len(ids) == 0 {
		return
	}
	_, _ = e.store.DB.ExecContext(ctx, `UPDATE intents SET state='ABORTED', applied_at=? WHERE state='PENDING' AND intent_id IN (`+placeholders(len(ids))+`)`, append([]any{e.nowText()}, int64Args(ids)...)...)
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	out := "?"
	for i := 1; i < n; i++ {
		out += ",?"
	}
	return out
}

func int64Args(ids []int64) []any {
	out := make([]any, len(ids))
	for i, id := range ids {
		out[i] = id
	}
	return out
}

type evictionIntentJSON struct {
	Round     int         `json:"round"`
	ShardID   string      `json:"shard_id"`
	Before    model.Shard `json:"before"`
	AfterPool string      `json:"after_pool"`
	Reason    string      `json:"reason"`
}

type writeIntentJSON struct {
	Round     int    `json:"round"`
	ShardID   string `json:"shard_id"`
	Digest    string `json:"digest"`
	BlobPath  string `json:"blob_path"`
	Precision string `json:"precision"`
	Bytes     int64  `json:"bytes"`
}
