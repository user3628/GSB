package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"

	"checkpointengine/internal/model"
)

func (e *Engine) State(ctx context.Context) (model.StateView, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return model.StateView{}, err
	}
	defer tx.Rollback()
	st, err := loadStateTx(ctx, tx)
	if err != nil {
		return model.StateView{}, err
	}
	view := model.StateView{CurrentPolicyVersion: st.CurrentPolicyVersion, Revision: st.Revision,
		LossScale: st.LossScale, LossScaleGrowth: st.LossScaleGrowth}
	if st.CurrentPolicyVersion > 0 {
		p, err := loadPolicyTx(ctx, tx, st.CurrentPolicyVersion)
		if err != nil {
			return model.StateView{}, err
		}
		view.Policy = &p.Snapshot
	}
	view.Pools, err = loadPoolsTx(ctx, tx)
	if err != nil {
		return model.StateView{}, err
	}
	return view, nil
}

func (e *Engine) CurrentPolicy(ctx context.Context) (*model.Snapshot, int64, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()
	st, err := loadStateTx(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	if st.CurrentPolicyVersion == 0 {
		return nil, st.Revision, nil
	}
	p, err := loadPolicyTx(ctx, tx, st.CurrentPolicyVersion)
	if err != nil {
		return nil, 0, err
	}
	snap := p.Snapshot
	return &snap, st.Revision, nil
}

func (e *Engine) ListShards(ctx context.Context, round int, pool string) (model.ShardQuery, error) {
	pool = model.NormalizePool(pool)
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return model.ShardQuery{}, err
	}
	defer tx.Rollback()
	st, err := loadStateTx(ctx, tx)
	if err != nil {
		return model.ShardQuery{}, err
	}
	shards, err := loadShardsTx(ctx, tx, round, pool)
	if err != nil {
		return model.ShardQuery{}, err
	}
	return model.ShardQuery{Round: round, Pool: pool, Revision: st.Revision, Shards: shards}, nil
}

func (e *Engine) Pools(ctx context.Context) ([]model.PoolUsage, int64, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()
	st, err := loadStateTx(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	pools, err := loadPoolsTx(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	if st.CurrentPolicyVersion > 0 {
		p, err := loadPolicyTx(ctx, tx, st.CurrentPolicyVersion)
		if err == nil {
			for i := range pools {
				if pools[i].Pool == model.PoolVRAM {
					pools[i].LimitBytes = p.VRAMLimitBytes
				}
			}
		}
	}
	return pools, st.Revision, nil
}

func (e *Engine) Submission(ctx context.Context, id string) (*model.SubmissionResponse, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var result string
	err = tx.QueryRowContext(ctx, `SELECT result_json FROM submissions WHERE submission_id=?`, id).Scan(&result)
	if isNoRows(err) {
		return nil, model.WithDetails(model.NewAPIError(model.CodeNotFound, "submission not found"), map[string]any{"submission_id": id})
	}
	if err != nil {
		return nil, err
	}
	var response model.SubmissionResponse
	if err := json.Unmarshal([]byte(result), &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (e *Engine) RoundSummaries(ctx context.Context, round int) ([]model.RoundSummary, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	all, err := loadRoundSummariesTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if round == 0 {
		return all, nil
	}
	var out []model.RoundSummary
	for _, s := range all {
		if s.Round == round {
			out = append(out, s)
		}
	}
	return out, nil
}

func (e *Engine) Derivatives(ctx context.Context, round int) ([]model.Derivative, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return loadDerivativesTx(ctx, tx, round)
}

func (e *Engine) WriteQueue(ctx context.Context) ([]model.WriteTask, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT round, shard_id, precision, digest, bytes,
		priority, write_ms, reason FROM write_queue WHERE status='WAITING'
		ORDER BY priority DESC, round ASC, shard_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.WriteTask
	for rows.Next() {
		var t model.WriteTask
		if err := rows.Scan(&t.Round, &t.ShardID, &t.Precision, &t.Digest, &t.Bytes,
			&t.Priority, &t.WriteMS, &t.QueueReason); err != nil {
			return nil, err
		}
		t.Status = model.WritePending
		out = append(out, t)
	}
	return out, rows.Err()
}

func (e *Engine) WriteRecords(ctx context.Context, submission string) ([]model.WriteRecord, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	query := `SELECT record_id, submission_id, round, shard_id, digest, bytes, pool, committed_at, blob_path FROM write_records`
	var args []any
	if submission != "" {
		query += ` WHERE submission_id=?`
		args = append(args, submission)
	}
	query += ` ORDER BY record_id`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.WriteRecord
	for rows.Next() {
		var r model.WriteRecord
		if err := rows.Scan(&r.RecordID, &r.SubmissionID, &r.Round, &r.ShardID,
			&r.Digest, &r.Bytes, &r.Pool, &r.CommittedAt, &r.BlobPath); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (e *Engine) PrecisionResults(ctx context.Context, round int) ([]model.PrecisionResult, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return loadPrecisionResultsTx(ctx, tx, round)
}

func (e *Engine) Intents(ctx context.Context, pendingOnly bool) ([]model.Intent, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	query := `SELECT intent_id, kind, submission_id, state, before_json, after_json, created_at, applied_at FROM intents`
	if pendingOnly {
		query += ` WHERE state='PENDING'`
	}
	query += ` ORDER BY intent_id`
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Intent
	for rows.Next() {
		var i model.Intent
		if err := rows.Scan(&i.ID, &i.Kind, &i.SubmissionID, &i.State, &i.BeforeJSON,
			&i.AfterJSON, &i.CreatedAt, &i.AppliedAt); err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

func (e *Engine) Snapshots(ctx context.Context) ([]model.Snapshot, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT version FROM strategy_snapshots ORDER BY version`)
	if err != nil {
		return nil, err
	}
	var versions []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return nil, err
		}
		versions = append(versions, v)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var out []model.Snapshot
	for _, v := range versions {
		p, err := loadPolicyTx(ctx, tx, v)
		if err != nil {
			return nil, err
		}
		out = append(out, p.Snapshot)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}
