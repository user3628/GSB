package store

import (
	"context"
	"encoding/json"
	"time"

	"ropechunk/internal/model"
)

func (s *Store) RecordBudgetEvents(ctx context.Context, events []model.BudgetEvent) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, event := range events {
		created := timestamp(event.CreatedAt)
		if _, err := tx.ExecContext(ctx, `INSERT INTO budget_events(run_id,shard_id,stage,batch,head,row_block,
			status,reason,estimated_us,remaining_us,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			event.RunID, event.ShardID, event.Stage, event.Batch, event.Head, event.RowBlock,
			event.Status, event.Reason, event.EstimatedUS, event.RemainingUS, created); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListBudgetEvents(ctx context.Context, runID string) ([]model.BudgetEvent, error) {
	query := `SELECT run_id,shard_id,stage,batch,head,row_block,status,reason,estimated_us,remaining_us,created_at
		FROM budget_events`
	args := []any{}
	if runID != "" {
		query += ` WHERE run_id=?`
		args = append(args, runID)
	}
	query += ` ORDER BY id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.BudgetEvent
	for rows.Next() {
		var e model.BudgetEvent
		if err := rows.Scan(&e.RunID, &e.ShardID, &e.Stage, &e.Batch, &e.Head, &e.RowBlock,
			&e.Status, &e.Reason, &e.EstimatedUS, &e.RemainingUS, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) SaveMemoryPlan(ctx context.Context, runID string, plan model.MemoryPlan) error {
	fullJSON, err := json.Marshal(plan.Full)
	if err != nil {
		return err
	}
	stepsJSON, err := json.Marshal(plan.Steps)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO memory_plans(run_id,shard_id,stage,budget_bytes,
		full_estimate_json,mode,steps_json,created_at) VALUES(?,?,?,?,?,?,?,?)`,
		runID, plan.ShardID, plan.Stage, plan.Budget, string(fullJSON), plan.Mode, string(stepsJSON), timestamp(plan.CreatedAt))
	return err
}

func (s *Store) ListMemoryPlans(ctx context.Context, runID string) ([]model.MemoryPlan, error) {
	query := `SELECT shard_id,stage,budget_bytes,full_estimate_json,mode,steps_json,created_at FROM memory_plans`
	args := []any{}
	if runID != "" {
		query += ` WHERE run_id=?`
		args = append(args, runID)
	}
	query += ` ORDER BY id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.MemoryPlan
	for rows.Next() {
		var p model.MemoryPlan
		var fullJSON, stepsJSON string
		if err := rows.Scan(&p.ShardID, &p.Stage, &p.Budget, &fullJSON, &p.Mode, &stepsJSON, &p.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(fullJSON), &p.Full); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(stepsJSON), &p.Steps); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) StartRun(ctx context.Context, run model.RunRecord) error {
	if run.StartedAt == "" {
		run.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO runs(id,config_version,stage,status,started_at,finished_at,note)
		VALUES(?,?,?,?,?,?,?)`, run.ID, run.ConfigVersion, run.Stage, run.Status, run.StartedAt, run.FinishedAt, run.Note)
	return err
}

func (s *Store) FinishRun(ctx context.Context, runID, status, note string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET status=?,finished_at=?,note=? WHERE id=?`,
		status, time.Now().UTC().Format(time.RFC3339Nano), note, runID)
	return err
}

func (s *Store) ListRuns(ctx context.Context) ([]model.RunRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,config_version,stage,status,started_at,finished_at,note FROM runs ORDER BY started_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.RunRecord
	for rows.Next() {
		var r model.RunRecord
		if err := rows.Scan(&r.ID, &r.ConfigVersion, &r.Stage, &r.Status, &r.StartedAt, &r.FinishedAt, &r.Note); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
