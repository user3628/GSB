package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"deployverify/internal/derive"
)

type ProbeResultRecord struct {
	RunID       string `json:"runId"`
	Env         string `json:"environment"`
	Service     string `json:"service"`
	Status      string `json:"status"`
	Code        string `json:"code,omitempty"`
	URL         string `json:"url,omitempty"`
	LatencyMS   int64  `json:"latencyMs"`
	Detail      string `json:"detail,omitempty"`
	CompletedAt string `json:"completedAt"`
}

type ProbeRun struct {
	ID                string              `json:"id"`
	IntentID          string              `json:"intentId,omitempty"`
	Version           int64               `json:"version"`
	Status            string              `json:"status"`
	GlobalConcurrency int                 `json:"globalConcurrency"`
	EnvConcurrency    int                 `json:"envConcurrency"`
	QueueCapacity     int                 `json:"queueCapacity"`
	BudgetMS          int64               `json:"budgetMs"`
	ProbeTimeoutMS    int64               `json:"probeTimeoutMs"`
	ElapsedMS         int64               `json:"elapsedMs"`
	ConsumedBudgetMS  int64               `json:"consumedBudgetMs"`
	TotalTasks        int                 `json:"totalTasks"`
	StartedCount      int                 `json:"startedCount"`
	SucceededCount    int                 `json:"succeededCount"`
	FailedCount       int                 `json:"failedCount"`
	TimeoutCount      int                 `json:"timeoutCount"`
	SkippedCount      int                 `json:"skippedCount"`
	DuplicateCount    int                 `json:"duplicateCount"`
	Report            any                 `json:"report,omitempty"`
	StartedAt         string              `json:"startedAt"`
	FinishedAt        string              `json:"finishedAt,omitempty"`
	Results           []ProbeResultRecord `json:"results,omitempty"`
}

func (s *Store) CreateProbeRun(ctx context.Context, run *ProbeRun, request any) error {
	intentID, err := s.InsertIntent(ctx, "probe_round", run.ID, request)
	if err != nil {
		return err
	}
	run.IntentID = intentID
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO probe_runs(id, intent_id, version, status, global_concurrency, env_concurrency, queue_capacity, budget_ms, probe_timeout_ms, total_tasks, started_at, report_json)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		run.ID, intentID, run.Version, run.Status, run.GlobalConcurrency, run.EnvConcurrency, run.QueueCapacity,
		run.BudgetMS, run.ProbeTimeoutMS, run.TotalTasks, nowISO(), "{}")
	return err
}

func (s *Store) CommitProbeResult(ctx context.Context, result ProbeResultRecord, consumedBudgetMS int64) (err error) {
	intentID, err := s.InsertIntent(ctx, "probe_result", result.RunID+":"+result.Env+":"+result.Service, result)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = s.finishIntent(context.Background(), intentID, "aborted", err.Error())
		}
	}()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO probe_results(run_id, environment, service, status, code, url, latency_ms, detail, completed_at)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(run_id, environment, service) DO UPDATE SET status=excluded.status, code=excluded.code, url=excluded.url,
		   latency_ms=excluded.latency_ms, detail=excluded.detail, completed_at=excluded.completed_at`,
		result.RunID, result.Env, result.Service, result.Status, result.Code, result.URL, result.LatencyMS, result.Detail, nowISO()); err != nil {
		return err
	}
	planStatus := result.Status
	if result.Status == "failure" {
		planStatus = "failed"
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE units SET probe_status=?, last_run_id=?, last_code=?, probed_at=? WHERE environment=? AND service=?`,
		planStatus, result.RunID, result.Code, nowISO(), result.Env, result.Service); err != nil {
		return err
	}
	var payload string
	err = tx.QueryRowContext(ctx, `SELECT payload_json FROM plans WHERE environment=? AND service=? ORDER BY version DESC LIMIT 1`, result.Env, result.Service).Scan(&payload)
	if err == nil {
		var plan derive.PlanItem
		if jsonErr := json.Unmarshal([]byte(payload), &plan); jsonErr != nil {
			return jsonErr
		}
		plan.Status = planStatus
		updated, jsonErr := json.Marshal(plan)
		if jsonErr != nil {
			return jsonErr
		}
		if _, err := tx.ExecContext(ctx, `UPDATE plans SET status=?, payload_json=? WHERE environment=? AND service=? AND version=?`,
			planStatus, string(updated), result.Env, result.Service, plan.Version); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE probe_runs SET consumed_budget_ms=MAX(consumed_budget_ms, ?) WHERE id=?`, consumedBudgetMS, result.RunID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', updated_at=? WHERE id=?`, nowISO(), intentID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) FinishProbeRun(ctx context.Context, run ProbeRun) error {
	reportJSON, err := json.Marshal(run.Report)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx,
		`UPDATE probe_runs SET status=?, elapsed_ms=?, consumed_budget_ms=?, total_tasks=?, started_count=?,
		   succeeded_count=?, failed_count=?, timeout_count=?, skipped_count=?, duplicate_count=?,
		   report_json=?, finished_at=? WHERE id=?`,
		run.Status, run.ElapsedMS, run.ConsumedBudgetMS, run.TotalTasks, run.StartedCount, run.SucceededCount,
		run.FailedCount, run.TimeoutCount, run.SkippedCount, run.DuplicateCount, string(reportJSON), nowISO(), run.ID)
	if err != nil {
		return err
	}
	if run.IntentID != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', last_error='', updated_at=? WHERE id=? AND status='pending'`, nowISO(), run.IntentID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) GetProbeRun(ctx context.Context, id string) (*ProbeRun, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, intent_id, version, status, global_concurrency, env_concurrency, queue_capacity, budget_ms,
		        probe_timeout_ms, elapsed_ms, consumed_budget_ms, total_tasks, started_count, succeeded_count,
		        failed_count, timeout_count, skipped_count, duplicate_count, report_json, started_at, finished_at
		 FROM probe_runs WHERE id=?`, id)
	run := &ProbeRun{}
	var reportJSON, startedAt, finishedAt, intentID sql.NullString
	if err := row.Scan(&run.ID, &intentID, &run.Version, &run.Status, &run.GlobalConcurrency, &run.EnvConcurrency,
		&run.QueueCapacity, &run.BudgetMS, &run.ProbeTimeoutMS, &run.ElapsedMS, &run.ConsumedBudgetMS,
		&run.TotalTasks, &run.StartedCount, &run.SucceededCount, &run.FailedCount, &run.TimeoutCount,
		&run.SkippedCount, &run.DuplicateCount, &reportJSON, &startedAt, &finishedAt); err != nil {
		return nil, err
	}
	run.IntentID = intentID.String
	run.StartedAt = startedAt.String
	run.FinishedAt = finishedAt.String
	if reportJSON.String != "" && reportJSON.String != "{}" {
		var report any
		if err := json.Unmarshal([]byte(reportJSON.String), &report); err == nil {
			run.Report = report
		}
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT run_id, environment, service, status, code, url, latency_ms, detail, completed_at
		 FROM probe_results WHERE run_id=? ORDER BY environment ASC, service ASC`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var result ProbeResultRecord
		var code, url, detail sql.NullString
		if err := rows.Scan(&result.RunID, &result.Env, &result.Service, &result.Status, &code, &url, &result.LatencyMS, &detail, &result.CompletedAt); err != nil {
			return nil, err
		}
		result.Code = code.String
		result.URL = url.String
		result.Detail = detail.String
		run.Results = append(run.Results, result)
	}
	return run, rows.Err()
}

func (s *Store) ListProbeRuns(ctx context.Context, limit int) ([]ProbeRun, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM probe_runs ORDER BY started_at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	out := []ProbeRun{}
	for _, id := range ids {
		run, err := s.GetProbeRun(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, *run)
	}
	return out, nil
}
