package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strconv"

	"local/windowengine/internal/domain"
)

func (s *Store) CreateRound(ctx context.Context, round Round, tasks []Task) error {
	return s.withWriteTx(ctx, func(tx *sql.Tx) error {
		for _, task := range tasks {
			var active string
			err := tx.QueryRowContext(ctx, `
				SELECT task_id FROM recalc_tasks
				WHERE snapshot_id = ? AND window_id = ? AND group_key = ? AND status IN ('QUEUED', 'RUNNING')
				LIMIT 1`, task.SnapshotID, task.WindowID, task.GroupKey,
			).Scan(&active)
			if err == nil {
				return domain.NewError(domain.CodeInvalidTransition, "window group already has an active recalculation task", map[string]any{
					"snapshot_id": task.SnapshotID, "window_id": task.WindowID,
					"group_key": task.GroupKey, "active_task_id": active,
				})
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO rounds(
				round_id, snapshot_id, status, global_concurrency, tenant_concurrency, time_budget_ms,
				queue_capacity, task_delay_ms, force_recalc, considered_count, remaining_budget_ms,
				budgets_json, started_at_ms
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			round.RoundID, round.SnapshotID, round.Status, round.GlobalConcurrency, round.TenantConcurrency,
			round.TimeBudgetMS, round.QueueCapacity, round.TaskDelayMS, boolInt(round.ForceRecalc),
			len(tasks), round.RemainingBudgetMS, round.BudgetsJSON, round.StartedAtMS,
		); err != nil {
			return err
		}
		for index := range tasks {
			task := &tasks[index]
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO recalc_tasks(
					task_id, round_id, snapshot_id, tenant_id, priority, window_id, group_key,
					spec_id, window_type, start_ms, end_ms, cost_ms, status, attempt, created_at_ms
				) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'QUEUED', 1, ?)`,
				task.TaskID, round.RoundID, task.SnapshotID, task.TenantID, task.Priority,
				task.WindowID, task.GroupKey, task.SpecID, task.WindowType, task.StartMS,
				task.EndMS, task.CostMS, nowMS(),
			); err != nil {
				if isUniqueViolation(err) {
					return domain.NewError(domain.CodeInvalidTransition, "duplicate active recalculation task", map[string]any{
						"task_id": task.TaskID, "window_id": task.WindowID, "group_key": task.GroupKey,
					})
				}
				return err
			}
		}
		return nil
	})
}

func (s *Store) StartTask(ctx context.Context, task Task) error {
	return s.withWriteTx(ctx, func(tx *sql.Tx) error {
		current, err := loadTaskTx(ctx, tx, task.TaskID)
		if err != nil {
			return err
		}
		if current.Status != "QUEUED" {
			return domain.NewError(domain.CodeInvalidTransition, "task cannot start from its current state", map[string]any{
				"task_id": task.TaskID, "status": current.Status, "allowed_from": "QUEUED",
			})
		}
		payload := taskPayload(current)
		created := nowMS()
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO intents(
				intent_id, intent_type, status, round_id, task_id, tenant_id, window_id,
				payload_json, created_at_ms, updated_at_ms
			) VALUES(?, 'RECALC_START', 'PENDING', ?, ?, ?, ?, ?, ?, ?)`,
			newID("intent_RECALC_START"), current.RoundID, current.TaskID, current.TenantID,
			current.WindowID, payload, created, created,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE recalc_tasks SET status = 'RUNNING', started_at_ms = ?, attempt = attempt
			WHERE task_id = ? AND status = 'QUEUED'`, created, current.TaskID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE windows SET status = 'RUNNING', updated_at_ms = ?
			WHERE snapshot_id = ? AND window_id = ?`, created, current.SnapshotID, current.WindowID,
		); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) StageAggregate(ctx context.Context, taskID string, aggregate domain.Aggregate) (string, error) {
	attemptID := newID("stage")
	err := s.withWriteTx(ctx, func(tx *sql.Tx) error {
		task, err := loadTaskTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if task.Status != "RUNNING" {
			return domain.NewError(domain.CodeInvalidTransition, "only a running task may stage an aggregate", map[string]any{
				"task_id": taskID, "status": task.Status,
			})
		}
		aggregateJSON := aggregatePersistenceJSON(aggregate)
		var minValue, maxValue any
		if aggregate.MinScaled != nil {
			minValue = *aggregate.MinScaled
		}
		if aggregate.MaxScaled != nil {
			maxValue = *aggregate.MaxScaled
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO window_staging(
				attempt_id, task_id, snapshot_id, window_id, tenant_id, group_key, spec_id,
				window_type, start_ms, end_ms, boundary, count_value, sum_scaled, min_scaled,
				max_scaled, sample_size, rounding_mode, aggregate_json, status, created_at_ms
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'STAGED', ?)`,
			attemptID, taskID, aggregate.SnapshotID, aggregate.WindowID, aggregate.TenantID,
			aggregate.GroupKey, aggregate.SpecID, aggregate.WindowType, aggregate.StartMS,
			aggregate.EndMS, aggregate.Boundary, aggregate.Count, aggregate.SumScaled,
			minValue, maxValue, aggregate.SampleSize, aggregate.Rounding, aggregateJSON, nowMS(),
		)
		return err
	})
	if err != nil {
		return "", err
	}
	return attemptID, nil
}

func (s *Store) PrepareCommitIntent(ctx context.Context, taskID, attemptID string, aggregate domain.Aggregate) error {
	return s.withWriteTx(ctx, func(tx *sql.Tx) error {
		task, err := loadTaskTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if task.Status != "RUNNING" {
			return domain.NewError(domain.CodeInvalidTransition, "commit intent requires a running task", map[string]any{
				"task_id": taskID, "status": task.Status,
			})
		}
		var status string
		err = tx.QueryRowContext(ctx, `
			SELECT status FROM window_staging WHERE attempt_id = ? AND task_id = ?`, attemptID, taskID,
		).Scan(&status)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.NewError(domain.CodeInvalidTransition, "staged aggregate was not found", map[string]any{
				"task_id": taskID, "attempt_id": attemptID,
			})
		}
		if err != nil {
			return err
		}
		if status != "STAGED" {
			return domain.NewError(domain.CodeInvalidTransition, "staged aggregate is not in STAGED state", map[string]any{
				"task_id": taskID, "attempt_id": attemptID, "status": status,
			})
		}
		payload := aggregatePersistenceJSON(aggregate)
		created := nowMS()
		_, err = tx.ExecContext(ctx, `
			INSERT INTO intents(
				intent_id, intent_type, status, round_id, task_id, tenant_id, window_id,
				payload_json, created_at_ms, updated_at_ms
			) VALUES(?, 'RECALC_COMMIT', 'PENDING', ?, ?, ?, ?, ?, ?, ?)`,
			newID("intent_RECALC_COMMIT"), task.RoundID, task.TaskID, task.TenantID,
			task.WindowID, payload, created, created,
		)
		return err
	})
}

func (s *Store) CommitTask(ctx context.Context, taskID, attemptID string, aggregate domain.Aggregate) (TenantState, error) {
	var state TenantState
	err := s.withWriteTx(ctx, func(tx *sql.Tx) error {
		task, err := loadTaskTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if task.Status != "RUNNING" {
			return domain.NewError(domain.CodeInvalidTransition, "only a running task may commit", map[string]any{
				"task_id": taskID, "status": task.Status,
			})
		}
		var stagingStatus string
		err = tx.QueryRowContext(ctx, `
			SELECT status FROM window_staging WHERE attempt_id = ? AND task_id = ?`, attemptID, taskID,
		).Scan(&stagingStatus)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && stagingStatus != "STAGED") {
			return domain.NewError(domain.CodeInvalidTransition, "staged aggregate is not promotable", map[string]any{
				"task_id": taskID, "attempt_id": attemptID, "staging_status": stagingStatus,
			})
		}
		if err != nil {
			return err
		}
		var pendingCommitIntents int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM intents
			WHERE task_id = ? AND intent_type = 'RECALC_COMMIT' AND status = 'PENDING'`,
			taskID,
		).Scan(&pendingCommitIntents); err != nil {
			return err
		}
		if pendingCommitIntents == 0 {
			return domain.NewError(domain.CodeInvalidTransition, "commit requires a durable RECALC_COMMIT intent", map[string]any{
				"task_id": taskID, "attempt_id": attemptID,
			})
		}

		var baseVersion int64
		err = tx.QueryRowContext(ctx, `
			SELECT version FROM tenant_intermediate WHERE snapshot_id = ? AND tenant_id = ?`,
			task.SnapshotID, task.TenantID,
		).Scan(&baseVersion)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.NewError(domain.CodeInvalidTransition, "tenant intermediate state is missing", map[string]any{
				"snapshot_id": task.SnapshotID, "tenant_id": task.TenantID,
			})
		}
		if err != nil {
			return err
		}
		aggregate.Version = baseVersion + 1
		var minValue, maxValue any
		if aggregate.MinScaled != nil {
			minValue = *aggregate.MinScaled
		}
		if aggregate.MaxScaled != nil {
			maxValue = *aggregate.MaxScaled
		}
		committedAt := nowMS()
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO window_aggregates(
				snapshot_id, window_id, tenant_id, spec_id, group_key, window_type, start_ms,
				end_ms, boundary, count_value, sum_scaled, min_scaled, max_scaled, sample_size,
				rounding_mode, version, task_id, committed_at_ms
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(snapshot_id, window_id) DO UPDATE SET
				tenant_id = excluded.tenant_id,
				spec_id = excluded.spec_id,
				group_key = excluded.group_key,
				window_type = excluded.window_type,
				start_ms = excluded.start_ms,
				end_ms = excluded.end_ms,
				boundary = excluded.boundary,
				count_value = excluded.count_value,
				sum_scaled = excluded.sum_scaled,
				min_scaled = excluded.min_scaled,
				max_scaled = excluded.max_scaled,
				sample_size = excluded.sample_size,
				rounding_mode = excluded.rounding_mode,
				version = excluded.version,
				task_id = excluded.task_id,
				committed_at_ms = excluded.committed_at_ms`,
			aggregate.SnapshotID, aggregate.WindowID, aggregate.TenantID, aggregate.SpecID,
			aggregate.GroupKey, aggregate.WindowType, aggregate.StartMS, aggregate.EndMS,
			aggregate.Boundary, aggregate.Count, aggregate.SumScaled, minValue, maxValue,
			aggregate.SampleSize, aggregate.Rounding, aggregate.Version, task.TaskID, committedAt,
		); err != nil {
			return err
		}

		state, err = recomputeTenantStateTx(ctx, tx, task.SnapshotID, task.TenantID, aggregate.Version, committedAt)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE windows SET status = 'COMMITTED', version = ?, updated_at_ms = ?
			WHERE snapshot_id = ? AND window_id = ?`,
			aggregate.Version, committedAt, task.SnapshotID, task.WindowID,
		); err != nil {
			return err
		}
		resultJSON := aggregatePersistenceJSON(aggregate)
		if _, err := tx.ExecContext(ctx, `
			UPDATE recalc_tasks
			SET status = 'COMMITTED', consumed_ms = cost_ms, result_json = ?, finished_at_ms = ?
			WHERE task_id = ? AND status = 'RUNNING'`,
			resultJSON, committedAt, task.TaskID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE window_staging SET status = 'PROMOTED' WHERE attempt_id = ? AND task_id = ?`,
			attemptID, task.TaskID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE intents SET status = 'APPLIED', updated_at_ms = ?
			WHERE task_id = ? AND status = 'PENDING'`, committedAt, task.TaskID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO commit_log(
				snapshot_id, tenant_id, task_id, window_id, group_key, committed_version,
				aggregate_json, created_at_ms
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
			task.SnapshotID, task.TenantID, task.TaskID, task.WindowID, task.GroupKey,
			aggregate.Version, resultJSON, committedAt,
		); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return TenantState{}, err
	}
	return state, nil
}

func (s *Store) FailTask(ctx context.Context, taskID string, taskErr error) error {
	var businessErr *domain.Error
	if !errors.As(taskErr, &businessErr) {
		businessErr = domain.NewError(domain.CodeInternal, taskErr.Error(), nil)
	}
	return s.withWriteTx(ctx, func(tx *sql.Tx) error {
		task, err := loadTaskTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if task.Status == "COMMITTED" {
			return nil
		}
		finished := nowMS()
		errorJSON := mustJSON(businessErr)
		if _, err := tx.ExecContext(ctx, `
			UPDATE recalc_tasks
			SET status = 'FAILED', consumed_ms = CASE WHEN status = 'RUNNING' THEN cost_ms ELSE 0 END,
				error_json = ?, finished_at_ms = ?
			WHERE task_id = ?`, errorJSON, finished, taskID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE window_staging SET status = 'ROLLED_BACK' WHERE task_id = ? AND status = 'STAGED'`,
			taskID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE intents SET status = 'ROLLED_BACK', recovery_note = ?, updated_at_ms = ?
			WHERE task_id = ? AND status = 'PENDING'`,
			"task failed: "+string(businessErr.Code), finished, taskID,
		); err != nil {
			return err
		}
		windowStatus := "PENDING"
		var aggregateCount int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM window_aggregates WHERE snapshot_id = ? AND window_id = ?`,
			task.SnapshotID, task.WindowID,
		).Scan(&aggregateCount); err != nil {
			return err
		}
		if aggregateCount > 0 {
			windowStatus = "COMMITTED"
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE windows SET status = ?, updated_at_ms = ? WHERE snapshot_id = ? AND window_id = ?`,
			windowStatus, finished, task.SnapshotID, task.WindowID,
		)
		return err
	})
}

func (s *Store) ShedTask(ctx context.Context, task Task, decision ShedDecision) error {
	var intentID string
	if err := s.withWriteTx(ctx, func(tx *sql.Tx) error {
		current, err := loadTaskTx(ctx, tx, task.TaskID)
		if err != nil {
			return err
		}
		if current.Status != "QUEUED" {
			return domain.NewError(domain.CodeInvalidTransition, "only queued tasks may be shed", map[string]any{
				"task_id": task.TaskID, "status": current.Status,
			})
		}
		intentID = newID("intent_RECALC_SHED")
		created := nowMS()
		_, err = tx.ExecContext(ctx, `
			INSERT INTO intents(
				intent_id, intent_type, status, round_id, task_id, tenant_id, window_id,
				payload_json, created_at_ms, updated_at_ms
			) VALUES(?, 'RECALC_SHED', 'PENDING', ?, ?, ?, ?, ?, ?, ?)`,
			intentID, current.RoundID, current.TaskID, current.TenantID, current.WindowID,
			mustJSON(decision), created, created,
		)
		return err
	}); err != nil {
		return err
	}

	return s.withWriteTx(ctx, func(tx *sql.Tx) error {
		current, err := loadTaskTx(ctx, tx, task.TaskID)
		if err != nil {
			return err
		}
		if current.Status != "QUEUED" {
			return domain.NewError(domain.CodeInvalidTransition, "task changed state before shed commit", map[string]any{
				"task_id": task.TaskID, "status": current.Status,
			})
		}
		created := nowMS()
		reasonJSON := mustJSON(decision.Basis)
		if _, err := tx.ExecContext(ctx, `
			UPDATE recalc_tasks
			SET status = 'SHED', consumed_ms = 0, shed_reason_json = ?, finished_at_ms = ?
			WHERE task_id = ? AND status = 'QUEUED'`, reasonJSON, created, current.TaskID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE windows SET status = 'SHED', updated_at_ms = ?
			WHERE snapshot_id = ? AND window_id = ?`, created, current.SnapshotID, current.WindowID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO shed_decisions(
				round_id, task_id, snapshot_id, tenant_id, window_id, group_key, priority,
				window_start_ms, decision_order, reason_code, basis_json, consumed_budget_ms,
				remaining_budget_ms, created_at_ms
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			decision.RoundID, current.TaskID, current.SnapshotID, current.TenantID,
			current.WindowID, current.GroupKey, current.Priority, current.StartMS,
			decision.DecisionOrder, decision.ReasonCode, mustJSON(decision.Basis),
			decision.ConsumedBudgetMS, decision.RemainingBudgetMS, created,
		); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE intents SET status = 'APPLIED', updated_at_ms = ?
			WHERE intent_id = ? AND status = 'PENDING'`, created, intentID,
		)
		return err
	})
}

func (s *Store) RecordTrace(ctx context.Context, roundID string, sequence int64, event TraceEvent) error {
	event.Sequence = sequence
	if event.AtMS == 0 {
		event.AtMS = nowMS()
	}
	detailJSON := ""
	if len(event.Detail) > 0 {
		detailJSON = mustJSON(event.Detail)
	}
	return s.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO scheduler_trace(
				round_id, sequence, event_type, tenant_id, window_id, task_id, group_key,
				priority, cost_ms, consumed_before_ms, consumed_after_ms, remaining_budget_ms,
				reason, detail_json, at_ms
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			roundID, sequence, event.EventType, event.TenantID, event.WindowID,
			event.TaskID, event.GroupKey, event.Priority, event.CostMS,
			event.ConsumedBeforeMS, event.ConsumedAfterMS, event.RemainingBudgetMS,
			event.Reason, detailJSON, event.AtMS,
		)
		return err
	})
}

func (s *Store) FinishRound(ctx context.Context, round Round) error {
	var errorJSON any
	if round.ErrorJSON != "" {
		errorJSON = round.ErrorJSON
	}
	return s.withWriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			UPDATE rounds SET
				status = ?, dispatched_count = ?, committed_count = ?, shed_count = ?,
				aborted_count = ?, failed_count = ?, consumed_budget_ms = ?,
				remaining_budget_ms = ?, max_global_running = ?, max_tenant_running = ?,
				error_json = ?, finished_at_ms = ?
			WHERE round_id = ?`,
			round.Status, round.DispatchedCount, round.CommittedCount, round.ShedCount,
			round.AbortedCount, round.FailedCount, round.ConsumedBudgetMS,
			round.RemainingBudgetMS, round.MaxGlobalRunning, round.MaxTenantRunning,
			errorJSON, round.FinishedAtMS, round.RoundID,
		)
		return err
	})
}

func loadTaskTx(ctx context.Context, tx *sql.Tx, taskID string) (Task, error) {
	var task Task
	var started, finished sql.NullInt64
	var result, taskError, shed sql.NullString
	err := tx.QueryRowContext(ctx, `
		SELECT task_id, round_id, snapshot_id, tenant_id, priority, window_id, group_key,
			spec_id, window_type, start_ms, end_ms, cost_ms, status, attempt, consumed_ms,
			result_json, error_json, shed_reason_json, created_at_ms, started_at_ms, finished_at_ms
		FROM recalc_tasks WHERE task_id = ?`, taskID,
	).Scan(
		&task.TaskID, &task.RoundID, &task.SnapshotID, &task.TenantID, &task.Priority,
		&task.WindowID, &task.GroupKey, &task.SpecID, &task.WindowType, &task.StartMS,
		&task.EndMS, &task.CostMS, &task.Status, &task.Attempt, &task.ConsumedMS,
		&result, &taskError, &shed, &task.CreatedAtMS, &started, &finished,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return task, domain.NewError(domain.CodeNotFound, "recalculation task not found", map[string]any{"task_id": taskID})
	}
	if err != nil {
		return task, err
	}
	task.ResultJSON = result.String
	task.ErrorJSON = taskError.String
	task.ShedJSON = shed.String
	task.StartedAtMS = started.Int64
	task.FinishedAtMS = finished.Int64
	return task, nil
}

func taskPayload(task Task) string {
	return mustJSON(map[string]any{
		"task_id": task.TaskID, "round_id": task.RoundID, "snapshot_id": task.SnapshotID,
		"tenant_id": task.TenantID, "priority": task.Priority, "window_id": task.WindowID,
		"group_key": task.GroupKey, "spec_id": task.SpecID, "window_type": task.WindowType,
		"window_start_ms": task.StartMS, "window_end_ms": task.EndMS, "cost_ms": task.CostMS,
	})
}

func aggregatePersistenceJSON(aggregate domain.Aggregate) string {
	minScaled := any(nil)
	minFixed := any(nil)
	if aggregate.MinScaled != nil {
		minScaled = strconv.FormatInt(*aggregate.MinScaled, 10)
		minFixed = domain.FormatFixed(*aggregate.MinScaled)
	}
	maxScaled := any(nil)
	maxFixed := any(nil)
	if aggregate.MaxScaled != nil {
		maxScaled = strconv.FormatInt(*aggregate.MaxScaled, 10)
		maxFixed = domain.FormatFixed(*aggregate.MaxScaled)
	}
	return mustJSON(map[string]any{
		"snapshot_id": aggregate.SnapshotID, "window_id": aggregate.WindowID,
		"tenant_id": aggregate.TenantID, "spec_id": aggregate.SpecID,
		"group_key": aggregate.GroupKey, "window_type": aggregate.WindowType,
		"window_start_ms": aggregate.StartMS, "window_end_ms": aggregate.EndMS,
		"boundary": aggregate.Boundary, "inclusivity": "start inclusive; end exclusive",
		"count": aggregate.Count, "sum_scaled": strconv.FormatInt(aggregate.SumScaled, 10),
		"sum_fixed": domain.FormatFixed(aggregate.SumScaled), "min_scaled": minScaled,
		"min_fixed": minFixed, "max_scaled": maxScaled, "max_fixed": maxFixed,
		"sample_size": aggregate.SampleSize, "scale": domain.Scale,
		"rounding_mode": aggregate.Rounding, "version": aggregate.Version,
	})
}

type groupAccumulator struct {
	windowCount int64
	totalCount  int64
	totalSum    int64
	min         *int64
	max         *int64
}

func recomputeTenantStateTx(
	ctx context.Context, tx *sql.Tx, snapshotID, tenantID string, version, updatedAt int64,
) (TenantState, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT group_key, count_value, sum_scaled, min_scaled, max_scaled
		FROM window_aggregates
		WHERE snapshot_id = ? AND tenant_id = ?
		ORDER BY group_key, start_ms, window_id`, snapshotID, tenantID,
	)
	if err != nil {
		return TenantState{}, err
	}
	groups := make(map[string]*groupAccumulator)
	var totalCount, totalSum, windowCount int64
	var totalMin, totalMax *int64
	for rows.Next() {
		var groupKey string
		var count, sum int64
		var minValue, maxValue sql.NullInt64
		if err := rows.Scan(&groupKey, &count, &sum, &minValue, &maxValue); err != nil {
			rows.Close()
			return TenantState{}, err
		}
		group := groups[groupKey]
		if group == nil {
			group = &groupAccumulator{}
			groups[groupKey] = group
		}
		if group.totalCount, err = checkedAddForTenant(group.totalCount, count, tenantID, "total_count"); err != nil {
			rows.Close()
			return TenantState{}, err
		}
		if group.totalSum, err = checkedAddForTenant(group.totalSum, sum, tenantID, "total_sum"); err != nil {
			rows.Close()
			return TenantState{}, err
		}
		if totalCount, err = checkedAddForTenant(totalCount, count, tenantID, "tenant_total_count"); err != nil {
			rows.Close()
			return TenantState{}, err
		}
		if totalSum, err = checkedAddForTenant(totalSum, sum, tenantID, "tenant_total_sum"); err != nil {
			rows.Close()
			return TenantState{}, err
		}
		if minValue.Valid {
			group.min = lowerBound(group.min, minValue.Int64)
			totalMin = lowerBound(totalMin, minValue.Int64)
		}
		if maxValue.Valid {
			group.max = upperBound(group.max, maxValue.Int64)
			totalMax = upperBound(totalMax, maxValue.Int64)
		}
		group.windowCount++
		windowCount++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return TenantState{}, err
	}
	rows.Close()

	groupKeys := make([]string, 0, len(groups))
	for groupKey := range groups {
		groupKeys = append(groupKeys, groupKey)
	}
	sort.Strings(groupKeys)
	entries := make([]GroupIndexEntry, 0, len(groupKeys))
	for _, groupKey := range groupKeys {
		group := groups[groupKey]
		entry := GroupIndexEntry{
			GroupKey: groupKey, Version: version, WindowCount: group.windowCount,
			TotalCount: group.totalCount, TotalSumScaled: strconv.FormatInt(group.totalSum, 10),
			TotalSumFixed: domain.FormatFixed(group.totalSum),
		}
		setScaledPointers(&entry.MinScaled, &entry.MinFixed, group.min)
		setScaledPointers(&entry.MaxScaled, &entry.MaxFixed, group.max)
		entries = append(entries, entry)
	}
	state := TenantState{
		SnapshotID: snapshotID, TenantID: tenantID, Version: version,
		TotalCount: totalCount, TotalSumScaled: strconv.FormatInt(totalSum, 10),
		TotalSumFixed: domain.FormatFixed(totalSum), CommittedWindowCount: windowCount,
		GroupCount: int64(len(entries)), Groups: entries, UpdatedAtMS: updatedAt,
	}
	setScaledPointers(&state.MinScaled, &state.MinFixed, totalMin)
	setScaledPointers(&state.MaxScaled, &state.MaxFixed, totalMax)
	groupIndexJSON := mustJSON(entries)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO tenant_intermediate(
			snapshot_id, tenant_id, version, total_count, total_sum_scaled, min_scaled,
			max_scaled, committed_window_count, group_count, group_index_json, updated_at_ms
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(snapshot_id, tenant_id) DO UPDATE SET
			version = excluded.version,
			total_count = excluded.total_count,
			total_sum_scaled = excluded.total_sum_scaled,
			min_scaled = excluded.min_scaled,
			max_scaled = excluded.max_scaled,
			committed_window_count = excluded.committed_window_count,
			group_count = excluded.group_count,
			group_index_json = excluded.group_index_json,
			updated_at_ms = excluded.updated_at_ms`,
		snapshotID, tenantID, version, totalCount, totalSum, nullableInt(totalMin),
		nullableInt(totalMax), windowCount, len(entries), groupIndexJSON, updatedAt,
	); err != nil {
		return TenantState{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM group_index WHERE snapshot_id = ? AND tenant_id = ?`, snapshotID, tenantID); err != nil {
		return TenantState{}, err
	}
	for _, entry := range entries {
		var minArg, maxArg any
		if entry.MinScaled != nil {
			minArg = *entry.MinScaled
		}
		if entry.MaxScaled != nil {
			maxArg = *entry.MaxScaled
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO group_index(
				snapshot_id, tenant_id, group_key, version, window_count, total_count,
				total_sum_scaled, min_scaled, max_scaled, updated_at_ms
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			snapshotID, tenantID, entry.GroupKey, version, entry.WindowCount,
			entry.TotalCount, entry.TotalSumScaled, minArg, maxArg, updatedAt,
		); err != nil {
			return TenantState{}, err
		}
	}
	return state, nil
}

func checkedAddForTenant(left, right int64, tenantID, field string) (int64, error) {
	result, ok := domain.CheckedAddInt64(left, right)
	if !ok {
		return 0, domain.NewError(domain.CodeNumericOverflow, "tenant intermediate int64 sum overflow", map[string]any{
			"tenant_id": tenantID, "field": field, "running_sum": left, "next_value": right,
		})
	}
	return result, nil
}

func lowerBound(current *int64, candidate int64) *int64 {
	if current == nil || candidate < *current {
		value := candidate
		return &value
	}
	return current
}

func upperBound(current *int64, candidate int64) *int64 {
	if current == nil || candidate > *current {
		value := candidate
		return &value
	}
	return current
}

func nullableInt(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func setScaledPointers(scaled **string, fixed **string, value *int64) {
	if value == nil {
		*scaled = nil
		*fixed = nil
		return
	}
	scaledText := strconv.FormatInt(*value, 10)
	fixedText := domain.FormatFixed(*value)
	*scaled = &scaledText
	*fixed = &fixedText
}

func (s *Store) Task(ctx context.Context, taskID string) (Task, error) {
	var task Task
	err := s.withWriteTx(ctx, func(tx *sql.Tx) error {
		loaded, err := loadTaskTx(ctx, tx, taskID)
		if err != nil {
			return err
		}
		task = loaded
		return nil
	})
	return task, err
}

func (s *Store) TasksForRound(ctx context.Context, roundID string) ([]Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT task_id, round_id, snapshot_id, tenant_id, priority, window_id, group_key,
			spec_id, window_type, start_ms, end_ms, cost_ms, status, attempt, consumed_ms,
			result_json, error_json, shed_reason_json, created_at_ms, started_at_ms, finished_at_ms
		FROM recalc_tasks WHERE round_id = ?
		ORDER BY priority, start_ms, spec_id, group_key, window_id`, roundID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks := make([]Task, 0)
	for rows.Next() {
		var task Task
		var result, taskError, shed sql.NullString
		var started, finished sql.NullInt64
		if err := rows.Scan(
			&task.TaskID, &task.RoundID, &task.SnapshotID, &task.TenantID, &task.Priority,
			&task.WindowID, &task.GroupKey, &task.SpecID, &task.WindowType, &task.StartMS,
			&task.EndMS, &task.CostMS, &task.Status, &task.Attempt, &task.ConsumedMS,
			&result, &taskError, &shed, &task.CreatedAtMS, &started, &finished,
		); err != nil {
			return nil, err
		}
		task.ResultJSON = result.String
		task.ErrorJSON = taskError.String
		task.ShedJSON = shed.String
		task.StartedAtMS = started.Int64
		task.FinishedAtMS = finished.Int64
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (s *Store) Round(ctx context.Context, roundID string) (Round, error) {
	var round Round
	var errorJSON sql.NullString
	var finished sql.NullInt64
	query := `
		SELECT round_id, snapshot_id, status, global_concurrency, tenant_concurrency,
			time_budget_ms, queue_capacity, task_delay_ms, force_recalc, considered_count,
			dispatched_count, committed_count, shed_count, aborted_count, failed_count,
			consumed_budget_ms, remaining_budget_ms, max_global_running, max_tenant_running,
			budgets_json, error_json, started_at_ms, finished_at_ms
		FROM rounds`
	var err error
	if roundID == "" {
		err = s.db.QueryRowContext(ctx, query+` ORDER BY started_at_ms DESC, round_id DESC LIMIT 1`).Scan(
			&round.RoundID, &round.SnapshotID, &round.Status, &round.GlobalConcurrency,
			&round.TenantConcurrency, &round.TimeBudgetMS, &round.QueueCapacity, &round.TaskDelayMS,
			&round.ForceRecalc, &round.ConsideredCount, &round.DispatchedCount, &round.CommittedCount,
			&round.ShedCount, &round.AbortedCount, &round.FailedCount, &round.ConsumedBudgetMS,
			&round.RemainingBudgetMS, &round.MaxGlobalRunning, &round.MaxTenantRunning,
			&round.BudgetsJSON, &errorJSON, &round.StartedAtMS, &finished,
		)
	} else {
		err = s.db.QueryRowContext(ctx, query+` WHERE round_id = ?`, roundID).Scan(
			&round.RoundID, &round.SnapshotID, &round.Status, &round.GlobalConcurrency,
			&round.TenantConcurrency, &round.TimeBudgetMS, &round.QueueCapacity, &round.TaskDelayMS,
			&round.ForceRecalc, &round.ConsideredCount, &round.DispatchedCount, &round.CommittedCount,
			&round.ShedCount, &round.AbortedCount, &round.FailedCount, &round.ConsumedBudgetMS,
			&round.RemainingBudgetMS, &round.MaxGlobalRunning, &round.MaxTenantRunning,
			&round.BudgetsJSON, &errorJSON, &round.StartedAtMS, &finished,
		)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return round, domain.NewError(domain.CodeNotFound, "recalculation round not found", map[string]any{"round_id": roundID})
	}
	if err != nil {
		return round, err
	}
	round.ErrorJSON = errorJSON.String
	round.FinishedAtMS = finished.Int64
	return round, nil
}

func (s *Store) Trace(ctx context.Context, roundID string) ([]TraceEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT sequence, event_type, tenant_id, window_id, task_id, group_key, priority,
			cost_ms, consumed_before_ms, consumed_after_ms, remaining_budget_ms, reason,
			detail_json, at_ms
		FROM scheduler_trace WHERE round_id = ? ORDER BY sequence, id`, roundID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]TraceEvent, 0)
	for rows.Next() {
		var event TraceEvent
		var detail sql.NullString
		if err := rows.Scan(
			&event.Sequence, &event.EventType, &event.TenantID, &event.WindowID,
			&event.TaskID, &event.GroupKey, &event.Priority, &event.CostMS,
			&event.ConsumedBeforeMS, &event.ConsumedAfterMS, &event.RemainingBudgetMS,
			&event.Reason, &detail, &event.AtMS,
		); err != nil {
			return nil, err
		}
		if detail.Valid && detail.String != "" {
			_ = json.Unmarshal([]byte(detail.String), &event.Detail)
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) ShedDecisions(ctx context.Context, roundID string) ([]ShedDecision, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT round_id, task_id, snapshot_id, tenant_id, window_id, group_key, priority,
			window_start_ms, decision_order, reason_code, basis_json, consumed_budget_ms,
			remaining_budget_ms, created_at_ms
		FROM shed_decisions WHERE round_id = ? ORDER BY decision_order, id`, roundID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	decisions := make([]ShedDecision, 0)
	for rows.Next() {
		var decision ShedDecision
		var basis string
		if err := rows.Scan(
			&decision.RoundID, &decision.TaskID, &decision.SnapshotID, &decision.TenantID,
			&decision.WindowID, &decision.GroupKey, &decision.Priority, &decision.WindowStartMS,
			&decision.DecisionOrder, &decision.ReasonCode, &basis,
			&decision.ConsumedBudgetMS, &decision.RemainingBudgetMS, &decision.CreatedAtMS,
		); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(basis), &decision.Basis)
		decisions = append(decisions, decision)
	}
	return decisions, rows.Err()
}

func (s *Store) Rounds(ctx context.Context, limit int) ([]Round, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT round_id FROM rounds ORDER BY started_at_ms DESC, round_id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	rounds := make([]Round, 0, len(ids))
	for _, id := range ids {
		round, err := s.Round(ctx, id)
		if err != nil {
			return nil, err
		}
		rounds = append(rounds, round)
	}
	return rounds, nil
}

func (s *Store) TenantState(ctx context.Context, snapshotID, tenantID string) (TenantState, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return TenantState{}, err
	}
	defer rollback(tx)
	state, err := tenantStateTx(ctx, tx, snapshotID, tenantID)
	if err != nil {
		return TenantState{}, err
	}
	if err := tx.Commit(); err != nil {
		return TenantState{}, err
	}
	return state, nil
}

func tenantStateTx(ctx context.Context, tx *sql.Tx, snapshotID, tenantID string) (TenantState, error) {
	var state TenantState
	var minValue, maxValue sql.NullInt64
	var groupIndexJSON string
	var totalSum int64
	err := tx.QueryRowContext(ctx, `
		SELECT version, total_count, total_sum_scaled, min_scaled, max_scaled,
			committed_window_count, group_count, group_index_json, updated_at_ms
		FROM tenant_intermediate WHERE snapshot_id = ? AND tenant_id = ?`,
		snapshotID, tenantID,
	).Scan(
		&state.Version, &state.TotalCount, &totalSum, &minValue, &maxValue,
		&state.CommittedWindowCount, &state.GroupCount, &groupIndexJSON, &state.UpdatedAtMS,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return state, domain.NewError(domain.CodeNotFound, "tenant state not found", map[string]any{
			"snapshot_id": snapshotID, "tenant_id": tenantID,
		})
	}
	if err != nil {
		return state, err
	}
	state.SnapshotID = snapshotID
	state.TenantID = tenantID
	state.TotalSumScaled = strconv.FormatInt(totalSum, 10)
	state.TotalSumFixed = domain.FormatFixed(totalSum)
	setScaledPointers(&state.MinScaled, &state.MinFixed, nullInt64Pointer(minValue))
	setScaledPointers(&state.MaxScaled, &state.MaxFixed, nullInt64Pointer(maxValue))
	_ = json.Unmarshal([]byte(groupIndexJSON), &state.Groups)
	if state.Groups == nil {
		state.Groups = []GroupIndexEntry{}
	}
	return state, nil
}
