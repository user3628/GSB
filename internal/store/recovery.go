package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"

	"local/windowengine/internal/domain"
)

const recoveryDisclaimer = "Report is derived only from committed SQLite intent, task, staging, and window rows; no exit-time report is promised."

func (s *Store) Recover(ctx context.Context) (RecoveryReport, error) {
	reportID := newID("recovery")
	var report RecoveryReport
	err := s.withWriteTx(ctx, func(tx *sql.Tx) error {
		createdAt := nowMS()
		pendingIntents, err := pendingIntentsTx(ctx, tx)
		if err != nil {
			return err
		}
		report = RecoveryReport{
			ReportID: reportID, BasedOnDurableFacts: true,
			PendingIntentCount: len(pendingIntents), PendingIntents: pendingIntents,
			AbortedTaskIDs: []string{}, ResetWindowIDs: []string{},
			FinalizedIntentIDs: []string{}, RunningTaskIDs: []string{},
			InterruptedRoundIDs: []string{}, Disclaimer: recoveryDisclaimer, CreatedAtMS: createdAt,
		}
		abortedSet := make(map[string]struct{})
		resetSet := make(map[string]struct{})
		finalizedSet := make(map[string]struct{})

		for _, intent := range pendingIntents {
			task, taskErr := loadTaskTx(ctx, tx, intent.TaskID)
			if taskErr != nil {
				var business *domain.Error
				if !errors.As(taskErr, &business) || business.Code != domain.CodeNotFound {
					return taskErr
				}
				note := "task row was absent; intent finalized without task mutation"
				if _, err := tx.ExecContext(ctx, `
					UPDATE intents SET status = 'ROLLED_BACK', recovery_note = ?, updated_at_ms = ?
					WHERE intent_id = ? AND status = 'PENDING'`, note, createdAt, intent.IntentID,
				); err != nil {
					return err
				}
				finalizedSet[intent.IntentID] = struct{}{}
				continue
			}
			if task.Status == "COMMITTED" {
				note := "task was already committed; intent marked APPLIED from durable task state"
				if _, err := tx.ExecContext(ctx, `
					UPDATE intents SET status = 'APPLIED', recovery_note = ?, updated_at_ms = ?
					WHERE intent_id = ? AND status = 'PENDING'`, note, createdAt, intent.IntentID,
				); err != nil {
					return err
				}
				finalizedSet[intent.IntentID] = struct{}{}
				continue
			}
			windowReset, err := abortTaskForRecoveryTx(ctx, tx, task, "pending intent discovered at startup", createdAt)
			if err != nil {
				return err
			}
			abortedSet[task.TaskID] = struct{}{}
			if windowReset {
				resetSet[task.WindowID] = struct{}{}
			}
			if _, err := tx.ExecContext(ctx, `
				UPDATE intents SET status = 'ROLLED_BACK', recovery_note = ?, updated_at_ms = ?
				WHERE intent_id = ? AND status = 'PENDING'`,
				"rolled back because "+intent.IntentType+" was not durably completed",
				createdAt, intent.IntentID,
			); err != nil {
				return err
			}
			finalizedSet[intent.IntentID] = struct{}{}
		}

		rows, err := tx.QueryContext(ctx, `
			SELECT task_id FROM recalc_tasks
			WHERE status IN ('QUEUED', 'RUNNING')
			ORDER BY created_at_ms, task_id`)
		if err != nil {
			return err
		}
		orphanIDs := make([]string, 0)
		for rows.Next() {
			var taskID string
			if err := rows.Scan(&taskID); err != nil {
				rows.Close()
				return err
			}
			orphanIDs = append(orphanIDs, taskID)
		}
		rows.Close()
		for _, taskID := range orphanIDs {
			task, err := loadTaskTx(ctx, tx, taskID)
			if err != nil {
				return err
			}
			report.RunningTaskIDs = append(report.RunningTaskIDs, taskID)
			windowReset, err := abortTaskForRecoveryTx(ctx, tx, task, "orphaned nonterminal task at startup", createdAt)
			if err != nil {
				return err
			}
			abortedSet[taskID] = struct{}{}
			if windowReset {
				resetSet[task.WindowID] = struct{}{}
			}
			intents, err := pendingIntentsForTaskTx(ctx, tx, taskID)
			if err != nil {
				return err
			}
			for _, intent := range intents {
				finalizedSet[intent.IntentID] = struct{}{}
			}
		}

		roundRows, err := tx.QueryContext(ctx, `
			SELECT round_id FROM rounds WHERE status = 'RUNNING' ORDER BY started_at_ms, round_id`)
		if err != nil {
			return err
		}
		for roundRows.Next() {
			var roundID string
			if err := roundRows.Scan(&roundID); err != nil {
				roundRows.Close()
				return err
			}
			report.InterruptedRoundIDs = append(report.InterruptedRoundIDs, roundID)
		}
		roundRows.Close()
		if _, err := tx.ExecContext(ctx, `
			UPDATE rounds SET status = 'INTERRUPTED', finished_at_ms = ?
			WHERE status = 'RUNNING'`, createdAt,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE window_staging SET status = 'ROLLED_BACK'
			WHERE status = 'STAGED'
			  AND task_id IN (SELECT task_id FROM recalc_tasks WHERE status <> 'COMMITTED')`,
		); err != nil {
			return err
		}

		report.AbortedTaskIDs = sortedKeys(abortedSet)
		report.ResetWindowIDs = sortedKeys(resetSet)
		report.FinalizedIntentIDs = sortedKeys(finalizedSet)
		report.Details = map[string]any{
			"pending_intents":      report.PendingIntents,
			"aborted_task_ids":     report.AbortedTaskIDs,
			"reset_window_ids":     report.ResetWindowIDs,
			"finalized_intent_ids": report.FinalizedIntentIDs,
			"running_task_ids":     report.RunningTaskIDs,
			"interrupted_rounds":   report.InterruptedRoundIDs,
		}
		detailsJSON := mustJSON(report)
		_, err = tx.ExecContext(ctx, `
			INSERT INTO recovery_reports(
				report_id, based_on_durable_facts, pending_intent_count,
				aborted_task_ids_json, reset_window_ids_json, details_json,
				disclaimer, created_at_ms
			) VALUES(?, 1, ?, ?, ?, ?, ?, ?)`,
			reportID, report.PendingIntentCount, mustJSON(report.AbortedTaskIDs),
			mustJSON(report.ResetWindowIDs), detailsJSON, recoveryDisclaimer, createdAt,
		)
		return err
	})
	if err != nil {
		return RecoveryReport{}, err
	}
	return report, nil
}

func abortTaskForRecoveryTx(
	ctx context.Context, tx *sql.Tx, task Task, note string, at int64,
) (bool, error) {
	if task.Status == "COMMITTED" {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE recalc_tasks SET status = 'ABORTED', consumed_ms = 0,
			error_json = ?, finished_at_ms = ? WHERE task_id = ?`,
		mustJSON(domain.NewError(domain.CodeInvalidTransition, "task aborted during startup recovery", map[string]any{
			"task_id": task.TaskID, "previous_status": task.Status, "note": note,
		})), at, task.TaskID,
	); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE window_staging SET status = 'ROLLED_BACK'
		WHERE task_id = ? AND status = 'STAGED'`, task.TaskID,
	); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE intents SET status = 'ROLLED_BACK', recovery_note = ?, updated_at_ms = ?
		WHERE task_id = ? AND status = 'PENDING'`, note, at, task.TaskID,
	); err != nil {
		return false, err
	}
	var aggregateCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM window_aggregates WHERE snapshot_id = ? AND window_id = ?`,
		task.SnapshotID, task.WindowID,
	).Scan(&aggregateCount); err != nil {
		return false, err
	}
	windowStatus := "PENDING"
	if aggregateCount > 0 {
		windowStatus = "COMMITTED"
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE windows SET status = ?, updated_at_ms = ? WHERE snapshot_id = ? AND window_id = ?`,
		windowStatus, at, task.SnapshotID, task.WindowID,
	); err != nil {
		return false, err
	}
	return aggregateCount == 0, nil
}

func pendingIntentsTx(ctx context.Context, tx *sql.Tx) ([]Intent, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, intent_id, intent_type, status, round_id, task_id, tenant_id,
			window_id, payload_json, recovery_note, created_at_ms, updated_at_ms
		FROM intents WHERE status = 'PENDING' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	intents := make([]Intent, 0)
	for rows.Next() {
		var intent Intent
		if err := rows.Scan(
			&intent.ID, &intent.IntentID, &intent.IntentType, &intent.Status,
			&intent.RoundID, &intent.TaskID, &intent.TenantID, &intent.WindowID,
			&intent.PayloadJSON, &intent.RecoveryNote, &intent.CreatedAtMS, &intent.UpdatedAtMS,
		); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(intent.PayloadJSON), &intent.Payload)
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}

func pendingIntentsForTaskTx(ctx context.Context, tx *sql.Tx, taskID string) ([]Intent, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, intent_id, intent_type, status, round_id, task_id, tenant_id,
			window_id, payload_json, recovery_note, created_at_ms, updated_at_ms
		FROM intents WHERE task_id = ? AND status = 'PENDING' ORDER BY id`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	intents := make([]Intent, 0)
	for rows.Next() {
		var intent Intent
		if err := rows.Scan(
			&intent.ID, &intent.IntentID, &intent.IntentType, &intent.Status,
			&intent.RoundID, &intent.TaskID, &intent.TenantID, &intent.WindowID,
			&intent.PayloadJSON, &intent.RecoveryNote, &intent.CreatedAtMS, &intent.UpdatedAtMS,
		); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(intent.PayloadJSON), &intent.Payload)
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (s *Store) RecoveryReport(ctx context.Context, reportID string) (RecoveryReport, *domain.Error) {
	query := `
		SELECT report_id, based_on_durable_facts, pending_intent_count,
			aborted_task_ids_json, reset_window_ids_json, details_json, disclaimer, created_at_ms
		FROM recovery_reports`
	var row *sql.Row
	if reportID == "" {
		row = s.db.QueryRowContext(ctx, query+` ORDER BY created_at_ms DESC, report_id DESC LIMIT 1`)
	} else {
		row = s.db.QueryRowContext(ctx, query+` WHERE report_id = ?`, reportID)
	}
	var report RecoveryReport
	var basedOnFacts int
	var abortedJSON, resetJSON, detailsJSON string
	if err := row.Scan(
		&report.ReportID, &basedOnFacts, &report.PendingIntentCount,
		&abortedJSON, &resetJSON, &detailsJSON, &report.Disclaimer, &report.CreatedAtMS,
	); errors.Is(err, sql.ErrNoRows) {
		return report, domain.NewError(domain.CodeNotFound, "recovery report not found", map[string]any{"report_id": reportID})
	} else if err != nil {
		return report, domain.NewError(domain.CodeInternal, "read recovery report failed", map[string]any{"reason": err.Error()})
	}
	report.BasedOnDurableFacts = basedOnFacts == 1
	_ = json.Unmarshal([]byte(abortedJSON), &report.AbortedTaskIDs)
	_ = json.Unmarshal([]byte(resetJSON), &report.ResetWindowIDs)
	var details RecoveryReport
	_ = json.Unmarshal([]byte(detailsJSON), &details)
	report.PendingIntents = details.PendingIntents
	report.FinalizedIntentIDs = details.FinalizedIntentIDs
	report.RunningTaskIDs = details.RunningTaskIDs
	report.InterruptedRoundIDs = details.InterruptedRoundIDs
	report.Details = details.Details
	if report.AbortedTaskIDs == nil {
		report.AbortedTaskIDs = []string{}
	}
	if report.ResetWindowIDs == nil {
		report.ResetWindowIDs = []string{}
	}
	if report.PendingIntents == nil {
		report.PendingIntents = []Intent{}
	}
	if report.FinalizedIntentIDs == nil {
		report.FinalizedIntentIDs = []string{}
	}
	if report.RunningTaskIDs == nil {
		report.RunningTaskIDs = []string{}
	}
	if report.InterruptedRoundIDs == nil {
		report.InterruptedRoundIDs = []string{}
	}
	return report, nil
}

func (s *Store) RecoveryReports(ctx context.Context, limit int) ([]RecoveryReport, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT report_id FROM recovery_reports ORDER BY created_at_ms DESC, report_id DESC LIMIT ?`, limit)
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
	reports := make([]RecoveryReport, 0, len(ids))
	for _, id := range ids {
		report, business := s.RecoveryReport(ctx, id)
		if business != nil {
			return nil, business
		}
		reports = append(reports, report)
	}
	return reports, nil
}
