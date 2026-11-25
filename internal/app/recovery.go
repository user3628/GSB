package app

import (
	"context"
	"encoding/json"

	"elastic-scale/internal/engine"
	"elastic-scale/internal/model"
	"elastic-scale/internal/store"
)

func storeNowMS() int64 { return store.NowMS() }

func (a *App) recover(ctx context.Context) (*model.RecoveryReport, error) {
	started := store.NowMS()
	rows, err := a.db.QueryContext(ctx, `SELECT commit_id FROM intents WHERE status='pending' ORDER BY created_at_ms`)
	if err != nil {
		return nil, err
	}
	var pending []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		pending = append(pending, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	report := &model.RecoveryReport{StartedAtMS: started, FoundPendingIntents: len(pending), AbortedCommitIDs: pending}
	for _, id := range pending {
		errJSON, _ := json.Marshal(model.NewError("E_RECOVERY_ABORTED", "process exited before the intent transaction committed", map[string]any{"commit_id": id}))
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='aborted', error_code='E_RECOVERY_ABORTED', error_json=?, updated_at_ms=? WHERE commit_id=? AND status='pending'`, string(errJSON), store.NowMS(), id); err != nil {
			return nil, err
		}
	}
	current, err := getMeta(ctx, tx, "current_version")
	if err != nil {
		return nil, err
	}
	materialized, err := getMeta(ctx, tx, "materialized_version")
	if err != nil {
		return nil, err
	}
	report.RecoveredToVersion = current
	report.MaterializedVersion = current
	needRebuild := len(pending) > 0 || materialized != current
	notes := []string{}
	if needRebuild && current > 0 {
		state, err := loadState(ctx, tx)
		if err != nil {
			notes = append(notes, "derived rebuild skipped while loading committed state failed: "+err.Error())
			report.MaterializedVersion = materialized
		} else {
			views, err := engine.BuildAll(state)
			if err != nil {
				notes = append(notes, "derived rebuild failed: "+err.Error())
				report.MaterializedVersion = materialized
			} else {
				if err := persistDerivedTx(ctx, tx, state, views, nil); err != nil {
					return nil, err
				}
				if err := setMeta(ctx, tx, "materialized_version", current); err != nil {
					return nil, err
				}
				notes = append(notes, "materialized metrics, impact sets, decisions and refs were rebuilt from the last complete SQLite version")
				report.MaterializedVersion = current
			}
		}
	} else if len(pending) > 0 {
		notes = append(notes, "pending intents aborted; no committed snapshot version existed yet")
	} else {
		notes = append(notes, "no pending intents; derived views already match the last complete version")
	}
	report.FinishedAtMS = store.NowMS()
	report.Action = "clean_scan"
	if len(pending) > 0 {
		report.Action = "aborted_pending_intents"
	}
	if needRebuild {
		report.Action = "aborted_pending_intents_and_atomic_derived_rebuild"
	}
	report.Notes = notes
	payload, _ := json.Marshal(report)
	res, err := tx.ExecContext(ctx, `INSERT INTO recovery_reports(started_at_ms,finished_at_ms,report_json) VALUES(?,?,?)`, report.StartedAtMS, report.FinishedAtMS, string(payload))
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	report.ID = id
	payload, _ = json.Marshal(report)
	if _, err := tx.ExecContext(ctx, `UPDATE recovery_reports SET report_json=? WHERE id=?`, string(payload), id); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	a.recovery = report
	return report, nil
}

func (a *App) LatestRecovery(ctx context.Context) (*model.RecoveryReport, error) {
	var payload string
	err := a.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT 1`).Scan(&payload)
	if err != nil {
		return nil, err
	}
	var report model.RecoveryReport
	if err := json.Unmarshal([]byte(payload), &report); err != nil {
		return nil, err
	}
	return &report, nil
}
