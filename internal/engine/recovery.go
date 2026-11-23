package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"

	"local/configmiddleware/internal/core"
	"local/configmiddleware/internal/store"
)

type preparedIntent struct {
	ID          string         `json:"intent_id"`
	Kind        string         `json:"kind"`
	AggregateID string         `json:"aggregate_id"`
	Payload     map[string]any `json:"payload,omitempty"`
	CreatedAt   string         `json:"created_at"`
}

func (e *Engine) RecoverStartup(ctx context.Context) (core.RecoveryReport, error) {
	started := core.Now()
	report := core.RecoveryReport{
		ReportID:  core.NewID("recovery"),
		StartedAt: started,
		Facts:     map[string]any{"db_path": e.store.Path()},
		Actions:   []map[string]any{},
	}
	_, err := e.store.IntentWriteWithID(ctx, "RECOVERY_SCAN", "recovery:"+report.ReportID, report, func(tx *sql.Tx, recoveryIntentID string) (bool, error) {
		report.Facts["recovery_intent_id"] = recoveryIntentID
		var latest int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM snapshots`).Scan(&latest); err != nil {
			return false, err
		}
		report.Facts["max_committed_snapshot"] = latest

		intents, err := preparedIntentsTx(ctx, tx, recoveryIntentID)
		if err != nil {
			return false, err
		}
		report.ScannedIntents = len(intents)
		report.Facts["prepared_intents"] = intents

		openBatches, err := openBatchesTx(ctx, tx)
		if err != nil {
			return false, err
		}
		report.Facts["open_batches"] = openBatches

		affected := map[string]string{}
		affectedBatches := map[string]bool{}
		for _, batchID := range openBatches {
			affectedBatches[batchID] = true
			rows, err := tx.QueryContext(ctx, `SELECT push_id,status FROM pushes WHERE batch_id=?`, batchID)
			if err != nil {
				return false, err
			}
			for rows.Next() {
				var pushID, status string
				if err := rows.Scan(&pushID, &status); err != nil {
					rows.Close()
					return false, err
				}
				affected[pushID] = "batch was open during restart"
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return false, err
			}
			rows.Close()
		}
		for _, in := range intents {
			pushID := intentPushID(in)
			if pushID == "" {
				continue
			}
			affected[pushID] = "prepared intent " + in.ID + " (" + in.Kind + ")"
			if push, err := getPushTx(ctx, tx, pushID); err == nil {
				affectedBatches[push.BatchID] = true
			} else if !errors.Is(err, sql.ErrNoRows) {
				return false, err
			}
		}

		pushIDs := make([]string, 0, len(affected))
		for pushID := range affected {
			pushIDs = append(pushIDs, pushID)
		}
		sort.Strings(pushIDs)
		for _, pushID := range pushIDs {
			push, err := getPushTx(ctx, tx, pushID)
			if errors.Is(err, sql.ErrNoRows) {
				report.Actions = append(report.Actions, map[string]any{
					"type": "push_missing", "push_id": pushID, "reason": affected[pushID],
				})
				continue
			}
			if err != nil {
				return false, err
			}
			if push.Status == core.StatusApplied || push.Status == core.StatusAborted {
				report.Actions = append(report.Actions, map[string]any{
					"type": "push_unchanged", "push_id": pushID, "status": push.Status, "reason": affected[pushID],
				})
				continue
			}
			now := core.Now()
			if _, err := tx.ExecContext(ctx, `UPDATE pushes SET status='ABORTED',error_code='E_RECOVERED_ABORT',error_message=?,error_context_json=?,updated_at=? WHERE push_id=?`,
				"restart recovered an incomplete operation", marshal(map[string]any{"reason": affected[pushID]}), now, pushID); err != nil {
				return false, err
			}
			if err := insertTransitionTx(ctx, tx, pushID, push.Status, core.StatusAborted, "startup recovery: "+affected[pushID], now); err != nil {
				return false, err
			}
			if push.AttemptIntentID != "" {
				_, _ = tx.ExecContext(ctx, `UPDATE intents SET status='RECOVERED',finished_at=?,detail_json=? WHERE intent_id=? AND status='PREPARED'`,
					now, marshal(map[string]any{"action": "push_aborted", "reason": affected[pushID]}), push.AttemptIntentID)
			}
			report.Actions = append(report.Actions, map[string]any{
				"type": "push_aborted", "push_id": pushID, "from": push.Status, "to": core.StatusAborted,
				"version": push.Version, "client_id": push.ClientID, "reason": affected[pushID],
			})
		}

		batchIDs := make([]string, 0, len(affectedBatches))
		for batchID := range affectedBatches {
			batchIDs = append(batchIDs, batchID)
		}
		sort.Strings(batchIDs)
		for _, batchID := range batchIDs {
			var batchStatus string
			if err := tx.QueryRowContext(ctx, `SELECT status FROM batches WHERE batch_id=?`, batchID).Scan(&batchStatus); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				return false, err
			}
			if batchStatus == "ABORTED" {
				continue
			}
			var total, applied int
			pushRows, err := tx.QueryContext(ctx, `SELECT status FROM pushes WHERE batch_id=?`, batchID)
			if err != nil {
				return false, err
			}
			for pushRows.Next() {
				var pushStatus string
				if err := pushRows.Scan(&pushStatus); err != nil {
					pushRows.Close()
					return false, err
				}
				total++
				if pushStatus == core.StatusApplied {
					applied++
				}
			}
			pushRows.Close()
			if total > 0 && applied == total {
				now := core.Now()
				if _, err := tx.ExecContext(ctx, `UPDATE batches SET status='COMPLETED',finished_at=?,error_code='' WHERE batch_id=?`, now, batchID); err != nil {
					return false, err
				}
				report.Actions = append(report.Actions, map[string]any{
					"type": "batch_completed", "batch_id": batchID, "from": batchStatus, "to": "COMPLETED",
					"reason": "all persisted pushes are APPLIED",
				})
				continue
			}
			now := core.Now()
			if _, err := tx.ExecContext(ctx, `UPDATE batches SET status='ABORTED',finished_at=?,error_code='E_RECOVERED_ABORT' WHERE batch_id=?`, now, batchID); err != nil {
				return false, err
			}
			report.Actions = append(report.Actions, map[string]any{
				"type": "batch_aborted", "batch_id": batchID, "from": batchStatus, "to": "ABORTED", "reason": "restart recovery",
			})
		}

		now := core.Now()
		for _, in := range intents {
			_, err := tx.ExecContext(ctx, `UPDATE intents SET status='RECOVERED',finished_at=?,detail_json=? WHERE intent_id=? AND status='PREPARED'`,
				now, marshal(map[string]any{"action": "startup_recovery", "kind": in.Kind, "aggregate_id": in.AggregateID}), in.ID)
			if err != nil {
				return false, err
			}
			report.Actions = append(report.Actions, map[string]any{
				"type": "intent_recovered", "intent_id": in.ID, "kind": in.Kind, "aggregate_id": in.AggregateID,
			})
		}

		clients, err := restoredClientsTx(ctx, tx)
		if err != nil {
			return false, err
		}
		report.RestoredClients = clients
		report.Facts["restored_client_count"] = len(clients)
		report.FinishedAt = now
		reportJSON, err := json.Marshal(report)
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO recovery_reports(report_id,started_at,finished_at,report_json) VALUES(?,?,?,?)`,
			report.ReportID, report.StartedAt, report.FinishedAt, string(reportJSON))
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return core.RecoveryReport{}, err
	}
	e.recovery = report
	return report, nil
}

func preparedIntentsTx(ctx context.Context, tx *sql.Tx, excludeIntentID string) ([]preparedIntent, error) {
	rows, err := tx.QueryContext(ctx, `SELECT intent_id,kind,aggregate_id,payload_json,created_at FROM intents WHERE status='PREPARED' AND intent_id<>? ORDER BY created_at`, excludeIntentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []preparedIntent
	for rows.Next() {
		var in preparedIntent
		var payloadRaw string
		if err := rows.Scan(&in.ID, &in.Kind, &in.AggregateID, &payloadRaw, &in.CreatedAt); err != nil {
			return nil, err
		}
		in.Payload = store.UnmarshalMap(payloadRaw)
		result = append(result, in)
	}
	return result, rows.Err()
}

func openBatchesTx(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT batch_id FROM batches WHERE status IN ('OPEN','DISPATCHING') ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func intentPushID(in preparedIntent) string {
	if in.AggregateID != "" && (in.Kind == "PUSH_ATTEMPT" || in.Kind == "PUSH_DISPATCH" || in.Kind == "PUSH_ABORT" || in.Kind == "ACK_INGEST") {
		return in.AggregateID
	}
	if value, ok := in.Payload["push_id"].(string); ok {
		return value
	}
	return ""
}

func restoredClientsTx(ctx context.Context, tx *sql.Tx) ([]core.ClientVersion, error) {
	rows, err := tx.QueryContext(ctx, `SELECT c.client_id, COALESCE(MAX(CASE WHEN p.status='APPLIED' THEN p.version END),0) FROM clients c LEFT JOIN pushes p ON p.client_id=c.client_id GROUP BY c.client_id ORDER BY c.client_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []core.ClientVersion
	for rows.Next() {
		var cv core.ClientVersion
		if err := rows.Scan(&cv.ClientID, &cv.Version); err != nil {
			return nil, err
		}
		if cv.Version == 0 {
			cv.Status = "NO_APPLIED_VERSION"
		} else {
			cv.Status = core.StatusApplied
		}
		result = append(result, cv)
	}
	return result, rows.Err()
}
