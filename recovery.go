package feedbackstream

import (
	"context"
	"encoding/json"
	"fmt"
)

func (s *Store) Recover(ctx context.Context) (*RecoveryReport, error) {
	pending, err := s.ListPendingIntents(ctx)
	if err != nil {
		return nil, err
	}
	active, err := s.ActivePolicy(ctx)
	if err != nil {
		return nil, err
	}
	report := &RecoveryReport{
		DBPath: s.dbPath, ScannedPendingIntents: len(pending),
		RolledBackBatches: []string{}, DiscardedWindowIntents: []string{},
		AbortedPolicySwitches: []string{}, AbortedBackpressure: []string{},
		Actions: []RecoveryAction{},
		Notice:  "report contains only durable facts; recovery does not claim that an interrupted window can be completed automatically",
	}
	if active != nil {
		report.ActivePolicyVersion = active.Snapshot.Version
	}

	for _, intent := range pending {
		action := RecoveryAction{IntentID: intent.ID, Kind: intent.Kind, AggregateID: intent.AggregateID}
		switch intent.Kind {
		case IntentStartBatch:
			batchID := intent.AggregateID
			res, err := s.db.ExecContext(ctx, `UPDATE batches SET status=?, updated_at_ms=? WHERE batch_id=? AND status=?`,
				StatusQueued, nowMS(), batchID, StatusRunning)
			if err != nil {
				return nil, err
			}
			_, _ = s.db.ExecContext(ctx, `UPDATE batch_requests SET status=? WHERE batch_id=? AND status=?`,
				StatusQueued, batchID, StatusRunning)
			rows, _ := res.RowsAffected()
			report.RolledBackBatches = append(report.RolledBackBatches, batchID)
			action.Action = "ROLLBACK_UNCOMMITTED_BATCH_TO_QUEUED"
			action.Detail = fmt.Sprintf("reset_running_batch_rows=%d", rows)
			if err := s.finishRecoveryIntent(ctx, intent.ID, IntentRecovered, action.Action); err != nil {
				return nil, err
			}

		case IntentWindowConclusion:
			_, err := s.db.ExecContext(ctx, `DELETE FROM window_results WHERE committed_intent_id=? AND status='DRAFT'`, intent.ID)
			if err != nil {
				return nil, err
			}
			report.DiscardedWindowIntents = append(report.DiscardedWindowIntents, intent.ID)
			action.Action = "DISCARD_UNCOMMITTED_WINDOW_CONCLUSION_TRANSACTION"
			action.Detail = "SQLite transaction rollback removed uncommitted rows before restart"
			if err := s.finishRecoveryIntent(ctx, intent.ID, IntentDiscarded, action.Action); err != nil {
				return nil, err
			}

		case IntentSwitchPolicy:
			var payload struct {
				NewVersion int64 `json:"new_version"`
			}
			_ = json.Unmarshal([]byte(intent.PayloadJSON), &payload)
			if payload.NewVersion > 0 {
				_, err := s.db.ExecContext(ctx, `UPDATE policies SET status='ABORTED' WHERE version=? AND status='STAGED'`, payload.NewVersion)
				if err != nil {
					return nil, err
				}
			}
			report.AbortedPolicySwitches = append(report.AbortedPolicySwitches, intent.ID)
			action.Action = "ABORT_POLICY_SWITCH_OLD_POLICY_RETAINED"
			action.Detail = fmt.Sprintf("new_version=%d active_version=%d", payload.NewVersion, report.ActivePolicyVersion)
			if err := s.finishRecoveryIntent(ctx, intent.ID, IntentAborted, action.Action); err != nil {
				return nil, err
			}

		case IntentBackpressure:
			report.AbortedBackpressure = append(report.AbortedBackpressure, intent.ID)
			action.Action = "ABORT_UNCOMMITTED_BACKPRESSURE_TRANSITION"
			if err := s.finishRecoveryIntent(ctx, intent.ID, IntentAborted, action.Action); err != nil {
				return nil, err
			}

		default:
			action.Action = "MARK_PENDING_INTENT_RECOVERED"
			if err := s.finishRecoveryIntent(ctx, intent.ID, IntentRecovered, action.Action); err != nil {
				return nil, err
			}
		}
		report.Actions = append(report.Actions, action)
	}
	if err := s.SaveRecoveryReport(ctx, *report); err != nil {
		return nil, err
	}
	return report, nil
}

func (s *Store) finishRecoveryIntent(ctx context.Context, id, status, action string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET status=?, recovery_action=?, updated_at_ms=? WHERE intent_id=?`,
		status, action, nowMS(), id)
	return err
}

func (s *Store) ListRecoveryReports(ctx context.Context, limit int) ([]RecoveryReport, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RecoveryReport{}
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var report RecoveryReport
		if err := json.Unmarshal([]byte(data), &report); err != nil {
			return nil, err
		}
		out = append(out, report)
	}
	return out, rows.Err()
}
