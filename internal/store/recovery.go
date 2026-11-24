package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"

	"driftconverge/internal/apperr"
)

type RecoveryAction struct {
	IntentID string `json:"intentId"`
	Kind     string `json:"kind"`
	RoundID  string `json:"roundId,omitempty"`
	ObjectID string `json:"objectId,omitempty"`
	Observed string `json:"observedDurableState"`
	Action   string `json:"action"`
	Result   string `json:"result"`
	Detail   string `json:"detail,omitempty"`
}

type RecoveryReport struct {
	ScannedIntents           int              `json:"scannedIntents"`
	CommittedIntents         int              `json:"committedIntents"`
	RolledBackIntents        int              `json:"rolledBackIntents"`
	PendingWrites            int              `json:"pendingWrites"`
	AbortedBatchVersions     int              `json:"abortedBatchVersions"`
	AbortedRounds            int              `json:"abortedRounds"`
	LastCompleteBatchVersion int64            `json:"lastCompleteBatchVersion,omitempty"`
	Actions                  []RecoveryAction `json:"actions"`
}

func (s *Store) Recover(ctx context.Context) (*RecoveryReport, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	report := &RecoveryReport{Actions: []RecoveryAction{}}
	rows, err := s.db.QueryContext(ctx, `SELECT intent_id,kind,round_id,object_id,payload FROM write_intents WHERE state='pending' ORDER BY created_at,intent_id`)
	if err != nil {
		return nil, apperr.New(apperr.PersistenceFailed, "scan pending intents: %v", err)
	}
	type pendingIntent struct {
		id, kind, roundID, objectID, payload string
	}
	pending := []pendingIntent{}
	for rows.Next() {
		var p pendingIntent
		if err := rows.Scan(&p.id, &p.kind, &p.roundID, &p.objectID, &p.payload); err != nil {
			rows.Close()
			return nil, apperr.New(apperr.PersistenceFailed, "scan pending intent row: %v", err)
		}
		pending = append(pending, p)
	}
	if err := rows.Close(); err != nil {
		return nil, apperr.New(apperr.PersistenceFailed, "close pending intent rows: %v", err)
	}
	if err := rows.Err(); err != nil {
		return nil, apperr.New(apperr.PersistenceFailed, "iterate pending intents: %v", err)
	}
	report.ScannedIntents = len(pending)
	for _, intent := range pending {
		action := RecoveryAction{IntentID: intent.id, Kind: intent.kind, RoundID: intent.roundID, ObjectID: intent.objectID}
		switch intent.kind {
		case "SNAPSHOT_COMMIT":
			version := snapshotVersionFromPayload(intent.payload)
			exists, err := s.policyExists(ctx, version)
			if err != nil {
				return nil, err
			}
			if exists {
				action.Observed = "policy_snapshot_present"
				action.Action = "mark_committed"
				action.Result = "consistent"
				if err := s.markIntent(ctx, intent.id, "committed"); err != nil {
					return nil, err
				}
				_ = s.logIntent(ctx, intent.id, "recovery_committed", "snapshot row observed on disk")
				report.CommittedIntents++
			} else {
				action.Observed = "policy_snapshot_absent"
				action.Action = "rollback"
				action.Result = "no_durable_snapshot"
				if err := s.markIntent(ctx, intent.id, "rolled_back"); err != nil {
					return nil, err
				}
				_ = s.logIntent(ctx, intent.id, "recovery_rolled_back", "snapshot data was not durably committed")
				report.RolledBackIntents++
			}
		case "BATCH_COMMIT":
			var versionID int64
			var state string
			err := s.db.QueryRowContext(ctx, `SELECT id,state FROM batch_versions WHERE intent_id=?`, intent.id).Scan(&versionID, &state)
			if errors.Is(err, sql.ErrNoRows) {
				action.Observed = "batch_version_absent"
				action.Action = "rollback"
				action.Result = "no_durable_batch_version"
				if err := s.markIntent(ctx, intent.id, "rolled_back"); err != nil {
					return nil, err
				}
				_ = s.logIntent(ctx, intent.id, "recovery_rolled_back", "batch version row absent")
				report.RolledBackIntents++
				break
			}
			if err != nil {
				return nil, apperr.New(apperr.PersistenceFailed, "scan batch version for intent %s: %v", intent.id, err)
			}
			if state == "complete" {
				action.Observed = "batch_version_complete"
				action.Action = "mark_committed"
				action.Result = "consistent"
				if err := s.markIntent(ctx, intent.id, "committed"); err != nil {
					return nil, err
				}
				_ = s.logIntent(ctx, intent.id, "recovery_committed", "complete batch version observed")
				report.CommittedIntents++
			} else {
				action.Observed = "batch_version_" + state
				action.Action = "mark_aborted_and_rollback"
				action.Result = "incomplete_batch_removed_from_schedulable_set"
				if _, err := s.db.ExecContext(ctx, `UPDATE batch_versions SET state='aborted',completed_at=? WHERE id=?`, nowText(), versionID); err != nil {
					return nil, apperr.New(apperr.PersistenceFailed, "abort staged batch version %d: %v", versionID, err)
				}
				if err := s.markIntent(ctx, intent.id, "rolled_back"); err != nil {
					return nil, err
				}
				_ = s.logIntent(ctx, intent.id, "recovery_aborted_batch", "staging batch version marked aborted")
				report.RolledBackIntents++
				report.AbortedBatchVersions++
			}
		case "WRITE_COMMIT":
			var recordState string
			err := s.db.QueryRowContext(ctx, `SELECT state FROM write_records WHERE intent_id=?`, intent.id).Scan(&recordState)
			if errors.Is(err, sql.ErrNoRows) {
				action.Observed = "write_record_absent"
				action.Action = "rollback"
				action.Result = "pending_writeback_without_record"
				if err := s.markIntent(ctx, intent.id, "rolled_back"); err != nil {
					return nil, err
				}
				_ = s.logIntent(ctx, intent.id, "recovery_pending_writeback", "manifest write record absent; intent rolled back")
				report.RolledBackIntents++
				report.PendingWrites++
				break
			}
			if err != nil {
				return nil, apperr.New(apperr.PersistenceFailed, "scan write record for intent %s: %v", intent.id, err)
			}
			if recordState == "converged" || recordState == "unresolved" || recordState == "failed" {
				action.Observed = "write_record_" + recordState
				action.Action = "mark_committed"
				action.Result = "terminal_record_observed"
				if err := s.markIntent(ctx, intent.id, "committed"); err != nil {
					return nil, err
				}
				_ = s.logIntent(ctx, intent.id, "recovery_committed", "terminal write record observed")
				report.CommittedIntents++
			} else {
				action.Observed = "write_record_" + recordState
				action.Action = "rollback_to_pending_writeback"
				action.Result = "write_not_proven_committed"
				if _, err := s.db.ExecContext(ctx, `UPDATE write_records SET state='pending_writeback',updated_at=? WHERE intent_id=?`, nowText(), intent.id); err != nil {
					return nil, apperr.New(apperr.PersistenceFailed, "mark write record pending_writeback: %v", err)
				}
				if err := s.markIntent(ctx, intent.id, "rolled_back"); err != nil {
					return nil, err
				}
				_ = s.logIntent(ctx, intent.id, "recovery_pending_writeback", "uncommitted write marked pending_writeback")
				report.RolledBackIntents++
				report.PendingWrites++
			}
		default:
			action.Observed = "unknown_intent_kind"
			action.Action = "rollback"
			action.Result = "unsupported_intent"
			if err := s.markIntent(ctx, intent.id, "rolled_back"); err != nil {
				return nil, err
			}
			_ = s.logIntent(ctx, intent.id, "recovery_rolled_back", "unknown intent kind")
			report.RolledBackIntents++
		}
		report.Actions = append(report.Actions, action)
	}

	rows, err = s.db.QueryContext(ctx, `SELECT round_id FROM rounds WHERE state='running' ORDER BY round_id`)
	if err != nil {
		return nil, apperr.New(apperr.PersistenceFailed, "scan interrupted rounds: %v", err)
	}
	roundIDs := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, apperr.New(apperr.PersistenceFailed, "scan interrupted round: %v", err)
		}
		roundIDs = append(roundIDs, id)
	}
	_ = rows.Close()
	for _, id := range roundIDs {
		payload, _ := json.Marshal(map[string]any{
			"roundId": id, "state": "aborted", "reason": "process interruption recovered from durable running state",
		})
		if _, err := s.db.ExecContext(ctx, `UPDATE rounds SET state='aborted',error_code=?,result_json=?,updated_at=? WHERE round_id=? AND state='running'`,
			string(apperr.RecoveredInterruption), string(payload), nowText(), id); err != nil {
			return nil, apperr.New(apperr.PersistenceFailed, "abort recovered round %s: %v", id, err)
		}
		report.AbortedRounds++
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id),0) FROM batch_versions WHERE state='complete'`).Scan(&report.LastCompleteBatchVersion); err != nil {
		return nil, apperr.New(apperr.PersistenceFailed, "read last complete batch version: %v", err)
	}
	sort.Slice(report.Actions, func(i, j int) bool { return report.Actions[i].IntentID < report.Actions[j].IntentID })
	return report, nil
}

func (s *Store) policyExists(ctx context.Context, version int64) (bool, error) {
	if version <= 0 {
		return false, nil
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM policy_snapshots WHERE version=?`, version).Scan(&count); err != nil {
		return false, apperr.New(apperr.PersistenceFailed, "check snapshot presence: %v", err)
	}
	return count > 0, nil
}

func snapshotVersionFromPayload(payload string) int64 {
	var value struct {
		SnapshotVersion int64 `json:"snapshotVersion"`
	}
	_ = json.Unmarshal([]byte(payload), &value)
	return value.SnapshotVersion
}

func (s *Store) PendingIntentCount(ctx context.Context) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM write_intents WHERE state='pending'`).Scan(&count); err != nil {
		return 0, apperr.New(apperr.PersistenceFailed, "count pending intents: %v", err)
	}
	return count, nil
}
