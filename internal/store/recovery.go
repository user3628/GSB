package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"replicationlag/internal/apperr"
	"replicationlag/internal/domain"
)

type RecoveryEvent struct {
	IntentID            int64  `json:"intent_id"`
	BatchID             string `json:"batch_id"`
	Operation           string `json:"operation"`
	BaseVersion         int64  `json:"base_version"`
	VersionAtScan       int64  `json:"version_at_scan"`
	LastCompleteVersion int64  `json:"last_complete_version"`
	Action              string `json:"action"`
	Conclusion          string `json:"conclusion"`
	ScannedAtMS         int64  `json:"scanned_at_ms"`
}

type RecoveryReport struct {
	ScannedAtMS         int64           `json:"scanned_at_ms"`
	LastCompleteVersion int64           `json:"last_complete_version"`
	PendingIntentsFound int             `json:"pending_intents_found"`
	Events              []RecoveryEvent `json:"events"`
	Conclusion          string          `json:"conclusion"`
}

type pendingIntent struct {
	id          int64
	batchID     string
	operation   string
	requestHash string
	baseVersion int64
	status      string
}

// RecoverPendingIntents scans durable intents after startup. A pending intent
// means its atomic data transaction did not commit; the current database
// version is therefore the last complete version. The function records an
// abort event, or performs a persisted-snapshot rollback if facts show a later
// version exists without a completed intent.
func (s *Store) RecoverPendingIntents(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := s.now()
	rows, err := s.db.QueryContext(ctx, `SELECT id, batch_id, operation, request_hash, base_version, status
		FROM intents WHERE status='PENDING' ORDER BY id`)
	if err != nil {
		return fmt.Errorf("scan pending intents: %w", err)
	}
	pending := make([]pendingIntent, 0)
	for rows.Next() {
		var intent pendingIntent
		if err := rows.Scan(&intent.id, &intent.batchID, &intent.operation, &intent.requestHash, &intent.baseVersion, &intent.status); err != nil {
			rows.Close()
			return fmt.Errorf("scan pending intent: %w", err)
		}
		pending = append(pending, intent)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close pending intent rows: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin recovery: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM recovery_events`); err != nil {
		return fmt.Errorf("clear prior recovery report: %w", err)
	}
	report := RecoveryReport{ScannedAtMS: now, Events: []RecoveryEvent{}}
	for _, intent := range pending {
		versionAtScan, err := versionFrom(ctx, tx)
		if err != nil {
			return err
		}
		action := "ABORT_PENDING_INTENT"
		conclusion := "no partial observation commit exists; batch marked aborted at last complete version"
		batchStatus, batchFound, err := batchStatusTx(ctx, tx, intent.batchID)
		if err != nil {
			return err
		}
		if batchFound && batchStatus == "COMPLETE" {
			action = "FINALIZE_COMPLETED_BATCH"
			conclusion = "batch data was already committed; intent finalized from persisted batch fact"
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='COMPLETED', finished_ms=?, detail_json=? WHERE id=?`,
				now, `{"outcome":"recovered_completed"}`, intent.id); err != nil {
				return fmt.Errorf("finalize recovered intent: %w", err)
			}
		} else {
			if versionAtScan > intent.baseVersion {
				action = "ROLLBACK_TO_LAST_COMPLETE"
				conclusion = "later version lacked a completed intent; restored last complete persisted snapshot"
				if err := rollbackSnapshotTx(ctx, tx, intent.baseVersion); err != nil {
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='ABORTED', target_version=?, finished_ms=?, detail_json=? WHERE id=?`,
				intent.baseVersion, now, `{"outcome":"recovered_aborted"}`, intent.id); err != nil {
				return fmt.Errorf("abort recovered intent: %w", err)
			}
			if intent.batchID != "" {
				body, _ := json.Marshal(apperr.New(apperr.CodeBatchAborted, "batch was aborted during recovery", map[string]any{
					"batch_id":              intent.batchID,
					"last_complete_version": intent.baseVersion,
				}).Public())
				if batchFound {
					if _, err := tx.ExecContext(ctx, `UPDATE batches SET status='ABORTED', http_status=?, response_json=?, version=?, finished_ms=? WHERE batch_id=?`,
						apperr.HTTPStatus(apperr.CodeBatchAborted), string(body), intent.baseVersion, now, intent.batchID); err != nil {
						return fmt.Errorf("abort recovered batch: %w", err)
					}
				} else {
					if _, err := tx.ExecContext(ctx, `INSERT INTO batches
						(batch_id, request_hash, operation, status, http_status, response_json, version, created_ms, finished_ms)
						VALUES (?, ?, ?, 'ABORTED', ?, ?, ?, ?, ?)`, intent.batchID, intent.requestHash, intent.operation,
						apperr.HTTPStatus(apperr.CodeBatchAborted), string(body), intent.baseVersion, now, now); err != nil {
						return fmt.Errorf("record aborted recovered batch: %w", err)
					}
				}
			}
		}
		lastComplete, err := versionFrom(ctx, tx)
		if err != nil {
			return err
		}
		event := RecoveryEvent{
			IntentID: intent.id, BatchID: intent.batchID, Operation: intent.operation,
			BaseVersion: intent.baseVersion, VersionAtScan: versionAtScan,
			LastCompleteVersion: lastComplete, Action: action, Conclusion: conclusion, ScannedAtMS: now,
		}
		report.Events = append(report.Events, event)
		if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_events
			(intent_id, batch_id, operation, base_version, version_at_scan, last_complete_version, action, conclusion, scanned_ms)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, event.IntentID, event.BatchID, event.Operation, event.BaseVersion,
			event.VersionAtScan, event.LastCompleteVersion, event.Action, event.Conclusion, event.ScannedAtMS); err != nil {
			return fmt.Errorf("record recovery event: %w", err)
		}
	}
	report.PendingIntentsFound = len(pending)
	lastComplete, err := versionFrom(ctx, tx)
	if err != nil {
		return err
	}
	report.LastCompleteVersion = lastComplete
	if len(pending) == 0 {
		report.Conclusion = "no pending intents; no recovery action required"
	} else {
		report.Conclusion = "all pending intents were resolved against persisted facts"
	}
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("encode recovery report: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO meta(key, value) VALUES ('recovery_report', ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, string(reportJSON)); err != nil {
		return fmt.Errorf("persist recovery report: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit recovery report: %w", err)
	}
	return nil
}

func batchStatusTx(ctx context.Context, tx *sql.Tx, batchID string) (string, bool, error) {
	if batchID == "" {
		return "", false, nil
	}
	var status string
	err := tx.QueryRowContext(ctx, `SELECT status FROM batches WHERE batch_id=?`, batchID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read batch during recovery: %w", err)
	}
	return status, true, nil
}

func rollbackSnapshotTx(ctx context.Context, tx *sql.Tx, version int64) error {
	snapshot := SnapshotData{Observations: []domain.Observation{}, Report: domain.MergeReport{Sequences: []domain.EvolutionSequence{}}}
	if version > 0 {
		var raw string
		if err := tx.QueryRowContext(ctx, `SELECT snapshot_json FROM version_snapshots WHERE version=?`, version).Scan(&raw); err != nil {
			return fmt.Errorf("load recovery snapshot version %d: %w", version, err)
		}
		if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
			return fmt.Errorf("decode recovery snapshot version %d: %w", version, err)
		}
	}
	if err := replaceObservations(ctx, tx, snapshot.Observations); err != nil {
		return err
	}
	if err := persistReport(ctx, tx, snapshot.Report); err != nil {
		return err
	}
	if err := setVersion(ctx, tx, version); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM version_snapshots WHERE version>?`, version); err != nil {
		return fmt.Errorf("remove rolled-back snapshots: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM commits WHERE version>?`, version); err != nil {
		return fmt.Errorf("remove rolled-back commit records: %w", err)
	}
	return nil
}

func (s *Store) RecoveryReport(ctx context.Context) (RecoveryReport, int64, error) {
	version, err := s.Version(ctx)
	if err != nil {
		return RecoveryReport{}, 0, err
	}
	var raw string
	err = s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='recovery_report'`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return RecoveryReport{LastCompleteVersion: version, Events: []RecoveryEvent{}, Conclusion: "no recovery scan has been recorded"}, version, nil
	}
	if err != nil {
		return RecoveryReport{}, 0, fmt.Errorf("read recovery report: %w", err)
	}
	var report RecoveryReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return RecoveryReport{}, 0, fmt.Errorf("decode recovery report: %w", err)
	}
	return report, version, nil
}
