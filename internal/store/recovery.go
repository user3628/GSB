package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// recover runs at startup while no listener or resolver is active. It turns
// every durably pending intent into either an aborted record or a replay from
// facts that are already visible in SQLite. It never guesses state from memory
// because a killed process has no trustworthy in-memory state.
func (s *Store) recover(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	started := s.nowMS()
	report := RecoveryReport{
		StartedAtMS:   started,
		FactsOnlyNote: "The report is derived solely from rows committed in SQLite after reopening the database.",
		Limitations: []string{
			"Upstream queries or cache writes that were still in memory at SIGKILL time are not reconstructable.",
			"A cache write whose transaction did not commit never became visible; only its durable pending intent is reported.",
			"A zone batch whose transaction did not fully commit is aborted; an already durable zone version is preserved and replayed from SQLite.",
			"SQLite cannot protect against filesystem or storage corruption outside its own commit guarantees.",
		},
	}
	pendingBatches := map[string]bool{}
	rows, err := s.db.QueryContext(ctx, `SELECT intent_id, kind, target FROM intention_log WHERE status='pending' ORDER BY created_ms, intent_id`)
	if err != nil {
		return err
	}
	type intentRow struct{ id, kind, target string }
	var intents []intentRow
	for rows.Next() {
		var r intentRow
		if err := rows.Scan(&r.id, &r.kind, &r.target); err != nil {
			rows.Close()
			return err
		}
		intents = append(intents, r)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	report.PendingIntents = len(intents)
	for _, intent := range intents {
		switch intent.kind {
		case "cache_write":
			res, err := s.db.ExecContext(ctx, `DELETE FROM cache_entries WHERE intent_id=?`, intent.id)
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			report.CacheEntriesRolledBack += int(n)
			if err := s.markIntent(ctx, intent.id, "aborted", "cache write transaction was not committed; cache row rolled back"); err != nil {
				return err
			}
			report.Details = append(report.Details, fmt.Sprintf("cache intent %s aborted; rolled back %d cache row(s)", intent.id, n))
		case "snapshot", "delta", "zone_snapshot", "zone_delta":
			pendingBatches[intent.target] = true
			replayed, err := s.replayBatch(ctx, intent.id, intent.target, &report)
			if err != nil {
				return err
			}
			if !replayed {
				if err := s.abortRecoveredBatch(ctx, intent.id, intent.target, &report); err != nil {
					return err
				}
			}
		default:
			if err := s.markIntent(ctx, intent.id, "aborted", "unknown pending intent kind"); err != nil {
				return err
			}
			report.Details = append(report.Details, fmt.Sprintf("unknown intent %s (%s) aborted", intent.id, intent.kind))
		}
	}

	rows, err = s.db.QueryContext(ctx, `SELECT batch_id FROM zone_batches WHERE status='pending' ORDER BY created_ms, batch_id`)
	if err != nil {
		return err
	}
	var extraBatches []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		if !pendingBatches[id] {
			extraBatches = append(extraBatches, id)
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, batchID := range extraBatches {
		intentID := ""
		_ = s.db.QueryRowContext(ctx, `SELECT intent_id FROM intention_log WHERE target=? AND status='pending' LIMIT 1`, batchID).Scan(&intentID)
		replayed, err := s.replayBatch(ctx, intentID, batchID, &report)
		if err != nil {
			return err
		}
		if !replayed {
			if err := s.abortRecoveredBatch(ctx, intentID, batchID, &report); err != nil {
				return err
			}
		}
	}

	report.FinishedAtMS = s.nowMS()
	b, err := json.Marshal(report)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO recovery_reports(started_ms, finished_ms, report_json) VALUES(?,?,?)`,
		report.StartedAtMS, report.FinishedAtMS, string(b)); err != nil {
		return err
	}
	s.recovery = report
	return nil
}

func (s *Store) replayBatch(ctx context.Context, intentID, batchID string, report *RecoveryReport) (bool, error) {
	var zone, kind, checksum, status string
	var targetVersion int64
	err := s.db.QueryRowContext(ctx, `SELECT zone, kind, target_version, checksum, status FROM zone_batches WHERE batch_id=?`, batchID).
		Scan(&zone, &kind, &targetVersion, &checksum, &status)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if status == "committed" || status == "committed_replayed" {
		if intentID != "" {
			_ = s.markIntent(ctx, intentID, "committed", "zone batch was already durably committed")
		}
		return true, nil
	}
	var zoneVersion int64
	var zoneChecksum string
	err = s.db.QueryRowContext(ctx, `SELECT version, checksum FROM zones WHERE zone=?`, zone).Scan(&zoneVersion, &zoneChecksum)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	if zoneVersion < targetVersion || !strings.EqualFold(zoneChecksum, checksum) {
		return false, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	now := s.nowMS()
	invalidated, err := invalidateZoneCachesTx(ctx, tx, zone)
	if err != nil {
		return false, err
	}
	derived, err := rebuildDerivedTx(ctx, tx, zone, zoneVersion)
	if err != nil {
		return false, err
	}
	if err := bumpZoneEpochTx(ctx, tx); err != nil {
		return false, err
	}
	result := ZoneCommitResult{
		BatchID: batchID, Zone: zone, Kind: kind, Version: zoneVersion,
		InvalidatedCacheEntries: invalidated, DerivedRows: derived,
	}
	resultJSON, _ := json.Marshal(result)
	if _, err := tx.ExecContext(ctx, `UPDATE zone_batches SET status='committed_replayed', result_json=?, message='replayed from durable zone facts', updated_ms=? WHERE batch_id=?`,
		string(resultJSON), now, batchID); err != nil {
		return false, err
	}
	if intentID != "" {
		if err := markIntentTx(ctx, tx, intentID, "replayed", "zone batch replayed from durable SQLite facts", now); err != nil {
			return false, err
		}
	}
	idemKind := "zone_snapshot"
	if kind == "delta" || kind == "zone_delta" {
		idemKind = "zone_delta"
	}
	var hash string
	_ = tx.QueryRowContext(ctx, `SELECT checksum FROM zone_batches WHERE batch_id=?`, batchID).Scan(&hash)
	if err := saveIdempotency(ctx, tx, idemKind, batchID, hash, "completed", "", "", result, now); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	report.ZoneBatchesReplayed++
	report.Details = append(report.Details, fmt.Sprintf("zone batch %s (%s/%s) replayed at durable version %d; invalidated %d cache row(s)", batchID, zone, kind, zoneVersion, invalidated))
	return true, nil
}

func (s *Store) abortRecoveredBatch(ctx context.Context, intentID, batchID string, report *RecoveryReport) error {
	now := s.nowMS()
	res, err := s.db.ExecContext(ctx, `UPDATE zone_batches SET status='aborted', code='E_INTERRUPTED', message='process terminated before batch transaction committed', updated_ms=? WHERE batch_id=? AND status='pending'`, now, batchID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		report.ZoneBatchesAborted++
		report.Details = append(report.Details, fmt.Sprintf("zone batch %s aborted; no durable target version was found", batchID))
	} else {
		report.Details = append(report.Details, fmt.Sprintf("zone batch %s was not pending", batchID))
	}
	if intentID != "" {
		if err := s.markIntent(ctx, intentID, "aborted", "batch did not have complete durable zone facts"); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (RecoveryReport, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT 1`).Scan(&raw)
	if err == sql.ErrNoRows {
		return RecoveryReport{}, nil
	}
	if err != nil {
		return RecoveryReport{}, err
	}
	var report RecoveryReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return RecoveryReport{}, err
	}
	return report, nil
}
