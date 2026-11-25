package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// Recover scans durable unfinished intents after a forced termination. It only
// reports rows that actually existed on disk. Old committed snapshots and
// derived revisions are never deleted by recovery.
func (e *Engine) Recover(ctx context.Context) (RecoveryReport, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	e.stateMu.Lock()
	defer e.stateMu.Unlock()

	report := RecoveryReport{RecoveredAt: nowText()}
	err := writeTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		meta, err := readCurrentMeta(ctx, tx)
		if err != nil {
			return err
		}
		report.CurrentVersion = meta.Version
		report.CurrentDerivedRevision = meta.DerivedRevision

		rows, err := tx.QueryContext(ctx, `SELECT id,kind,request_id,round_id,derived_revision,target_version
			FROM intents WHERE status='RUNNING' ORDER BY created_at`)
		if err != nil {
			return err
		}
		type intentFact struct {
			id, kind, requestID, roundID string
			revision, version            int64
		}
		var intents []intentFact
		for rows.Next() {
			var in intentFact
			if err := rows.Scan(&in.id, &in.kind, &in.requestID, &in.roundID, &in.revision, &in.version); err != nil {
				rows.Close()
				return err
			}
			intents = append(intents, in)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		report.ScannedIntents = len(intents)

		for _, in := range intents {
			now := nowText()
			if in.roundID != "" || in.kind == "probe" {
				res, err := tx.ExecContext(ctx, `UPDATE probe_results SET status='PENDING',committed=0,
					ok=0,code='',latency_ms=0,observed_at='',detail=''
					WHERE round_id=? AND status!='COMMITTED'`, in.roundID)
				if err != nil {
					return err
				}
				n, _ := res.RowsAffected()
				report.ResetProbeResults += int(n)
				if _, err := tx.ExecContext(ctx, `UPDATE probe_rounds SET status='ABORTED',completed_at=?,
					error_code='E_RECOVERED_ABORTED' WHERE round_id=? AND status!='COMPLETE'`, now, in.roundID); err != nil {
					return err
				}
			}
			if in.requestID != "" {
				if _, err := tx.ExecContext(ctx, `UPDATE requests SET status='ABORTED',error_code='E_RECOVERED_ABORTED',
					updated_at=? WHERE request_id=? AND status='RUNNING'`, now, in.requestID); err != nil {
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='ABORTED',updated_at=? WHERE id=?`, now, in.id); err != nil {
				return err
			}
			report.AbortedIntents++
			report.Details = append(report.Details, fmt.Sprintf("aborted intent %s kind=%s version=%d revision=%d",
				in.id, in.kind, in.version, in.revision))
		}

		// Remove every staged derived revision and every orphan revision newer
		// than the active pointer. This restores the derived graph to the last
		// complete version even if an intent row itself was not yet committed.
		rows, err = tx.QueryContext(ctx, `SELECT revision FROM derived_revisions WHERE status='STAGING' OR revision>?`, meta.DerivedRevision)
		if err != nil {
			return err
		}
		var revisions []int64
		for rows.Next() {
			var revision int64
			if err := rows.Scan(&revision); err != nil {
				rows.Close()
				return err
			}
			revisions = append(revisions, revision)
		}
		rows.Close()
		for _, revision := range revisions {
			if _, err := tx.ExecContext(ctx, `DELETE FROM health_summary WHERE revision=?`, revision); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM weight_allocations WHERE revision=?`, revision); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM probe_plans WHERE revision=?`, revision); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM derived_revisions WHERE revision=?`, revision); err != nil {
				return err
			}
			report.DiscardedDerivedRevisions++
			report.Details = append(report.Details, fmt.Sprintf("discarded incomplete derived revision %d", revision))
		}

		rows, err = tx.QueryContext(ctx, `SELECT version FROM snapshots WHERE status='STAGING' OR version>?`, meta.Version)
		if err != nil {
			return err
		}
		var versions []int64
		for rows.Next() {
			var version int64
			if err := rows.Scan(&version); err != nil {
				rows.Close()
				return err
			}
			versions = append(versions, version)
		}
		rows.Close()
		for _, version := range versions {
			if _, err := tx.ExecContext(ctx, `DELETE FROM snapshot_instances WHERE snapshot_version=?`, version); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM snapshots WHERE version=?`, version); err != nil {
				return err
			}
			report.DiscardedStagedSnapshots++
			report.Details = append(report.Details, fmt.Sprintf("discarded uncommitted snapshot version %d", version))
		}

		// Defensive pointer audit. Two-phase activation normally makes these
		// no-ops, but they guarantee a complete pointer even after external
		// SQLite changes or a torn legacy database.
		var snapshotStatus string
		err = tx.QueryRowContext(ctx, `SELECT status FROM snapshots WHERE version=?`, meta.Version).Scan(&snapshotStatus)
		if err == sql.ErrNoRows {
			if meta.Version != 0 {
				report.Details = append(report.Details, "active snapshot pointer had no row; kept prior active instances")
			}
		} else if err != nil {
			return err
		} else if snapshotStatus != "COMPLETE" {
			var fallback int64
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM snapshots WHERE status='COMPLETE'`).Scan(&fallback); err != nil {
				return err
			}
			if err := setMeta(ctx, tx, "current_version", fallback); err != nil {
				return err
			}
			report.CurrentVersion = fallback
			report.Details = append(report.Details, fmt.Sprintf("active snapshot was %s; pointer moved to complete version %d", snapshotStatus, fallback))
		}

		var derivedStatus string
		err = tx.QueryRowContext(ctx, `SELECT status FROM derived_revisions WHERE revision=?`, meta.DerivedRevision).Scan(&derivedStatus)
		if err == sql.ErrNoRows {
			if meta.DerivedRevision != 0 {
				report.Details = append(report.Details, "active derived pointer had no row; kept active instances")
			}
		} else if err != nil {
			return err
		} else if derivedStatus != "COMPLETE" {
			var fallback int64
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(revision),0) FROM derived_revisions WHERE status='COMPLETE'`).Scan(&fallback); err != nil {
				return err
			}
			if err := setMeta(ctx, tx, "current_derived_revision", fallback); err != nil {
				return err
			}
			report.CurrentDerivedRevision = fallback
			report.Details = append(report.Details, fmt.Sprintf("active derived revision was %s; pointer moved to complete revision %d", derivedStatus, fallback))
		}

		raw, _ := json.Marshal(report)
		_, err = tx.ExecContext(ctx, `INSERT INTO recovery_reports(recovered_at,report_json,created_at) VALUES(?,?,?)`,
			report.RecoveredAt, string(raw), nowText())
		return err
	})
	if err != nil {
		return report, err
	}
	return report, nil
}

// RecoveryReport returns the most recently persisted recovery verdict.
func (e *Engine) LastRecoveryReport(ctx context.Context) (RecoveryReport, *Error) {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	var raw string
	err := readTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT 1`).Scan(&raw)
	})
	if err == sql.ErrNoRows {
		return RecoveryReport{RecoveredAt: ""}, nil
	}
	if err != nil {
		return RecoveryReport{}, newError(ErrInternal, "read recovery report failed", "detail", err.Error())
	}
	var report RecoveryReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return report, newError(ErrInternal, "decode recovery report failed", "detail", err.Error())
	}
	return report, nil
}

// RecoverySummary is a compact diagnostic view used by the CLI.
type RecoverySummary struct {
	Report            RecoveryReport `json:"report"`
	LatestRoundID     string         `json:"latestRoundId,omitempty"`
	DatabaseCheckedAt string         `json:"databaseCheckedAt"`
}

// RecoveryStatus combines the latest report with a live commit-pointer read.
func (e *Engine) RecoveryStatus(ctx context.Context) (RecoverySummary, *Error) {
	report, opErr := e.LastRecoveryReport(ctx)
	if opErr != nil {
		return RecoverySummary{}, opErr
	}
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	var latest string
	err := readTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT value FROM meta WHERE key='latest_round_id'),'')`).Scan(&latest)
	})
	if err != nil {
		return RecoverySummary{}, newError(ErrInternal, "read recovery status failed", "detail", err.Error())
	}
	return RecoverySummary{Report: report, LatestRoundID: latest, DatabaseCheckedAt: time.Now().UTC().Format(time.RFC3339Nano)}, nil
}
