package blockpool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const recoveryNote = "Generated at the next successful open from committed SQLite rows only. A killed process cannot be assumed to have produced an exit report."

// recoverOnOpen scans pending intents, aborts their uncommitted requests,
// materializes persisted lease expiry, and rebuilds every persisted reference
// count from the canonical refs table. Every mismatch is recorded before the
// corrected count is committed.
func (p *Pool) recoverOnOpen(ctx context.Context) error {
	tx, err := p.beginWrite(ctx)
	if err != nil {
		return fmt.Errorf("begin recovery: %w", err)
	}
	defer rollback(tx)
	now := p.nowMS()

	var pending int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM intents WHERE state='pending'`).Scan(&pending); err != nil {
		return fmt.Errorf("count pending intents: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='aborted', finished_at_ms=?,
		detail='process stopped before mutation transaction committed' WHERE state='pending'`, now); err != nil {
		return fmt.Errorf("abort pending intents: %w", err)
	}
	res, err := tx.ExecContext(ctx, `UPDATE requests SET status='aborted', completed_at_ms=?
		WHERE status='in_progress'`, now)
	if err != nil {
		return fmt.Errorf("abort in-progress requests: %w", err)
	}
	recoveredRequests, _ := res.RowsAffected()

	if _, err := tx.ExecContext(ctx, `UPDATE leases SET state='expired'
		WHERE state='active' AND expires_at_ms <= ?`, now); err != nil {
		return fmt.Errorf("mark expired leases: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM refs WHERE expires_at_ms <= ?`, now); err != nil {
		return fmt.Errorf("delete expired refs: %w", err)
	}

	type blockCount struct {
		digest    string
		persisted int64
		state     string
		effective int64
	}
	rows, err := tx.QueryContext(ctx, `SELECT b.digest, b.ref_count, b.state,
		(SELECT COUNT(*) FROM refs r WHERE r.digest=b.digest) AS effective
		FROM blocks b ORDER BY b.digest`)
	if err != nil {
		return fmt.Errorf("scan block counts: %w", err)
	}
	var counts []blockCount
	for rows.Next() {
		var c blockCount
		if err := rows.Scan(&c.digest, &c.persisted, &c.state, &c.effective); err != nil {
			rows.Close()
			return fmt.Errorf("scan block count: %w", err)
		}
		counts = append(counts, c)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close block count rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate block counts: %w", err)
	}

	reportRes, err := tx.ExecContext(ctx, `INSERT INTO recovery_reports
		(created_at_ms, pending_intents, aborted_intents, recovered_requests, diff_count, note)
		VALUES (?, ?, ?, ?, 0, ?)`, now, pending, pending, recoveredRequests, recoveryNote)
	if err != nil {
		return fmt.Errorf("create recovery report: %w", err)
	}
	reportID, err := reportRes.LastInsertId()
	if err != nil {
		return fmt.Errorf("read recovery report id: %w", err)
	}

	var diffCount int64
	for _, c := range counts {
		detail := ""
		if c.persisted != c.effective {
			detail = "persisted ref_count differs from committed refs rows"
		}
		if c.state == BlockReclaimed && c.effective != 0 {
			if detail != "" {
				detail += "; "
			}
			detail += "reclaimed block still has committed refs"
		}
		if detail != "" {
			if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_diffs
				(report_id, digest, persisted_ref_count, rebuilt_ref_count, effective_lease_count, detail)
				VALUES (?, ?, ?, ?, ?, ?)`, reportID, c.digest, c.persisted, c.effective, c.effective, detail); err != nil {
				return fmt.Errorf("record recovery diff: %w", err)
			}
			diffCount++
		}
		if _, err := tx.ExecContext(ctx, `UPDATE blocks SET ref_count=? WHERE digest=?`, c.effective, c.digest); err != nil {
			return fmt.Errorf("rebuild ref count: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE recovery_reports SET diff_count=? WHERE id=?`, diffCount, reportID); err != nil {
		return fmt.Errorf("update recovery diff count: %w", err)
	}

	// A committed reference always has an active lease row in the normal
	// engine. Flag and repair the persisted lease state if an externally
	// damaged database breaks that invariant.
	if _, err := tx.ExecContext(ctx, `UPDATE leases SET state='active'
		WHERE EXISTS (SELECT 1 FROM refs r WHERE r.lease_id=leases.id) AND state<>'active'`); err != nil {
		return fmt.Errorf("repair active lease markers: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE leases SET state='expired'
		WHERE state='active' AND NOT EXISTS (SELECT 1 FROM refs r WHERE r.lease_id=leases.id)
		  AND expires_at_ms <= ?`, now); err != nil {
		return fmt.Errorf("repair orphaned active leases: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE leases SET state='unmounted'
		WHERE state='active' AND NOT EXISTS (SELECT 1 FROM refs r WHERE r.lease_id=leases.id)`); err != nil {
		return fmt.Errorf("repair orphaned unexpired leases: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM batch_locks`); err != nil {
		return fmt.Errorf("clear abandoned batch locks: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit recovery: %w", err)
	}
	return nil
}

// RecoveryReport returns the most recent open-time report. It does not scan or
// claim to be able to report on a process that is still running or was killed
// before the next successful open.
func (p *Pool) RecoveryReport(ctx context.Context) (RecoveryReport, *Error) {
	tx, err := p.beginRead(ctx)
	if err != nil {
		return RecoveryReport{}, Errorf(CodeInternal, "begin recovery report read", "cause", err.Error())
	}
	defer rollback(tx)

	report := RecoveryReport{Diffs: make([]RecoveryDiff, 0)}
	err = tx.QueryRowContext(ctx, `SELECT id, created_at_ms, pending_intents, aborted_intents,
		recovered_requests, diff_count, note FROM recovery_reports ORDER BY id DESC LIMIT 1`).
		Scan(&report.ID, &report.CreatedAtMS, &report.PendingIntents, &report.AbortedIntents,
			&report.RecoveredRequests, &report.DiffCount, &report.Note)
	if errors.Is(err, sql.ErrNoRows) {
		return report, nil
	}
	if err != nil {
		return report, Errorf(CodeInternal, "read recovery report", "cause", err.Error())
	}
	rows, err := tx.QueryContext(ctx, `SELECT digest, persisted_ref_count, rebuilt_ref_count,
		effective_lease_count, detail FROM recovery_diffs WHERE report_id=? ORDER BY digest`, report.ID)
	if err != nil {
		return report, Errorf(CodeInternal, "read recovery diffs", "cause", err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var d RecoveryDiff
		if err := rows.Scan(&d.Digest, &d.PersistedRefCount, &d.RebuiltRefCount, &d.EffectiveLeaseCount, &d.Detail); err != nil {
			return report, Errorf(CodeInternal, "scan recovery diff", "cause", err.Error())
		}
		report.Diffs = append(report.Diffs, d)
	}
	if err := rows.Err(); err != nil {
		return report, Errorf(CodeInternal, "iterate recovery diffs", "cause", err.Error())
	}
	return report, nil
}
