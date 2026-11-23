package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gatewayfreshness.local/workbench/internal/freshness"
)

// Recover scans durable pending intents. An intent is the commit fence: if no
// committed batch exists, a pending intent cannot expose a decision because all
// decision, item, nonce, and audit writes live in one SQLite transaction. Any
// legacy/orphan rows carrying that exact planned version and batch id are
// removed defensively, and the batch is marked aborted in the intent ledger.
func (s *Store) Recover(ctx context.Context) (*freshness.RecoveryReport, error) {
	s.recoverMu.Lock()
	defer s.recoverMu.Unlock()
	if s.recovered {
		report, err := s.LatestRecovery(ctx)
		if err != nil {
			return nil, err
		}
		return report, nil
	}
	started := time.Now().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin recovery: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `SELECT id,batch_id,planned_window_version,content_hash,mode FROM intents WHERE state='pending' ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("scan pending intents: %w", err)
	}
	type pending struct {
		id      int64
		batchID string
		version int64
		hash    string
		mode    string
	}
	var pendingIntents []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.batchID, &p.version, &p.hash, &p.mode); err != nil {
			_ = rows.Close()
			return nil, err
		}
		pendingIntents = append(pendingIntents, p)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	report := &freshness.RecoveryReport{
		StartedAtMS: started, PendingIntents: len(pendingIntents), FactsOnly: true,
		Details: make([]freshness.RecoveryFact, 0, len(pendingIntents)),
	}
	for _, p := range pendingIntents {
		var decisionVersion int64
		err := tx.QueryRowContext(ctx, `SELECT window_version FROM batches WHERE batch_id=?`, p.batchID).Scan(&decisionVersion)
		decisionPresent := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("check committed decision for intent %d: %w", p.id, err)
		}
		fact := freshness.RecoveryFact{
			IntentID: p.id, BatchID: p.batchID, PlannedVersion: p.version, DecisionPresent: decisionPresent,
		}
		if decisionPresent && decisionVersion == p.version {
			fact.Action = "confirmed_committed"
			report.ConfirmedCommitted++
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='committed',updated_at_ms=? WHERE id=? AND state='pending'`,
				time.Now().UnixMilli(), p.id); err != nil {
				return nil, err
			}
		} else {
			fact.Action = "aborted_pending_intent"
			var nonceRows, eventRows, auditRows int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM nonces WHERE first_batch_id=? AND window_version=?`, p.batchID, p.version).Scan(&nonceRows); err != nil {
				return nil, err
			}
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM nonce_events WHERE batch_id=? AND window_version=?`, p.batchID, p.version).Scan(&eventRows); err != nil {
				return nil, err
			}
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_trail WHERE batch_id=? AND window_version=?`, p.batchID, p.version).Scan(&auditRows); err != nil {
				return nil, err
			}
			fact.CommitFactsFound = nonceRows+eventRows+auditRows > 0
			if _, err := tx.ExecContext(ctx, `DELETE FROM nonces WHERE first_batch_id=? AND window_version=?`, p.batchID, p.version); err != nil {
				return nil, err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM nonce_events WHERE batch_id=? AND window_version=?`, p.batchID, p.version); err != nil {
				return nil, err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM audit_trail WHERE batch_id=? AND window_version=?`, p.batchID, p.version); err != nil {
				return nil, err
			}
			details, _ := json.Marshal(map[string]any{
				"commit_facts_found":  fact.CommitFactsFound,
				"orphan_nonce_rows":   nonceRows,
				"orphan_nonce_events": eventRows,
				"orphan_audit_rows":   auditRows,
				"reason":              "no committed batch row; SQLite transaction was not committed",
			})
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='aborted',updated_at_ms=?,detail_json=? WHERE id=? AND state='pending'`,
				time.Now().UnixMilli(), string(details), p.id); err != nil {
				return nil, err
			}
			report.Aborted++
		}
		report.Details = append(report.Details, fact)
	}
	report.FinishedAtMS = time.Now().UnixMilli()
	facts, _ := json.Marshal(report.Details)
	if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_reports(started_at_ms,finished_at_ms,pending_intents,confirmed_committed,aborted,facts_json)
		VALUES(?,?,?,?,?,?)`, report.StartedAtMS, report.FinishedAtMS, report.PendingIntents, report.ConfirmedCommitted, report.Aborted, string(facts)); err != nil {
		return nil, fmt.Errorf("insert recovery report: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit recovery: %w", err)
	}
	s.recovered = true
	return report, nil
}

func (s *Store) LatestRecovery(ctx context.Context) (*freshness.RecoveryReport, error) {
	row := s.db.QueryRowContext(ctx, `SELECT started_at_ms,finished_at_ms,pending_intents,confirmed_committed,aborted,facts_json
		FROM recovery_reports ORDER BY id DESC LIMIT 1`)
	var r freshness.RecoveryReport
	var facts string
	if err := row.Scan(&r.StartedAtMS, &r.FinishedAtMS, &r.PendingIntents, &r.ConfirmedCommitted, &r.Aborted, &facts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &freshness.RecoveryReport{Details: []freshness.RecoveryFact{}, FactsOnly: true}, nil
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(facts), &r.Details)
	r.FactsOnly = true
	return &r, nil
}

func (s *Store) RecoveryHistory(ctx context.Context, limit int) ([]freshness.RecoveryReport, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `SELECT started_at_ms,finished_at_ms,pending_intents,confirmed_committed,aborted,facts_json
		FROM recovery_reports ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	reports := make([]freshness.RecoveryReport, 0)
	for rows.Next() {
		var r freshness.RecoveryReport
		var facts string
		if err := rows.Scan(&r.StartedAtMS, &r.FinishedAtMS, &r.PendingIntents, &r.ConfirmedCommitted, &r.Aborted, &facts); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(facts), &r.Details)
		r.FactsOnly = true
		reports = append(reports, r)
	}
	return reports, rows.Err()
}
