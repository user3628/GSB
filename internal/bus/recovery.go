package bus

import (
	"context"
	"database/sql"
	"encoding/json"
)

// Recover reconciles durable intents left by an interrupted process and
// invalidates all delivery leases owned by the previous process. Business
// transactions are atomic, so a transaction that did not commit leaves no
// partial rows; an intent with committed evidence is reported as recovered.
func (s *Service) Recover(ctx context.Context) (RecoveryReport, error) {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()

	started := s.nowMS()
	tx, release, err := s.beginWriteTx(ctx)
	if err != nil {
		return RecoveryReport{}, WrapError(CodeInternal, "begin recovery", err)
	}
	defer release()
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, operation, topic, group_name, partition
		FROM intents WHERE status = 'PENDING' ORDER BY created_at_ms, id`)
	if err != nil {
		return RecoveryReport{}, WrapError(CodeInternal, "scan pending intents", err)
	}
	type pendingIntent struct {
		id, operation, topic, group string
		partition                   int
	}
	var pending []pendingIntent
	for rows.Next() {
		var item pendingIntent
		if err := rows.Scan(&item.id, &item.operation, &item.topic, &item.group, &item.partition); err != nil {
			rows.Close()
			return RecoveryReport{}, WrapError(CodeInternal, "scan pending intent", err)
		}
		pending = append(pending, item)
	}
	if err := rows.Close(); err != nil {
		return RecoveryReport{}, WrapError(CodeInternal, "close pending intents", err)
	}
	if err := rows.Err(); err != nil {
		return RecoveryReport{}, WrapError(CodeInternal, "iterate pending intents", err)
	}

	report := RecoveryReport{
		StartedAtMS:    started,
		PendingScanned: len(pending),
		Intents:        make([]RecoveryIntent, 0, len(pending)),
	}
	for _, item := range pending {
		evidence, err := intentEvidence(ctx, tx, item.id)
		if err != nil {
			return RecoveryReport{}, WrapError(CodeInternal, "inspect pending intent", err)
		}
		outcome := "ROLLED_BACK"
		if evidence != "" {
			outcome = "COMMITTED"
			report.CommittedRecovered++
		} else {
			report.RolledBack++
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE intents SET status = ?, finished_at_ms = ? WHERE id = ? AND status = 'PENDING'`,
			outcome, s.nowMS(), item.id); err != nil {
			return RecoveryReport{}, WrapError(CodeInternal, "reconcile intent", err)
		}
		report.Intents = append(report.Intents, RecoveryIntent{
			ID:        item.id,
			Operation: item.operation,
			Topic:     item.topic,
			Group:     item.group,
			Partition: item.partition,
			Outcome:   outcome,
			Evidence:  evidence,
		})
	}

	now := s.nowMS()
	resetResult, err := tx.ExecContext(ctx, `
		UPDATE event_states
		SET state = 'REDELIVERED', lease_token = '', updated_at_ms = ?, transition_reason = 'PROCESS_RESTART'
		WHERE state = 'DISPATCHED'`, now)
	if err != nil {
		return RecoveryReport{}, WrapError(CodeInternal, "reset interrupted deliveries", err)
	}
	report.DeliveriesReset, _ = resetResult.RowsAffected()
	leaseResult, err := tx.ExecContext(ctx, `
		UPDATE consumer_offsets
		SET lease_token = '', lease_owner = '', lease_expires_at_ms = 0, updated_at_ms = ?
		WHERE lease_token <> '' OR lease_owner <> ''`, now)
	if err != nil {
		return RecoveryReport{}, WrapError(CodeInternal, "release interrupted leases", err)
	}
	report.LeasesReleased, _ = leaseResult.RowsAffected()

	if _, err := tx.ExecContext(ctx, `
		UPDATE idempotency SET status = 'ABANDONED', updated_at_ms = ?
		WHERE status = 'RUNNING'`, now); err != nil {
		return RecoveryReport{}, WrapError(CodeInternal, "abandon interrupted request records", err)
	}

	report.FinishedAtMS = s.nowMS()
	details, err := json.Marshal(report.Intents)
	if err != nil {
		return RecoveryReport{}, WrapError(CodeInternal, "encode recovery details", err)
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO recovery_runs(started_at_ms, finished_at_ms, pending_scanned, committed_recovered,
			rolled_back, leases_released, deliveries_reset, details_json)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		report.StartedAtMS, report.FinishedAtMS, report.PendingScanned, report.CommittedRecovered,
		report.RolledBack, report.LeasesReleased, report.DeliveriesReset, string(details))
	if err != nil {
		return RecoveryReport{}, WrapError(CodeInternal, "record recovery run", err)
	}
	report.RunID, _ = result.LastInsertId()
	if err := tx.Commit(); err != nil {
		return RecoveryReport{}, WrapError(CodeInternal, "commit recovery", err)
	}
	release()
	return report, nil
}

func (s *Service) LatestRecoveryReport(ctx context.Context) (RecoveryReport, error) {
	var report RecoveryReport
	var details string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, started_at_ms, finished_at_ms, pending_scanned, committed_recovered,
			rolled_back, leases_released, deliveries_reset, details_json
		FROM recovery_runs ORDER BY id DESC LIMIT 1`).
		Scan(&report.RunID, &report.StartedAtMS, &report.FinishedAtMS, &report.PendingScanned,
			&report.CommittedRecovered, &report.RolledBack, &report.LeasesReleased,
			&report.DeliveriesReset, &details)
	if err == sql.ErrNoRows {
		return RecoveryReport{}, NewError(CodeNotFound, "no recovery run has been recorded")
	}
	if err != nil {
		return RecoveryReport{}, WrapError(CodeInternal, "read recovery report", err)
	}
	if err := json.Unmarshal([]byte(details), &report.Intents); err != nil {
		return RecoveryReport{}, WrapError(CodeInternal, "decode recovery details", err)
	}
	return report, nil
}
