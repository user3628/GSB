package store

import (
	"context"
	"encoding/json"

	"mitmpipeline/internal/domain"
)

func (s *Store) Recover(ctx context.Context) (*domain.RecoveryReport, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	startedAt := domain.NowMS()
	report := &domain.RecoveryReport{StartedAtMS: startedAt, Facts: []string{}}

	rows, err := s.db.QueryContext(ctx, `SELECT intent_id,commit_version,kind,status,COALESCE(batch_id,''),COALESCE(payload_json,''),created_at_ms,updated_at_ms
		FROM intents WHERE status IN ('started','committing') ORDER BY commit_version`)
	if err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "scan unfinished intents")
	}
	pending := []domain.IntentRecord{}
	for rows.Next() {
		intent, scanErr := scanIntent(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		pending = append(pending, intent)
	}
	rows.Close()
	report.ScannedIntents = len(pending)

	for _, intent := range pending {
		now := domain.NowMS()
		if _, err := s.db.ExecContext(ctx, `UPDATE intents SET status='aborted', crash_point=?, updated_at_ms=? WHERE intent_id=? AND status IN ('started','committing')`,
			"restart_recovery: payload transaction was not durably committed", now, intent.IntentID); err != nil {
			return nil, domain.WrapError(domain.CodeInternal, err, "abort unfinished intent")
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE commits SET status='ABORTED', completed_at_ms=? WHERE version=? AND status='PENDING'`, now, intent.CommitVersion); err != nil {
			return nil, domain.WrapError(domain.CodeInternal, err, "abort pending commit")
		}
		intent.Status = "aborted"
		intent.UpdatedAtMS = now
		report.AbortedIntents = append(report.AbortedIntents, intent)
		report.Facts = append(report.Facts, "aborted unfinished intent "+intent.IntentID+" for commit "+itoa(intent.CommitVersion))
	}

	version, err := s.LatestCompleteVersion(ctx)
	if err != nil {
		return nil, err
	}
	report.LastCompleteCommit = version

	connections, err := s.ListConnectionIDs(ctx)
	if err != nil {
		return nil, err
	}
	report.VisibleConnectionIDs = connections
	report.Facts = append(report.Facts, "last complete commit is "+itoa(version))
	for _, id := range connections {
		report.Facts = append(report.Facts, "durable connection fact visible: "+id)
	}

	report.FinishedAtMS = domain.NowMS()
	reportJSON, _ := json.Marshal(report)
	if _, err := s.db.ExecContext(ctx, `INSERT INTO recovery_runs(started_at_ms,finished_at_ms,report_json) VALUES(?,?,?)`,
		startedAt, report.FinishedAtMS, string(reportJSON)); err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "persist recovery report")
	}
	return report, nil
}

type intentScanner interface {
	Scan(dest ...any) error
}

func scanIntent(scanner intentScanner) (domain.IntentRecord, error) {
	var intent domain.IntentRecord
	err := scanner.Scan(&intent.IntentID, &intent.CommitVersion, &intent.Kind, &intent.Status,
		&intent.BatchID, &intent.PayloadJSON, &intent.CreatedAtMS, &intent.UpdatedAtMS)
	if err != nil {
		return domain.IntentRecord{}, domain.WrapError(domain.CodeInternal, err, "scan intent")
	}
	return intent, nil
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (*domain.RecoveryReport, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_runs ORDER BY recovery_id DESC LIMIT 1`).Scan(&raw)
	if err != nil {
		return nil, domain.NewError(domain.CodeNotFound, "no recovery report")
	}
	var report domain.RecoveryReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "parse recovery report")
	}
	return &report, nil
}

func itoa(value int64) string {
	return jsonNumber(value)
}

func jsonNumber(value int64) string {
	data, _ := json.Marshal(value)
	return string(data)
}
