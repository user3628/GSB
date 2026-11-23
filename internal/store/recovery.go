package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"localinfer/workbench/internal/domain"
)

func (s *Store) Recover() (*domain.RecoveryReport, error) {
	started, err := s.loadStartedIntents()
	if err != nil {
		return nil, err
	}
	organizingRuns, err := s.loadOrganizingRuns()
	if err != nil {
		return nil, err
	}
	pendingBatches, err := s.loadOrganizingBatches()
	if err != nil {
		return nil, err
	}
	preserved, err := s.loadPreservedBatches()
	if err != nil {
		return nil, err
	}
	report := &domain.RecoveryReport{
		ReportID: fmt.Sprintf("rr-%d", time.Now().UTC().UnixNano()), CreatedAt: now(), StartedIntents: started,
		AbortedRuns: organizingRuns, AbortedBatches: pendingBatches, PreservedBatches: preserved,
		PersistedFacts: []string{}, Clean: len(started) == 0 && len(organizingRuns) == 0,
	}
	for _, i := range started {
		report.PersistedFacts = append(report.PersistedFacts, fmt.Sprintf("intent id=%d kind=%s state=started run=%s batch=%s", i.ID, i.Kind, i.RunID, i.BatchID))
	}
	for _, id := range organizingRuns {
		report.PersistedFacts = append(report.PersistedFacts, fmt.Sprintf("run id=%s state=organizing", id))
	}
	for _, id := range pendingBatches {
		report.PersistedFacts = append(report.PersistedFacts, fmt.Sprintf("batch id=%s state=quantile_pending_or_aborted_after_scan", id))
	}
	for _, id := range preserved {
		report.PersistedFacts = append(report.PersistedFacts, fmt.Sprintf("batch id=%s state=complete_preserved", id))
	}
	if report.Clean {
		report.CompletionClaim = "NO_INTERRUPTED_OPERATION"
		report.Conclusion = "clean startup; no unfinished intent or organizing run was found"
	} else {
		report.CompletionClaim = "NO_COMPLETION_CLAIM_FOR_INTERRUPTED_RUN"
		report.Conclusion = "recovery marked unfinished intents and organizing runs aborted; only previously committed complete batch facts are preserved"
	}
	payload, _ := json.Marshal(report)
	err = s.withIntent("RECOVERY_APPLY", "", "", "", map[string]any{
		"report_id": report.ReportID, "started_intents": len(started), "aborted_runs": len(organizingRuns),
		"aborted_batches": len(pendingBatches), "preserved_batches": len(preserved),
	}, func(tx *sql.Tx) error {
		if len(started) > 0 {
			if _, err := tx.Exec(`UPDATE intents SET state='aborted',finished_at=?,error=? WHERE state='started'`, now(), "recovered after process interruption"); err != nil {
				return err
			}
		}
		if len(organizingRuns) > 0 {
			if _, err := tx.Exec(`UPDATE batches SET status='aborted' WHERE status IN ('planned','organizing','quantile_pending')
				AND run_id IN (SELECT run_id FROM runs WHERE status='organizing')`); err != nil {
				return err
			}
			if _, err := tx.Exec(`UPDATE runs SET status='aborted',failure_code='E_INTERRUPTED',completed_at=? WHERE status='organizing'`, now()); err != nil {
				return err
			}
		}
		_, err := tx.Exec(`INSERT INTO recovery_reports(report_id,created_at,clean,payload_json) VALUES(?,?,?,?)`,
			report.ReportID, report.CreatedAt, boolToInt(report.Clean), string(payload))
		return err
	})
	if err != nil {
		return nil, err
	}
	return report, nil
}

func (s *Store) loadStartedIntents() ([]domain.IntentFact, error) {
	rows, err := s.db.Query(`SELECT id,kind,trace_id,run_id,batch_id,state,created_at,detail_json FROM intents WHERE state='started' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.IntentFact, 0)
	for rows.Next() {
		var f domain.IntentFact
		if err := rows.Scan(&f.ID, &f.Kind, &f.TraceID, &f.RunID, &f.BatchID, &f.State, &f.CreatedAt, &f.Detail); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (s *Store) loadOrganizingRuns() ([]string, error) {
	rows, err := s.db.Query(`SELECT run_id FROM runs WHERE status='organizing' ORDER BY created_at,run_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) loadOrganizingBatches() ([]string, error) {
	rows, err := s.db.Query(`SELECT b.batch_id FROM batches b JOIN runs r ON r.run_id=b.run_id
		WHERE r.status='organizing' AND b.status IN ('planned','organizing','quantile_pending') ORDER BY b.run_id,b.batch_seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) loadPreservedBatches() ([]string, error) {
	rows, err := s.db.Query(`SELECT b.batch_id FROM batches b JOIN runs r ON r.run_id=b.run_id
		WHERE r.status='organizing' AND b.status IN ('complete','precision_failed','latency_failed') ORDER BY b.run_id,b.batch_seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) LatestRecoveryReport() (*domain.RecoveryReport, error) {
	var payload []byte
	err := s.db.QueryRow(`SELECT payload_json FROM recovery_reports ORDER BY created_at DESC,report_id DESC LIMIT 1`).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.NewBizError(domain.CodeNotFound, "recovery report not found", nil)
	}
	if err != nil {
		return nil, err
	}
	var report domain.RecoveryReport
	if err := json.Unmarshal(payload, &report); err != nil {
		return nil, err
	}
	return &report, nil
}

func (s *Store) ListRecoveryReports() ([]domain.RecoveryReport, error) {
	rows, err := s.db.Query(`SELECT payload_json FROM recovery_reports ORDER BY created_at DESC,report_id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.RecoveryReport, 0)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var report domain.RecoveryReport
		if err := json.Unmarshal(payload, &report); err != nil {
			return nil, err
		}
		out = append(out, report)
	}
	return out, rows.Err()
}
