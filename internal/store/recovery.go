package store

import (
	"encoding/json"
	"fmt"
	"time"
)

type RecoveryReport struct {
	RecoveryID            string            `json:"recovery_id"`
	SessionID             string            `json:"session_id,omitempty"`
	RecoveredAtMS         int64             `json:"recovered_at_ms"`
	PendingIntentCount    int               `json:"pending_intent_count_before"`
	AbortedIntentIDs      []string          `json:"aborted_intent_ids"`
	TasksResetToPending   int64             `json:"tasks_reset_to_pending"`
	LastCompleteVersion   int64             `json:"last_complete_version"`
	RecoveredViewVersion  int64             `json:"recovered_view_version"`
	SessionStatus         string            `json:"session_status"`
	TasksByStatus         map[string]int    `json:"tasks_by_status"`
	Summaries             []HostSummaryView `json:"host_summaries"`
	FinalPendingIntents   int               `json:"final_pending_intents"`
	IntegrityCheck        string            `json:"integrity_check"`
	Consistent            bool              `json:"consistent"`
	ConsistencyViolations []string          `json:"consistency_violations,omitempty"`
	Notes                 []string          `json:"notes"`
}

type HostSummaryView struct {
	Host           string `json:"host"`
	ViewVersion    int64  `json:"view_version"`
	Total          int    `json:"total"`
	Done           int    `json:"done"`
	Conformant     int    `json:"conformant"`
	Drift          int    `json:"drift"`
	Deferred       int    `json:"deferred"`
	Errors         int    `json:"errors"`
	MaxDriftLevel  int    `json:"max_drift_level"`
	RemainingQuota int    `json:"remaining_quota"`
}

func (s *Store) Recover(sessionID string) (*RecoveryReport, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	recoveryID := fmt.Sprintf("recovery-%d", time.Now().UnixNano())
	report := &RecoveryReport{
		RecoveryID:    recoveryID,
		SessionID:     sessionID,
		RecoveredAtMS: time.Now().UnixMilli(),
		TasksByStatus: map[string]int{},
		Notes:         []string{},
	}
	recoveryIntentID, err := s.CreateIntent(sessionID, "recovery_run", 0, map[string]any{"recovery_id": recoveryID})
	if err != nil {
		return nil, err
	}
	pending, err := s.ListPendingIntents()
	if err != nil {
		return nil, err
	}
	for _, intent := range pending {
		if intent.IntentID == recoveryIntentID {
			continue
		}
		if sessionID != "" && intent.SessionID != sessionID {
			continue
		}
		report.PendingIntentCount++
		session, err := s.GetSession(intent.SessionID)
		if err != nil {
			return nil, err
		}
		if session == nil {
			_, err := s.writer.Exec(`UPDATE intents SET status='ABORTED', recovery_note='session missing; unfinished intent aborted from durable fact', updated_at_ms=? WHERE intent_id=?`, time.Now().UnixMilli(), intent.IntentID)
			if err != nil {
				return nil, err
			}
			report.AbortedIntentIDs = append(report.AbortedIntentIDs, intent.IntentID)
			report.Notes = append(report.Notes, "aborted intent "+intent.IntentID+" without session")
			continue
		}

		tx, err := s.writer.Begin()
		if err != nil {
			return nil, err
		}
		now := time.Now().UnixMilli()
		_, err = tx.Exec(`UPDATE intents SET status='ABORTED', recovery_note=?, updated_at_ms=? WHERE intent_id=?`,
			"force termination or incomplete write; rolled session to last complete version", now, intent.IntentID)
		if err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		result, err := tx.Exec(`UPDATE audit_tasks SET status='PENDING', reason='recovered_from_interrupted_run', updated_at_ms=? WHERE session_id=? AND status='RUNNING'`, now, intent.SessionID)
		if err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		reset, _ := result.RowsAffected()
		report.TasksResetToPending += reset

		targetVersion := session.LastCompleteVersion
		status := "RECOVERED"
		if targetVersion <= 0 {
			status = "ABORTED"
		}
		_, err = tx.Exec(`UPDATE sessions SET current_version=?, last_complete_version=?, view_version=?, status=?, updated_at_ms=? WHERE session_id=?`,
			targetVersion, targetVersion, targetVersion, status, now, intent.SessionID)
		if err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		hosts, err := listHostsTx(tx, intent.SessionID)
		if err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		for _, host := range hosts {
			if _, err := recomputeHostSummaryTx(tx, intent.SessionID, host, targetVersion); err != nil {
				_ = tx.Rollback()
				return nil, err
			}
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		report.AbortedIntentIDs = append(report.AbortedIntentIDs, intent.IntentID)
		sessionID = intent.SessionID
		report.SessionID = intent.SessionID
		report.LastCompleteVersion = targetVersion
		report.RecoveredViewVersion = targetVersion
		report.SessionStatus = status
	}

	if sessionID != "" {
		if report.SessionStatus == "" {
			session, err := s.GetSession(sessionID)
			if err != nil {
				return nil, err
			}
			if session != nil {
				report.LastCompleteVersion = session.LastCompleteVersion
				report.RecoveredViewVersion = session.ViewVersion
				report.SessionStatus = session.Status
			}
		}
		rows, err := s.reader.Query(`SELECT status, COUNT(*) FROM audit_tasks WHERE session_id=? GROUP BY status`, sessionID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var status string
			var count int
			if err := rows.Scan(&status, &count); err != nil {
				_ = rows.Close()
				return nil, err
			}
			report.TasksByStatus[status] = count
		}
		_ = rows.Close()

		summaries, err := s.ListHostSummaries(sessionID)
		if err != nil {
			return nil, err
		}
		for _, summary := range summaries {
			report.Summaries = append(report.Summaries, HostSummaryView{
				Host: summary.Host, ViewVersion: summary.ViewVersion, Total: summary.Total, Done: summary.Done,
				Conformant: summary.Conformant, Drift: summary.Drift, Deferred: summary.Deferred, Errors: summary.Errors,
				MaxDriftLevel: summary.MaxDriftLevel, RemainingQuota: summary.RemainingQuota,
			})
		}
		report.ConsistencyViolations = s.verifySummaryConsistency(sessionID)
	}

	if err := s.reader.QueryRow(`PRAGMA integrity_check`).Scan(&report.IntegrityCheck); err != nil {
		return nil, err
	}
	countQuery := `SELECT COUNT(*) FROM intents WHERE status='PENDING' AND intent_id <> ?`
	countArgs := []any{recoveryIntentID}
	if sessionID != "" {
		countQuery += ` AND session_id=?`
		countArgs = append(countArgs, sessionID)
	}
	if err := s.reader.QueryRow(countQuery, countArgs...).Scan(&report.FinalPendingIntents); err != nil {
		return nil, err
	}
	report.Consistent = report.IntegrityCheck == "ok" && report.FinalPendingIntents == 0 && len(report.ConsistencyViolations) == 0

	payload, _ := json.Marshal(report)
	tx, err := s.writer.Begin()
	if err != nil {
		return nil, err
	}
	if err := markIntentCommitted(tx, recoveryIntentID); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO recovery_reports(recovery_id,session_id,report_json,created_at_ms) VALUES(?,?,?,?)`,
		report.RecoveryID, report.SessionID, string(payload), report.RecoveredAtMS); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return report, nil
}

func (s *Store) verifySummaryConsistency(sessionID string) []string {
	violations := make([]string, 0)
	summaries, err := s.ListHostSummaries(sessionID)
	if err != nil {
		return []string{err.Error()}
	}
	for _, summary := range summaries {
		var total, done, conformant, drift, deferred, errors int
		err := s.reader.QueryRow(`SELECT COUNT(*),
			SUM(CASE WHEN status='DONE' THEN 1 ELSE 0 END),
			SUM(CASE WHEN status='DONE' AND drift_level=0 THEN 1 ELSE 0 END),
			SUM(CASE WHEN status='DONE' AND drift_level>0 THEN 1 ELSE 0 END),
			SUM(CASE WHEN status='DEFERRED' THEN 1 ELSE 0 END),
			SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END)
			FROM conclusions WHERE session_id=? AND host=?`, sessionID, summary.Host).
			Scan(&total, &done, &conformant, &drift, &deferred, &errors)
		if err != nil {
			violations = append(violations, summary.Host+": "+err.Error())
			continue
		}
		if total != summary.Total || done != summary.Done || conformant != summary.Conformant || drift != summary.Drift || deferred != summary.Deferred || errors != summary.Errors {
			violations = append(violations, fmt.Sprintf("%s: summary (%d,%d,%d,%d,%d,%d) != conclusions (%d,%d,%d,%d,%d,%d)",
				summary.Host, summary.Total, summary.Done, summary.Conformant, summary.Drift, summary.Deferred, summary.Errors,
				total, done, conformant, drift, deferred, errors))
		}
	}
	return violations
}

type CrashWindow struct {
	IntentID string
	tx       interface{ Rollback() error }
}

func (w *CrashWindow) Rollback() error {
	if w.tx == nil {
		return nil
	}
	return w.tx.Rollback()
}

func (s *Store) BeginCrashWindow(sessionID string) (*CrashWindow, error) {
	session, err := s.GetSession(sessionID)
	if err != nil {
		return nil, err
	}
	target := int64(1)
	if session != nil {
		target = session.CurrentVersion + 1
	}
	intentID, err := s.CreateIntent(sessionID, "snapshot_increment", target, map[string]any{"crash_demo": true})
	if err != nil {
		return nil, err
	}
	tx, err := s.writer.Begin()
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(`UPDATE host_summaries SET drift=999, summary_json='{"partial":"uncommitted"}' WHERE session_id=?`, sessionID)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return &CrashWindow{IntentID: intentID, tx: tx}, nil
}

func (s *Store) CreatePropagationIntent(sessionID string, targetVersion int64) (string, error) {
	return s.CreateIntent(sessionID, "propagation", targetVersion, map[string]any{"target_version": targetVersion})
}
