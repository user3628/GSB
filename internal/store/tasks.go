package store

import (
	"database/sql"
	"encoding/json"
	"time"

	"auditdrift/internal/model"
)

type TaskCommit struct {
	SessionID    string
	Host         string
	BaselineID   string
	Status       model.TaskStatus
	Conclusion   *model.Conclusion
	ErrorCode    model.ErrorCode
	Reason       string
	RepairNeeded bool
}

func (s *Store) CommitTaskResult(in TaskCommit) (*model.HostSummary, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	intentID, err := s.CreateIntent(in.SessionID, "task_result", 0, in)
	if err != nil {
		return nil, err
	}
	tx, err := s.writer.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	session, err := getSessionTx(tx, in.SessionID)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, model.NewCodedError("E_SESSION_NOT_FOUND", "session %s not found", in.SessionID)
	}
	now := time.Now().UnixMilli()
	_, err = tx.Exec(`UPDATE audit_tasks SET status=?, error_code=?, reason=?, last_intent_id=?, updated_at_ms=?
		WHERE session_id=? AND host=? AND baseline_id=?`,
		in.Status, in.ErrorCode, in.Reason, intentID, now, in.SessionID, in.Host, in.BaselineID)
	if err != nil {
		return nil, err
	}

	conclusion := in.Conclusion
	if conclusion == nil {
		baseline, err := loadBaselineTx(tx, in.SessionID, session.CurrentVersion, in.BaselineID)
		if err != nil {
			return nil, err
		}
		conclusion = &model.Conclusion{
			SessionID: in.SessionID, Host: in.Host, BaselineID: in.BaselineID,
			Category: baseline.Category, Status: in.Status, Reason: in.Reason,
			ErrorCode: in.ErrorCode, RepairCost: baseline.SeverityWeight, ViewVersion: session.ViewVersion,
			Details: map[string]any{},
		}
	}
	conclusion.SessionID = in.SessionID
	conclusion.Host = in.Host
	conclusion.BaselineID = in.BaselineID
	conclusion.ViewVersion = session.ViewVersion
	if conclusion.Status == "" {
		conclusion.Status = in.Status
	}
	if conclusion.ErrorCode == "" {
		conclusion.ErrorCode = in.ErrorCode
	}
	if conclusion.Reason == "" {
		conclusion.Reason = in.Reason
	}

	if _, err := tx.Exec(`DELETE FROM conclusions WHERE session_id=? AND host=? AND baseline_id=?`, in.SessionID, in.Host, in.BaselineID); err != nil {
		return nil, err
	}
	if err := insertConclusionTx(tx, conclusion); err != nil {
		return nil, err
	}

	if in.Status == model.TaskDone && in.RepairNeeded {
		cost := conclusion.RepairCost
		if cost <= 0 {
			cost = 1
		}
		_, err = tx.Exec(`UPDATE sessions SET repair_quota_remaining=repair_quota_remaining-?, updated_at_ms=? WHERE session_id=?`, cost, now, in.SessionID)
		if err != nil {
			return nil, err
		}
		session.RepairQuotaRemaining -= cost
	}

	summary, err := recomputeAllHostSummariesTx(tx, in.SessionID, in.Host, session.ViewVersion)
	if err != nil {
		return nil, err
	}
	if err := markIntentCommitted(tx, intentID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return summary, nil
}

func getSessionTx(tx *sql.Tx, sessionID string) (*Session, error) {
	row := tx.QueryRow(`SELECT session_id,status,current_version,last_complete_version,view_version,propagation_max_hops,repair_quota_total,repair_quota_remaining,updated_at_ms FROM sessions WHERE session_id=?`, sessionID)
	var session Session
	err := row.Scan(&session.SessionID, &session.Status, &session.CurrentVersion, &session.LastCompleteVersion, &session.ViewVersion, &session.PropagationMaxHops, &session.RepairQuotaTotal, &session.RepairQuotaRemaining, &session.UpdatedAtMS)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &session, nil
}

func loadBaselineTx(tx *sql.Tx, sessionID string, version int64, id string) (model.Baseline, error) {
	var b model.Baseline
	var category, expectedJSON, depsJSON string
	err := tx.QueryRow(`SELECT id,category,expected_json,severity_weight,depends_on_json FROM baselines WHERE session_id=? AND snapshot_version=? AND id=?`, sessionID, version, id).
		Scan(&b.ID, &category, &expectedJSON, &b.SeverityWeight, &depsJSON)
	if err != nil {
		return model.Baseline{}, err
	}
	b.Category = model.Category(category)
	if err := json.Unmarshal([]byte(expectedJSON), &b.Expected); err != nil {
		return model.Baseline{}, err
	}
	if err := json.Unmarshal([]byte(depsJSON), &b.DependsOn); err != nil {
		return model.Baseline{}, err
	}
	return b, nil
}

func recomputeHostSummaryTx(tx *sql.Tx, sessionID, host string, version int64) (*model.HostSummary, error) {
	rows, err := tx.Query(`SELECT status, drift_level, score_scaled FROM conclusions WHERE session_id=? AND host=?`, sessionID, host)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	summary := model.HostSummary{SessionID: sessionID, Host: host, ViewVersion: version}
	for rows.Next() {
		var status string
		var level int
		var score int64
		if err := rows.Scan(&status, &level, &score); err != nil {
			return nil, err
		}
		summary.Total++
		switch model.TaskStatus(status) {
		case model.TaskDeferred:
			summary.Deferred++
		case model.TaskError:
			summary.Errors++
		default:
			summary.Done++
			if level == 0 {
				summary.Conformant++
			} else {
				summary.Drift++
			}
		}
		if level > summary.MaxDriftLevel {
			summary.MaxDriftLevel = level
		}
		summary.WeightedScore += score
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var remaining int
	err = tx.QueryRow(`SELECT repair_quota_remaining FROM sessions WHERE session_id=?`, sessionID).Scan(&remaining)
	if err != nil {
		return nil, err
	}
	summary.RemainingQuota = remaining
	if err := upsertSummaryTx(tx, &summary); err != nil {
		return nil, err
	}
	return &summary, nil
}

func recomputeAllHostSummariesTx(tx *sql.Tx, sessionID, preferredHost string, version int64) (*model.HostSummary, error) {
	hosts, err := listHostsTx(tx, sessionID)
	if err != nil {
		return nil, err
	}
	var preferred *model.HostSummary
	for _, host := range hosts {
		summary, err := recomputeHostSummaryTx(tx, sessionID, host, version)
		if err != nil {
			return nil, err
		}
		if host == preferredHost {
			preferred = summary
		}
	}
	if preferred == nil {
		preferred, err = recomputeHostSummaryTx(tx, sessionID, preferredHost, version)
		if err != nil {
			return nil, err
		}
	}
	return preferred, nil
}
