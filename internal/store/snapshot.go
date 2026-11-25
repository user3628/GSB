package store

import (
	"database/sql"
	"encoding/json"
	"time"

	"auditdrift/internal/model"
)

type BaselineChange struct {
	Op       string          `json:"op"`
	ID       string          `json:"id,omitempty"`
	Baseline *model.Baseline `json:"baseline,omitempty"`
}

type Increment struct {
	SessionID       string              `json:"session_id"`
	BaseVersion     int64               `json:"base_version"`
	Reason          string              `json:"reason"`
	BaselineChanges []BaselineChange    `json:"baseline_changes,omitempty"`
	Observations    []model.Observation `json:"observations,omitempty"`
}

type IncrementResult struct {
	IntentID    string              `json:"intent_id"`
	Version     int64               `json:"version"`
	BaseVersion int64               `json:"base_version"`
	AffectedIDs []string            `json:"affected_baseline_ids"`
	Summaries   []model.HostSummary `json:"summaries"`
}

func (s *Store) CommitIncrement(in Increment) (*IncrementResult, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	intentID, err := s.CreateIntent(in.SessionID, "snapshot_increment", in.BaseVersion+1, in)
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
		_ = markIntentRejected(tx, intentID, "session not found")
		_ = tx.Commit()
		return nil, model.NewCodedError("E_SESSION_NOT_FOUND", "session %s not found", in.SessionID)
	}
	if session.CurrentVersion != in.BaseVersion {
		details := map[string]any{"current_version": session.CurrentVersion, "base_version": in.BaseVersion, "view_version": session.ViewVersion}
		_ = markIntentRejected(tx, intentID, "E_STALE_BASE")
		if commitErr := tx.Commit(); commitErr != nil {
			return nil, commitErr
		}
		return nil, model.NewCodedError(model.ErrStaleBase, "increment base %d is not current %d", in.BaseVersion, session.CurrentVersion).WithDetails(details)
	}

	baselines, _, err := loadBaselinesTx(tx, in.SessionID, session.CurrentVersion)
	if err != nil {
		return nil, err
	}
	merged := make(map[string]model.Baseline, len(baselines))
	for _, b := range baselines {
		merged[b.ID] = b
	}
	affected := make([]string, 0, len(in.BaselineChanges))
	for _, change := range in.BaselineChanges {
		switch change.Op {
		case "delete":
			id := change.ID
			if id == "" && change.Baseline != nil {
				id = change.Baseline.ID
			}
			delete(merged, id)
			affected = append(affected, id)
			_, err := tx.Exec(`DELETE FROM audit_tasks WHERE session_id=? AND baseline_id=?`, in.SessionID, id)
			if err != nil {
				return nil, err
			}
		case "upsert", "":
			if change.Baseline == nil {
				err = model.NewCodedError(model.ErrBaselineInvalid, "upsert change requires baseline")
			} else {
				merged[change.Baseline.ID] = *change.Baseline
				affected = append(affected, change.Baseline.ID)
			}
		default:
			err = model.NewCodedError(model.ErrBaselineInvalid, "unsupported change op %q", change.Op)
		}
		if err != nil {
			_ = markIntentRejected(tx, intentID, err.Error())
			_ = tx.Commit()
			return nil, err
		}
	}
	mergedList := make([]model.Baseline, 0, len(merged))
	for _, b := range merged {
		mergedList = append(mergedList, b)
	}
	if err := model.ValidateBaselines(mergedList); err != nil {
		_ = markIntentRejected(tx, intentID, err.Error())
		_ = tx.Commit()
		return nil, err
	}

	nextVersion := session.CurrentVersion + 1
	now := time.Now().UnixMilli()
	reason := in.Reason
	if reason == "" {
		reason = "increment"
	}
	_, err = tx.Exec(`INSERT INTO snapshots(session_id,version,base_version,reason,intent_id,created_at_ms) VALUES(?,?,?,?,?,?)`,
		in.SessionID, nextVersion, in.BaseVersion, reason, intentID, now)
	if err != nil {
		return nil, err
	}
	for _, b := range mergedList {
		if err := insertBaselineTx(tx, in.SessionID, nextVersion, b); err != nil {
			return nil, err
		}
	}
	if err := insertObservationsTx(tx, in.SessionID, nextVersion, in.Observations); err != nil {
		return nil, err
	}

	hosts, err := listHostsTx(tx, in.SessionID)
	if err != nil {
		return nil, err
	}
	for _, host := range hosts {
		for _, b := range mergedList {
			key := taskKey(in.SessionID, host, b.ID)
			_, err := tx.Exec(`INSERT OR IGNORE INTO audit_tasks(session_id,host,baseline_id,task_key,severity_weight,repair_cost,status,priority_order,updated_at_ms)
				VALUES(?,?,?,?,?,?,'PENDING',0,?)`,
				in.SessionID, host, b.ID, key, b.SeverityWeight, b.SeverityWeight, now)
			if err != nil {
				return nil, err
			}
		}
	}
	for _, b := range in.BaselineChanges {
		if b.Baseline != nil && b.Op != "delete" {
			_, err = tx.Exec(`UPDATE audit_tasks SET severity_weight=?, repair_cost=? WHERE session_id=? AND baseline_id=?`,
				b.Baseline.SeverityWeight, b.Baseline.SeverityWeight, in.SessionID, b.Baseline.ID)
			if err != nil {
				return nil, err
			}
		}
	}

	summaries, err := rebuildConclusionsTx(tx, in.SessionID, nextVersion, session.PropagationMaxHops, session.RepairQuotaRemaining)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(`UPDATE sessions SET current_version=?, last_complete_version=?, view_version=?, status='ACTIVE', updated_at_ms=? WHERE session_id=?`,
		nextVersion, nextVersion, nextVersion, now, in.SessionID)
	if err != nil {
		return nil, err
	}
	if err := markIntentCommitted(tx, intentID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &IncrementResult{IntentID: intentID, Version: nextVersion, BaseVersion: in.BaseVersion, AffectedIDs: affected, Summaries: summaries}, nil
}

func listHostsTx(tx *sql.Tx, sessionID string) ([]string, error) {
	rows, err := tx.Query(`SELECT DISTINCT host FROM audit_tasks WHERE session_id=? ORDER BY host`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	hosts := make([]string, 0)
	for rows.Next() {
		var host string
		if err := rows.Scan(&host); err != nil {
			return nil, err
		}
		hosts = append(hosts, host)
	}
	return hosts, rows.Err()
}

var _ = json.Marshal
