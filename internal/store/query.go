package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"auditdrift/internal/model"
)

func (s *Store) ListConclusions(sessionID, host string) ([]model.Conclusion, error) {
	query := `SELECT session_id,host,baseline_id,category,status,drift_category,own_level,drift_level,score_scaled,offset_permille,
		rounding_json,missing_json,extra_json,recommendation,propagation_json,repair_cost,reason,error_code,details_json,view_version
		FROM conclusions WHERE session_id=?`
	args := []any{sessionID}
	if host != "" {
		query += ` AND host=?`
		args = append(args, host)
	}
	query += ` ORDER BY host, baseline_id`
	rows, err := s.reader.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]model.Conclusion, 0)
	for rows.Next() {
		c, err := scanConclusion(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanConclusion(row scanner) (model.Conclusion, error) {
	var c model.Conclusion
	var category, status, driftCategory string
	var roundingJSON, missingJSON, extraJSON, propagationJSON, detailsJSON string
	err := row.Scan(&c.SessionID, &c.Host, &c.BaselineID, &category, &status, &driftCategory, &c.OwnLevel, &c.DriftLevel,
		&c.ScoreScaled, &c.OffsetPermille, &roundingJSON, &missingJSON, &extraJSON, &c.Recommendation, &propagationJSON,
		&c.RepairCost, &c.Reason, &c.ErrorCode, &detailsJSON, &c.ViewVersion)
	if err != nil {
		return c, err
	}
	c.Category = model.Category(category)
	c.Status = model.TaskStatus(status)
	c.DriftCategory = model.DriftCategory(driftCategory)
	_ = json.Unmarshal([]byte(roundingJSON), &c.LevelRounding)
	_ = json.Unmarshal([]byte(missingJSON), &c.MissingElements)
	_ = json.Unmarshal([]byte(extraJSON), &c.ExtraElements)
	_ = json.Unmarshal([]byte(propagationJSON), &c.Propagation)
	_ = json.Unmarshal([]byte(detailsJSON), &c.Details)
	return c, nil
}

func (s *Store) ListHostSummaries(sessionID string) ([]model.HostSummary, error) {
	rows, err := s.reader.Query(`SELECT session_id,host,view_version,total,done,conformant,drift,deferred,errors,max_drift_level,weighted_score_scaled,remaining_quota
		FROM host_summaries WHERE session_id=? ORDER BY host`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]model.HostSummary, 0)
	for rows.Next() {
		var h model.HostSummary
		if err := rows.Scan(&h.SessionID, &h.Host, &h.ViewVersion, &h.Total, &h.Done, &h.Conformant, &h.Drift, &h.Deferred, &h.Errors, &h.MaxDriftLevel, &h.WeightedScore, &h.RemainingQuota); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func (s *Store) GetHostSummary(sessionID, host string) (*model.HostSummary, error) {
	row := s.reader.QueryRow(`SELECT session_id,host,view_version,total,done,conformant,drift,deferred,errors,max_drift_level,weighted_score_scaled,remaining_quota
		FROM host_summaries WHERE session_id=? AND host=?`, sessionID, host)
	var h model.HostSummary
	err := row.Scan(&h.SessionID, &h.Host, &h.ViewVersion, &h.Total, &h.Done, &h.Conformant, &h.Drift, &h.Deferred, &h.Errors, &h.MaxDriftLevel, &h.WeightedScore, &h.RemainingQuota)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &h, nil
}

type BaselineRead struct {
	Conclusion      *model.Conclusion `json:"conclusion"`
	ViewVersion     int64             `json:"view_version"`
	ObservedVersion int64             `json:"observed_version"`
	VersionRelation string            `json:"version_relation"`
	PendingIntentID string            `json:"pending_intent_id,omitempty"`
}

func (s *Store) ReadBaselineConclusion(sessionID, host, baselineID string, observedVersion int64) (*BaselineRead, error) {
	var c model.Conclusion
	row := s.reader.QueryRow(`SELECT session_id,host,baseline_id,category,status,drift_category,own_level,drift_level,score_scaled,offset_permille,
		rounding_json,missing_json,extra_json,recommendation,propagation_json,repair_cost,reason,error_code,details_json,view_version
		FROM conclusions WHERE session_id=? AND host=? AND baseline_id=?`, sessionID, host, baselineID)
	conclusion, err := scanConclusion(row)
	if err != nil {
		return nil, err
	}
	c = conclusion
	pending, err := s.pendingPropagation(sessionID, observedVersion, c.ViewVersion)
	if err != nil {
		return nil, err
	}
	if pending != nil {
		return nil, model.NewCodedError(model.ErrStaleRead, "conclusion view version %d is behind observed version %d because propagation intent %s is incomplete", c.ViewVersion, observedVersion, pending.IntentID).
			WithDetails(map[string]any{"view_version": c.ViewVersion, "observed_version": observedVersion, "pending_intent": pending})
	}
	relation := "at_or_before_observed"
	if c.ViewVersion > observedVersion {
		relation = "newer_complete_view"
	}
	return &BaselineRead{Conclusion: &c, ViewVersion: c.ViewVersion, ObservedVersion: observedVersion, VersionRelation: relation}, nil
}

type HostRead struct {
	Summary         *model.HostSummary `json:"summary"`
	ViewVersion     int64              `json:"view_version"`
	ObservedVersion int64              `json:"observed_version"`
	VersionRelation string             `json:"version_relation"`
	PendingIntentID string             `json:"pending_intent_id,omitempty"`
}

func (s *Store) ReadHostSummary(sessionID, host string, observedVersion int64) (*HostRead, error) {
	summary, err := s.GetHostSummary(sessionID, host)
	if err != nil {
		return nil, err
	}
	if summary == nil {
		return nil, sql.ErrNoRows
	}
	pending, err := s.pendingPropagation(sessionID, observedVersion, summary.ViewVersion)
	if err != nil {
		return nil, err
	}
	if pending != nil {
		return nil, model.NewCodedError(model.ErrStaleRead, "host summary view version %d is behind observed version %d because propagation intent %s is incomplete", summary.ViewVersion, observedVersion, pending.IntentID).
			WithDetails(map[string]any{"view_version": summary.ViewVersion, "observed_version": observedVersion, "pending_intent": pending})
	}
	relation := "at_or_before_observed"
	if summary.ViewVersion > observedVersion {
		relation = "newer_complete_view"
	}
	return &HostRead{Summary: summary, ViewVersion: summary.ViewVersion, ObservedVersion: observedVersion, VersionRelation: relation}, nil
}

func (s *Store) pendingPropagation(sessionID string, observedVersion, viewVersion int64) (*Intent, error) {
	if observedVersion <= viewVersion {
		return nil, nil
	}
	row := s.reader.QueryRow(`SELECT intent_id,session_id,kind,target_version,payload_json,status,recovery_note,created_at_ms,updated_at_ms
		FROM intents WHERE session_id=? AND status='PENDING' AND kind IN ('propagation','snapshot_increment','session_initialize')
		AND target_version >= ? ORDER BY created_at_ms DESC LIMIT 1`, sessionID, observedVersion)
	intent, err := scanIntent(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &intent, nil
}

func scanIntent(row scanner) (Intent, error) {
	var intent Intent
	err := row.Scan(&intent.IntentID, &intent.SessionID, &intent.Kind, &intent.TargetVersion, &intent.PayloadJSON, &intent.Status, &intent.RecoveryNote, &intent.CreatedAtMS, &intent.UpdatedAtMS)
	return intent, err
}

func (s *Store) ListPendingIntents() ([]Intent, error) {
	rows, err := s.reader.Query(`SELECT intent_id,session_id,kind,target_version,payload_json,status,recovery_note,created_at_ms,updated_at_ms FROM intents WHERE status='PENDING' ORDER BY created_at_ms`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Intent, 0)
	for rows.Next() {
		intent, err := scanIntent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, intent)
	}
	return out, rows.Err()
}

func (s *Store) WithReadOnlyTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
