package edgeconfig

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
)

type ConvergenceView struct {
	NodeID          string `json:"node_id"`
	Region          string `json:"region"`
	ExpectedVersion int64  `json:"expected_version"`
	ExpectedDigest  string `json:"expected_digest"`
	ObservedVersion int64  `json:"observed_version"`
	AppliedDigest   string `json:"applied_digest"`
	EffectiveAtMS   int64  `json:"effective_at_ms"`
	Status          string `json:"status"`
	ReasonCode      string `json:"reason_code,omitempty"`
	LastReportID    string `json:"last_report_id,omitempty"`
	LastRoundID     string `json:"last_round_id,omitempty"`
	Weight          int64  `json:"weight"`
	UpdatedVersion  int64  `json:"updated_version"`
	UpdatedAtMS     int64  `json:"updated_at_ms"`
}

type StatusOverride struct {
	Status  string
	Reason  string
	Matched bool
}

func refreshAndRecomputeTx(ctx context.Context, tx *sql.Tx, version int64, roundID string, nodes []Node, policy Policy, overrides ...map[string]StatusOverride) (*Consistency, error) {
	var overrideMap map[string]StatusOverride
	if len(overrides) > 0 {
		overrideMap = overrides[0]
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT node_id, expected_version, expected_digest, observed_version,
		       applied_digest, effective_at_ms, last_report_id, last_round_id, weight
		FROM node_convergence ORDER BY node_id`)
	if err != nil {
		return nil, err
	}
	type row struct {
		view ConvergenceView
	}
	views := make([]ConvergenceView, 0, len(nodes))
	for rows.Next() {
		var view ConvergenceView
		if err := rows.Scan(&view.NodeID, &view.ExpectedVersion, &view.ExpectedDigest, &view.ObservedVersion,
			&view.AppliedDigest, &view.EffectiveAtMS, &view.LastReportID, &view.LastRoundID, &view.Weight); err != nil {
			rows.Close()
			return nil, err
		}
		views = append(views, view)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	nodeByID := make(map[string]Node, len(nodes))
	for _, node := range nodes {
		nodeByID[node.ID] = node
	}
	matched := make(map[string]bool, len(views))
	now := nowMS()
	for _, view := range views {
		status := StatusPending
		reason := ""
		converged := false
		switch {
		case view.ExpectedVersion > 0 && view.ObservedVersion < view.ExpectedVersion:
			status = StatusStale
			reason = CodeStaleRead
		case view.AppliedDigest == "":
			status = StatusPending
		case view.AppliedDigest == view.ExpectedDigest && view.ExpectedDigest != "":
			status = StatusConverged
			converged = true
		default:
			status = StatusDiverged
		}
		matched[view.NodeID] = converged
		reasonToStore := reason
		if override, ok := overrideMap[view.NodeID]; ok {
			if override.Status != "" {
				status = override.Status
			}
			if override.Reason != "" {
				reasonToStore = override.Reason
			}
			converged = override.Matched
			matched[view.NodeID] = override.Matched
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE node_convergence
			SET status=?, reason_code=?, last_round_id=CASE WHEN ?='' THEN last_round_id ELSE ? END,
			    updated_version=?, updated_at_ms=?
			WHERE node_id=?`,
			status, reasonToStore, roundID, roundID, version, now, view.NodeID); err != nil {
			return nil, err
		}
	}

	global := EvaluateConsistency("global", version, roundID, nodes, matched, policy.Numeric)
	regionNodes := make(map[string][]Node)
	regionMatched := make(map[string]map[string]bool)
	for _, node := range nodes {
		regionNodes[node.Region] = append(regionNodes[node.Region], node)
		if regionMatched[node.Region] == nil {
			regionMatched[node.Region] = make(map[string]bool)
		}
		regionMatched[node.Region][node.ID] = matched[node.ID]
	}
	regions := make([]string, 0, len(regionNodes))
	for region := range regionNodes {
		regions = append(regions, region)
	}
	sort.Strings(regions)
	global.RegionScores = make([]Consistency, 0, len(regions))
	for _, region := range regions {
		regionNodeList := regionNodes[region]
		regionConsistency := EvaluateConsistency("region:"+region, version, roundID, regionNodeList, regionMatched[region], policy.Numeric)
		if err := upsertConsistencyTx(ctx, tx, &regionConsistency, now); err != nil {
			return nil, err
		}
		global.RegionScores = append(global.RegionScores, regionConsistency)
	}
	if err := upsertConsistencyTx(ctx, tx, &global, now); err != nil {
		return nil, err
	}
	return &global, nil
}

func upsertConsistencyTx(ctx context.Context, tx *sql.Tx, consistency *Consistency, now int64) error {
	raw, err := json.Marshal(consistency)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO consistency_views(scope, commit_version, round_id, score_units, threshold_units, decision, consistency_json, updated_at_ms)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(scope) DO UPDATE SET
			commit_version=excluded.commit_version,
			round_id=excluded.round_id,
			score_units=excluded.score_units,
			threshold_units=excluded.threshold_units,
			decision=excluded.decision,
			consistency_json=excluded.consistency_json,
			updated_at_ms=excluded.updated_at_ms`,
		consistency.Scope, consistency.CommitVersion, consistency.RoundID,
		consistency.ScoreUnits, consistency.ThresholdUnits, consistency.Decision, string(raw), now)
	return err
}

func (s *Service) ListConvergence(ctx context.Context) ([]ConvergenceView, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT node_id, region, expected_version, expected_digest, observed_version,
		       applied_digest, effective_at_ms, status, reason_code, last_report_id,
		       last_round_id, weight, updated_version, updated_at_ms
		FROM node_convergence ORDER BY region, node_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	views := make([]ConvergenceView, 0)
	for rows.Next() {
		var view ConvergenceView
		if err := rows.Scan(&view.NodeID, &view.Region, &view.ExpectedVersion, &view.ExpectedDigest,
			&view.ObservedVersion, &view.AppliedDigest, &view.EffectiveAtMS, &view.Status,
			&view.ReasonCode, &view.LastReportID, &view.LastRoundID, &view.Weight,
			&view.UpdatedVersion, &view.UpdatedAtMS); err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, rows.Err()
}

func (s *Service) GetConsistency(ctx context.Context, scope string) (*Consistency, error) {
	if scope == "" {
		scope = "global"
	}
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT consistency_json FROM consistency_views WHERE scope=?`, scope).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, NewAPIError(404, CodeNotFound, "consistency scope not found: "+scope, nil)
	}
	if err != nil {
		return nil, err
	}
	var consistency Consistency
	if err := json.Unmarshal([]byte(raw), &consistency); err != nil {
		return nil, err
	}
	return &consistency, nil
}

func (s *Service) ListConsistencyScopes(ctx context.Context) ([]Consistency, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT consistency_json FROM consistency_views ORDER BY scope`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Consistency, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var consistency Consistency
		if err := json.Unmarshal([]byte(raw), &consistency); err != nil {
			return nil, err
		}
		result = append(result, consistency)
	}
	return result, rows.Err()
}
