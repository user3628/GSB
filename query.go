package grayrouter

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type ModelState struct {
	Name                string `json:"name"`
	ModelKey            string `json:"model_key"`
	Version             string `json:"version"`
	SnapshotVersion     int64  `json:"snapshot_version"`
	Signature           string `json:"signature"`
	StructuralSignature string `json:"structural_signature"`
	RoutePath           string `json:"route_path"`
	Current             bool   `json:"current"`
}

type StateView struct {
	HeadVersion      int64            `json:"head_version"`
	Models           []ModelState     `json:"models"`
	CurrentModelKeys []string         `json:"current_model_keys"`
	Counts           map[string]int64 `json:"counts"`
	InferExecutions  int64            `json:"infer_executions"`
	Recovery         *RecoveryReport  `json:"recovery,omitempty"`
}

func (h *Host) State(ctx context.Context) (*StateView, error) {
	head, err := h.headVersion(ctx)
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	rows, err := h.db.QueryContext(ctx, `SELECT mv.model_name,mv.model_key,mv.model_version,mv.snapshot_version,
		mv.signature,mv.structural_signature,COALESCE(re.path,''),
		CASE WHEN c.model_name IS NULL THEN 0 ELSE 1 END
		FROM model_versions mv
		LEFT JOIN current_models c ON c.model_key=mv.model_key
		LEFT JOIN routing_entries re ON re.model_key=mv.model_key
		ORDER BY mv.model_name,mv.model_version`)
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	defer rows.Close()
	view := &StateView{HeadVersion: head, Counts: map[string]int64{}, InferExecutions: h.InferExecutions, Recovery: h.recovery}
	for rows.Next() {
		var model ModelState
		var current int
		if err := rows.Scan(&model.Name, &model.ModelKey, &model.Version, &model.SnapshotVersion,
			&model.Signature, &model.StructuralSignature, &model.RoutePath, &current); err != nil {
			return nil, WrapInternal(err, nil)
		}
		model.Current = current == 1
		if model.Current {
			view.CurrentModelKeys = append(view.CurrentModelKeys, model.ModelKey)
		}
		view.Models = append(view.Models, model)
	}
	for _, table := range []string{"snapshots", "model_versions", "derived_artifacts", "routing_entries", "prepost_cache", "inflight_bindings", "gray_plans", "intents"} {
		var count int64
		if err := h.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err == nil {
			view.Counts[table] = count
		}
	}
	return view, nil
}

func (h *Host) CommitReport(ctx context.Context, version int64) (*CommitReport, error) {
	if version == 0 {
		head, err := h.headVersion(ctx)
		if err != nil {
			return nil, WrapInternal(err, nil)
		}
		version = head
	}
	var reportJSON, status string
	err := h.db.QueryRowContext(ctx, `SELECT report_json,status FROM snapshots WHERE version=?`, version).Scan(&reportJSON, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewAPIError(CodeNotFound, "commit version not found", map[string]any{"version": version})
	}
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	if status != "committed" {
		return nil, NewAPIError(CodeRecoveryAborted, "commit is not complete", map[string]any{"version": version, "status": status})
	}
	var report CommitReport
	if err := json.Unmarshal([]byte(reportJSON), &report); err != nil {
		return nil, WrapInternal(err, nil)
	}
	return &report, nil
}

type ArtifactView struct {
	ModelKey        string `json:"model_key"`
	Artifact        string `json:"artifact"`
	SnapshotVersion int64  `json:"snapshot_version"`
	Action          string `json:"action"`
	Status          string `json:"status"`
	Revision        string `json:"revision"`
	Checksum        string `json:"checksum"`
	DetailJSON      string `json:"detail_json"`
}

func (h *Host) Artifacts(ctx context.Context, modelKey string) ([]ArtifactView, error) {
	query := `SELECT model_key,artifact,snapshot_version,action,status,revision,checksum,detail_json FROM derived_artifacts`
	args := []any{}
	if modelKey != "" {
		query += ` WHERE model_key=?`
		args = append(args, modelKey)
	}
	query += ` ORDER BY model_key,artifact`
	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	defer rows.Close()
	var out []ArtifactView
	for rows.Next() {
		var item ArtifactView
		if err := rows.Scan(&item.ModelKey, &item.Artifact, &item.SnapshotVersion, &item.Action, &item.Status,
			&item.Revision, &item.Checksum, &item.DetailJSON); err != nil {
			return nil, WrapInternal(err, nil)
		}
		out = append(out, item)
	}
	return out, nil
}

func (h *Host) RequestTrailDecisions(ctx context.Context, trail RequestTrail, routeKey string) ([]GrayDecision, error) {
	out := make([]GrayDecision, 0, len(trail.Requests))
	for _, request := range trail.Requests {
		key := request.RouteKey
		if routeKey != "" {
			key = routeKey
		}
		decision, err := h.DecideGray(ctx, GrayDecisionRequest{RequestID: request.RequestID, RouteKey: key})
		if err != nil {
			return nil, err
		}
		out = append(out, *decision)
	}
	return out, nil
}
