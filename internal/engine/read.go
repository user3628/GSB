package engine

import (
	"context"
	"database/sql"
	"encoding/json"

	"configtree/internal/spec"
)

// ReadResponse is returned when the observed version is satisfied.
type ReadResponse struct {
	ComponentID string         `json:"component_id"`
	ViewVersion int64          `json:"view_version"`
	UsedVersion int64          `json:"used_version"`
	Observed    int64          `json:"observed_version"`
	Converged   bool           `json:"converged"`
	View        *ComponentView `json:"view"`
}

// StaleReadDetail accompanies E_STALE_READ. The pipeline never downgrades to
// an older or partial view.
type StaleReadDetail struct {
	ComponentID      string   `json:"component_id"`
	ViewVersion      int64    `json:"view_version"`
	ObservedVersion  int64    `json:"observed_version"`
	PendingKeys      []string `json:"pending_keys"`
	UnfinishedIntent string   `json:"unfinished_intent,omitempty"`
}

// ReadView adjudicates a read carrying an observed version.
func (e *Engine) ReadView(ctx context.Context, componentID string, observedVersion int64) (*ReadResponse, error) {
	var (
		viewJSON string
		version  int64
		digest   string
	)
	err := e.db.QueryRowContext(ctx,
		`SELECT view_json, version, digest FROM views WHERE component_id=?`,
		componentID).Scan(&viewJSON, &version, &digest)
	if err == sql.ErrNoRows {
		return nil, spec.NewError(spec.CodeNotFound, "component view not found",
			map[string]any{"component": componentID})
	}
	if err != nil {
		return nil, err
	}

	// Any prepared (unfinished) intent means convergence is in question.
	pending, intentID, err := e.pendingIntentInfo(ctx)
	if err != nil {
		return nil, err
	}

	// Stale: a recompute is unfinished (or has not been recovered yet) and the
	// reader observed a version newer than the durable view. The pipeline never
	// downgrades to an older or partial view in that situation.
	if len(pending) > 0 && observedVersion > version {
		return nil, spec.NewError(spec.CodeStaleRead,
			"effective view is behind the observed version or recompute is unfinished",
			map[string]any{"detail": StaleReadDetail{
				ComponentID:      componentID,
				ViewVersion:      version,
				ObservedVersion:  observedVersion,
				PendingKeys:      pending,
				UnfinishedIntent: intentID,
			}})
	}

	var view ComponentView
	if err := json.Unmarshal([]byte(viewJSON), &view); err != nil {
		return nil, err
	}
	return &ReadResponse{
		ComponentID: componentID,
		ViewVersion: version,
		UsedVersion: version,
		Observed:    observedVersion,
		Converged:   len(pending) == 0,
		View:        &view,
	}, nil
}

// AffectedAt returns the affected component set persisted for a version.
func (e *Engine) AffectedAt(ctx context.Context, version int64) (*AffectedSet, error) {
	var setJSON string
	err := e.db.QueryRowContext(ctx,
		`SELECT set_json FROM affected_sets WHERE version=?`, version).Scan(&setJSON)
	if err == sql.ErrNoRows {
		return nil, spec.NewError(spec.CodeNotFound, "affected set not found for version",
			map[string]any{"version": version})
	}
	if err != nil {
		return nil, err
	}
	var set AffectedSet
	if err := json.Unmarshal([]byte(setJSON), &set); err != nil {
		return nil, err
	}
	return &set, nil
}

// Commits lists committed and aborted commit records.
func (e *Engine) Commits(ctx context.Context) ([]map[string]any, error) {
	rows, err := e.db.QueryContext(ctx, `
		SELECT commit_id, kind, version, base_version, status, created_at
		FROM commits ORDER BY version ASC, created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, kind, status, createdAt string
		var version, base int64
		if err := rows.Scan(&id, &kind, &version, &base, &status, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"commit_id": id, "kind": kind, "version": version,
			"base_version": base, "status": status, "created_at": createdAt,
		})
	}
	return out, nil
}

func (e *Engine) pendingIntentInfo(ctx context.Context) ([]string, string, error) {
	var intentID, keysJSON string
	err := e.db.QueryRowContext(ctx, `
		SELECT intent_id, pending_keys FROM intents
		WHERE status='prepared' ORDER BY created_at LIMIT 1`).Scan(&intentID, &keysJSON)
	if err == sql.ErrNoRows {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	var keys []string
	if err := json.Unmarshal([]byte(keysJSON), &keys); err != nil {
		return nil, "", err
	}
	return keys, intentID, nil
}
