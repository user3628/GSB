package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"elastic-scale/internal/engine"
	"elastic-scale/internal/model"
)

func checkObservedTx(ctx context.Context, q queryRower, observed *int64) (current int64, materialized int64, err error) {
	current, err = getMeta(ctx, q, "current_version")
	if err != nil {
		return 0, 0, err
	}
	materialized, err = getMeta(ctx, q, "materialized_version")
	if err != nil {
		return 0, 0, err
	}
	if observed != nil && *observed > materialized {
		return current, materialized, model.NewError(model.CodeStaleRead, "materialized version is behind the reader observed version", map[string]any{
			"observed_version":             *observed,
			"current_version":              current,
			"current_materialized_version": materialized,
		})
	}
	return current, materialized, nil
}

func (a *App) ReadMetrics(ctx context.Context, serviceID string, observed *int64) (*model.ReadMetricsResponse, error) {
	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	current, materialized, err := checkObservedTx(ctx, tx, observed)
	if err != nil {
		return nil, err
	}
	var metricsJSON string
	err = tx.QueryRowContext(ctx, `SELECT metrics_json FROM derived_views WHERE service_id=?`, serviceID).Scan(&metricsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, model.NewError(model.CodeNotFound, "materialized metrics not found for service", map[string]any{"service_id": serviceID})
	}
	if err != nil {
		return nil, err
	}
	var metrics []model.MetricValue
	if err := json.Unmarshal([]byte(metricsJSON), &metrics); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &model.ReadMetricsResponse{
		ServiceID: serviceID, ObservedVersion: observed, CurrentVersion: current,
		MaterializedVersion: materialized, Fresh: true, Metrics: metrics,
	}, nil
}

func (a *App) ReadDecision(ctx context.Context, serviceID string, observed *int64, requiredSamples int64) (*model.DecisionResponse, error) {
	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	current, materialized, err := checkObservedTx(ctx, tx, observed)
	if err != nil {
		return nil, err
	}
	if requiredSamples > 0 {
		state, err := loadState(ctx, tx)
		if err != nil {
			return nil, err
		}
		svc, ok := state.Service(serviceID)
		if !ok {
			return nil, model.NewError(model.CodeNotFound, "service not found", map[string]any{"service_id": serviceID})
		}
		svc.Policy.MinLatencySamples = int(requiredSamples)
		decision, be := engine.BuildDecision(svc, state.SamplesFor(serviceID), materialized)
		if be != nil {
			if be.Context == nil {
				be.Context = map[string]any{}
			}
			be.Context["current_materialized_version"] = materialized
			return nil, be
		}
		decision.ObservedVersion = observed
		decision.CurrentVersion = current
		decision.MaterializedVersion = materialized
		_ = tx.Commit()
		return decision, nil
	}
	var available int
	var decisionJSON, errorJSON sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT decision_available, decision_json, decision_error_json FROM derived_views WHERE service_id=?`, serviceID).
		Scan(&available, &decisionJSON, &errorJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, model.NewError(model.CodeNotFound, "decision view not found for service", map[string]any{"service_id": serviceID})
	}
	if err != nil {
		return nil, err
	}
	if available == 0 {
		var be model.BusinessError
		if errorJSON.Valid && json.Unmarshal([]byte(errorJSON.String), &be) == nil {
			if be.Context == nil {
				be.Context = map[string]any{}
			}
			be.Context["current_materialized_version"] = materialized
			return nil, &be
		}
		return nil, model.NewError(model.CodeInsufficientSamples, "decision is unavailable", map[string]any{"service_id": serviceID})
	}
	var decision model.DecisionResponse
	if err := json.Unmarshal([]byte(decisionJSON.String), &decision); err != nil {
		return nil, err
	}
	decision.ObservedVersion = observed
	decision.CurrentVersion = current
	decision.MaterializedVersion = materialized
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &decision, nil
}

func (a *App) ReadImpact(ctx context.Context, serviceID string, observed *int64) (*model.ImpactResponse, error) {
	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	current, _, err := checkObservedTx(ctx, tx, observed)
	if err != nil {
		return nil, err
	}
	var impactJSON string
	err = tx.QueryRowContext(ctx, `SELECT impact_json FROM derived_views WHERE service_id=?`, serviceID).Scan(&impactJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, model.NewError(model.CodeNotFound, "impact view not found for service", map[string]any{"service_id": serviceID})
	}
	if err != nil {
		return nil, err
	}
	var impact model.ImpactResponse
	if err := json.Unmarshal([]byte(impactJSON), &impact); err != nil {
		return nil, err
	}
	impact.CurrentVersion = current
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &impact, nil
}

func (a *App) CurrentSnapshot(ctx context.Context) (*model.Snapshot, error) {
	current, _, err := a.Versions(ctx)
	if err != nil {
		return nil, err
	}
	if current == 0 {
		return nil, model.NewError(model.CodeNotFound, "no snapshot loaded", map[string]any{"current_version": 0})
	}
	var payload string
	err = a.db.QueryRowContext(ctx, `SELECT payload_json FROM snapshots WHERE version=?`, current).Scan(&payload)
	if err != nil {
		return nil, err
	}
	var snapshot model.Snapshot
	if err := json.Unmarshal([]byte(payload), &snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func (a *App) ListServices(ctx context.Context) ([]model.Service, error) {
	state, err := loadState(ctx, a.db)
	if err != nil {
		return nil, err
	}
	return state.Services, nil
}
