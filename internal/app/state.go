package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"elastic-scale/internal/engine"
	"elastic-scale/internal/model"
)

type CatalogRequest struct {
	CommitID string          `json:"commit_id"`
	Services []model.Service `json:"services"`
}

func loadState(ctx context.Context, q querier) (*engine.State, error) {
	version, err := getMeta(ctx, q, "current_version")
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, `SELECT payload_json FROM services ORDER BY ordinal, service_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var services []model.Service
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var svc model.Service
		if err := json.Unmarshal([]byte(payload), &svc); err != nil {
			return nil, err
		}
		svc.Normalize()
		services = append(services, svc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sampleRows, err := q.QueryContext(ctx, `SELECT service_id, instance_id, metric, sampled_at_ms, value, collection_weight FROM samples_current ORDER BY service_id, instance_id, metric, sampled_at_ms`)
	if err != nil {
		return nil, err
	}
	defer sampleRows.Close()
	var samples []model.Sample
	for sampleRows.Next() {
		var sample model.Sample
		if err := sampleRows.Scan(&sample.ServiceID, &sample.InstanceID, &sample.Metric, &sample.SampledAtMS, &sample.Value, &sample.CollectionWeight); err != nil {
			return nil, err
		}
		samples = append(samples, sample)
	}
	if err := sampleRows.Err(); err != nil {
		return nil, err
	}
	return engine.NewState(version, services, samples), nil
}

func replaceCatalogTx(ctx context.Context, tx *sql.Tx, services []model.Service) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM service_dependencies`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM service_subscriptions`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM services`); err != nil {
		return err
	}
	for i := range services {
		svc := services[i]
		svc.Normalize()
		payload, err := json.Marshal(svc)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO services(service_id, ordinal, payload_json) VALUES(?,?,?)`, svc.ID, i, string(payload)); err != nil {
			return err
		}
		for j, dep := range svc.Dependencies {
			if _, err := tx.ExecContext(ctx, `INSERT INTO service_dependencies(service_id, depends_on, ordinal) VALUES(?,?,?)`, svc.ID, dep, j); err != nil {
				return err
			}
		}
		for j, sub := range svc.Subscriptions {
			if _, err := tx.ExecContext(ctx, `INSERT INTO service_subscriptions(service_id, metric, weight, ordinal) VALUES(?,?,?,?)`, svc.ID, sub.Metric, sub.Weight, j); err != nil {
				return err
			}
		}
	}
	return nil
}

func replaceAllSamplesTx(ctx context.Context, tx *sql.Tx, samples []model.Sample) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM samples_current`); err != nil {
		return err
	}
	return upsertSamplesTx(ctx, tx, samples)
}

func upsertSamplesTx(ctx context.Context, tx *sql.Tx, samples []model.Sample) error {
	for _, sample := range model.NormalizeSamples(samples) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO samples_current(service_id,instance_id,metric,sampled_at_ms,value,collection_weight)
			VALUES(?,?,?,?,?,?)
			ON CONFLICT(service_id,instance_id,metric,sampled_at_ms)
			DO UPDATE SET value=excluded.value, collection_weight=excluded.collection_weight`,
			sample.ServiceID, sample.InstanceID, sample.Metric, sample.SampledAtMS, sample.Value, sample.CollectionWeight); err != nil {
			return err
		}
	}
	return nil
}

func deleteSampleTx(ctx context.Context, tx *sql.Tx, key model.SampleKey) (int64, error) {
	res, err := tx.ExecContext(ctx, `DELETE FROM samples_current WHERE service_id=? AND instance_id=? AND metric=? AND sampled_at_ms=?`,
		key.ServiceID, key.InstanceID, key.Metric, key.SampledAtMS)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func persistDerivedTx(ctx context.Context, tx *sql.Tx, state *engine.State, views map[string]*engine.Derived, scope []string) error {
	scoped := scope != nil
	if !scoped {
		if _, err := tx.ExecContext(ctx, `DELETE FROM view_refs`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM derived_views`); err != nil {
			return err
		}
	} else {
		for _, id := range scope {
			if _, err := tx.ExecContext(ctx, `DELETE FROM view_refs WHERE consumer_service=?`, id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM derived_views WHERE service_id=?`, id); err != nil {
				return err
			}
		}
	}
	ids := make([]string, 0, len(views))
	if scoped {
		ids = append(ids, scope...)
	} else {
		for id := range views {
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		dv := views[id]
		if dv == nil {
			return fmt.Errorf("missing derived view for %s", id)
		}
		metricsJSON := engine.MustJSON(dv.Metrics)
		impactJSON := engine.MustJSON(dv.Impact)
		refsJSON := engine.MustJSON(dv.Refs)
		var decisionJSON any
		var decisionAvailable int
		var decisionCode any
		var decisionErrorJSON any
		if dv.Decision != nil {
			decisionAvailable = 1
			b, _ := json.Marshal(dv.Decision)
			decisionJSON = string(b)
		} else if dv.DecisionError != nil {
			decisionAvailable = 0
			decisionCode = dv.DecisionError.Code
			b, _ := json.Marshal(dv.DecisionError)
			decisionErrorJSON = string(b)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO derived_views
			(service_id,version,metrics_json,metrics_checksum,impact_json,impact_checksum,
			 decision_available,decision_json,decision_checksum,decision_error_code,decision_error_json,refs_json,updated_at_ms)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, state.Version, metricsJSON, dv.MetricsChecksum, impactJSON, dv.ImpactChecksum,
			decisionAvailable, decisionJSON, dv.DecisionChecksum, decisionCode, decisionErrorJSON, refsJSON, storeNow()); err != nil {
			return err
		}
		for _, ref := range dv.Refs {
			if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO view_refs
				(consumer_service,ref_kind,target_service,target_version,target_checksum)
				VALUES(?,?,?,?,?)`, id, ref.Kind, ref.TargetServiceID, ref.TargetVersion, ref.TargetChecksum); err != nil {
				return err
			}
		}
	}
	return nil
}

func storeNow() int64 { return storeNowMS() }

func loadDerivedMapTx(ctx context.Context, q querier) (map[string]*engine.Derived, error) {
	rows, err := q.QueryContext(ctx, `SELECT service_id, metrics_json, impact_json, decision_available, decision_json, decision_checksum, decision_error_json, refs_json FROM derived_views`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*engine.Derived{}
	for rows.Next() {
		var id, metricsJSON, impactJSON, decisionChecksum, refsJSON string
		var available int
		var decisionJSON, errorJSON sql.NullString
		if err := rows.Scan(&id, &metricsJSON, &impactJSON, &available, &decisionJSON, &decisionChecksum, &errorJSON, &refsJSON); err != nil {
			return nil, err
		}
		dv := &engine.Derived{DecisionChecksum: decisionChecksum}
		if err := json.Unmarshal([]byte(metricsJSON), &dv.Metrics); err != nil {
			return nil, err
		}
		dv.MetricsChecksum = engine.Checksum(dv.Metrics)
		if err := json.Unmarshal([]byte(impactJSON), &dv.Impact); err != nil {
			return nil, err
		}
		dv.ImpactChecksum = dv.Impact.Checksum
		if err := json.Unmarshal([]byte(refsJSON), &dv.Refs); err != nil {
			return nil, err
		}
		if available == 1 && decisionJSON.Valid {
			var decision model.DecisionResponse
			if err := json.Unmarshal([]byte(decisionJSON.String), &decision); err != nil {
				return nil, err
			}
			dv.Decision = &decision
		} else if errorJSON.Valid && errorJSON.String != "" {
			var be model.BusinessError
			if err := json.Unmarshal([]byte(errorJSON.String), &be); err != nil {
				return nil, err
			}
			dv.DecisionError = &be
		}
		out[id] = dv
	}
	return out, rows.Err()
}

func insertRebuildRunTx(ctx context.Context, tx *sql.Tx, run *model.RebuildRun) error {
	scopeJSON, _ := json.Marshal(run.RecomputeScope)
	res, err := tx.ExecContext(ctx, `INSERT INTO rebuild_runs
		(version,requested_mode,selected_path,threshold_micro,propagation_threshold_micro,
		 touched_subscription_items,total_subscription_items,touched_sample_records,total_sample_records,
		 touched_ratio_micro,propagation_cardinality,service_count,recompute_scope_json,reason,
		 metrics_checksum,decisions_checksum,created_at_ms)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		run.Version, run.RequestedMode, run.SelectedPath, run.ThresholdMicro, run.PropagationThresholdMicro,
		run.TouchedSubscriptionItems, run.TotalSubscriptionItems, run.TouchedSampleRecords, run.TotalSampleRecords,
		run.TouchedRatioMicro, run.PropagationCardinality, run.ServiceCount, string(scopeJSON), run.Reason,
		run.MetricsChecksum, run.DecisionsChecksum, run.CreatedAtMS)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	run.ID = id
	return nil
}

func scanRebuildRun(scanner interface{ Scan(...any) error }) (*model.RebuildRun, error) {
	var run model.RebuildRun
	var scopeJSON string
	err := scanner.Scan(&run.ID, &run.Version, &run.RequestedMode, &run.SelectedPath, &run.ThresholdMicro,
		&run.PropagationThresholdMicro, &run.TouchedSubscriptionItems, &run.TotalSubscriptionItems,
		&run.TouchedSampleRecords, &run.TotalSampleRecords, &run.TouchedRatioMicro, &run.PropagationCardinality,
		&run.ServiceCount, &scopeJSON, &run.Reason, &run.MetricsChecksum, &run.DecisionsChecksum, &run.CreatedAtMS)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(scopeJSON), &run.RecomputeScope)
	return &run, nil
}

const rebuildRunColumns = `id,version,requested_mode,selected_path,threshold_micro,propagation_threshold_micro,
	touched_subscription_items,total_subscription_items,touched_sample_records,total_sample_records,
	touched_ratio_micro,propagation_cardinality,service_count,recompute_scope_json,reason,
	metrics_checksum,decisions_checksum,created_at_ms`

func (a *App) LatestRebuild(ctx context.Context) (*model.RebuildRun, error) {
	run, err := scanRebuildRun(a.db.QueryRowContext(ctx, `SELECT `+rebuildRunColumns+` FROM rebuild_runs ORDER BY id DESC LIMIT 1`))
	if err != nil {
		return nil, err
	}
	return run, nil
}

func (a *App) GetRebuild(ctx context.Context, id int64) (*model.RebuildRun, error) {
	run, err := scanRebuildRun(a.db.QueryRowContext(ctx, `SELECT `+rebuildRunColumns+` FROM rebuild_runs WHERE id=?`, id))
	if err != nil {
		return nil, err
	}
	return run, nil
}
