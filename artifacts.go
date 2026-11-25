package grayrouter

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

func stageModel(ctx context.Context, tx *sql.Tx, version int64, model ModelVersion, diff GraphDiff, plan MigrationPlan, artifacts []ArtifactSnapshot, now int64) error {
	graphJSON := string(canonicalJSON(model.Graph))
	routeJSON := string(canonicalJSON(model.Route))
	grayJSON := string(canonicalJSON(model.Gray))
	signature := GraphSignature(model.Graph)
	structural := StructuralSignature(model.Graph)
	if _, err := tx.ExecContext(ctx, `INSERT INTO model_versions
		(model_key,model_name,model_version,snapshot_version,graph_json,route_json,gray_json,signature,structural_signature,created_at_ns)
		VALUES(?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(model_key) DO UPDATE SET snapshot_version=excluded.snapshot_version,
		graph_json=excluded.graph_json,route_json=excluded.route_json,gray_json=excluded.gray_json,
		signature=excluded.signature,structural_signature=excluded.structural_signature`,
		model.Key(), model.Name, model.Version, version, graphJSON, routeJSON, grayJSON, signature, structural, now); err != nil {
		return WrapInternal(err, map[string]any{"stage": "store_model", "model": model.Key()})
	}
	diffJSON := string(canonicalJSON(diff))
	planJSON := string(canonicalJSON(plan))
	if _, err := tx.ExecContext(ctx, `INSERT INTO migration_plans(snapshot_version,model_key,diff_json,plan_json)
		VALUES(?,?,?,?) ON CONFLICT(snapshot_version,model_key) DO UPDATE SET diff_json=excluded.diff_json,plan_json=excluded.plan_json`,
		version, model.Key(), diffJSON, planJSON); err != nil {
		return WrapInternal(err, map[string]any{"stage": "store_plan"})
	}
	for _, artifact := range artifacts {
		detail := map[string]any{"action": artifact.Action, "categories": plan.Categories, "model_key": model.Key()}
		if _, err := tx.ExecContext(ctx, `INSERT INTO derived_artifacts
			(model_key,artifact,snapshot_version,action,status,revision,checksum,detail_json,updated_at_ns)
			VALUES(?,?,?,?, 'staging',?,?,?,?)
			ON CONFLICT(model_key,artifact) DO UPDATE SET snapshot_version=excluded.snapshot_version,
			action=excluded.action,status='staging',revision=excluded.revision,checksum=excluded.checksum,
			detail_json=excluded.detail_json,updated_at_ns=excluded.updated_at_ns`,
			model.Key(), artifact.Artifact, version, artifact.Action, artifact.Revision, artifact.Checksum, string(canonicalJSON(detail)), now); err != nil {
			return WrapInternal(err, map[string]any{"stage": "store_artifact", "artifact": artifact.Artifact})
		}
	}
	routeChecksum := sha256Hex([]byte(model.Key()), []byte(routeJSON))[:24]
	if _, err := tx.ExecContext(ctx, `INSERT INTO routing_entries
		(model_key,model_name,snapshot_version,path,config_json,checksum,status)
		VALUES(?,?,?,?,?,?, 'staging')
		ON CONFLICT(model_key) DO UPDATE SET snapshot_version=excluded.snapshot_version,path=excluded.path,
		config_json=excluded.config_json,checksum=excluded.checksum,status='staging'`,
		model.Key(), model.Name, version, model.Route.Path, routeJSON, routeChecksum); err != nil {
		return WrapInternal(err, map[string]any{"stage": "store_route"})
	}
	if err := stagePrePostCache(ctx, tx, version, model, signature, now); err != nil {
		return err
	}
	if err := stageGrayPlan(ctx, tx, version, model, plan, now); err != nil {
		return err
	}
	return nil
}

func stagePrePostCache(ctx context.Context, tx *sql.Tx, version int64, model ModelVersion, signature string, now int64) error {
	manifestKey := sha256Hex([]byte(model.Key()), []byte("prepost-manifest"))
	manifest := map[string]any{"model_key": model.Key(), "node_count": len(model.Graph.Nodes), "signature": signature, "handler": "deterministic-builtin"}
	if _, err := tx.ExecContext(ctx, `INSERT INTO prepost_cache
		(cache_key,model_key,step_id,phase,snapshot_version,payload_json,checksum,status)
		VALUES(?,?,?,'manifest',?,?,?, 'staging')
		ON CONFLICT(cache_key) DO UPDATE SET snapshot_version=excluded.snapshot_version,payload_json=excluded.payload_json,
		checksum=excluded.checksum,status='staging'`,
		manifestKey, model.Key(), "__manifest__", version, string(canonicalJSON(manifest)), manifestKey[:24]); err != nil {
		return WrapInternal(err, map[string]any{"stage": "prepost_manifest"})
	}
	for _, node := range model.Graph.Nodes {
		for _, phaseStep := range []struct {
			phase string
			steps []string
		}{{phase: "pre", steps: node.PreSteps}, {phase: "post", steps: node.PostSteps}} {
			for _, step := range phaseStep.steps {
				cacheKey := sha256Hex([]byte(model.Key()), []byte(node.ID), []byte(phaseStep.phase), []byte(step))
				payload := map[string]any{
					"model_key": model.Key(), "node_id": node.ID, "phase": phaseStep.phase,
					"step_id": step, "handler": "deterministic-identity-v1", "graph_signature": signature,
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO prepost_cache
					(cache_key,model_key,step_id,phase,snapshot_version,payload_json,checksum,status)
					VALUES(?,?,?,?,?,?,?, 'staging')
					ON CONFLICT(cache_key) DO UPDATE SET snapshot_version=excluded.snapshot_version,payload_json=excluded.payload_json,
					checksum=excluded.checksum,status='staging'`,
					cacheKey, model.Key(), step, phaseStep.phase, version, string(canonicalJSON(payload)), cacheKey[:24]); err != nil {
					return WrapInternal(err, map[string]any{"stage": "prepost_step", "step": step})
				}
			}
		}
	}
	return nil
}

func grayPlanForModel(model ModelVersion) (string, string, string, float64, *NormalizedWeights, error) {
	cfg := model.Gray
	routeKey := cfg.RouteKey
	if routeKey == "" {
		routeKey = "model-gray:" + model.Name
	}
	baseline := cfg.Baseline
	if baseline == "" {
		baseline = model.Key()
	}
	canary := cfg.Canary
	if canary == "" {
		canary = model.Key()
	}
	rate := cfg.CanaryRate
	weights := cfg.Weights
	if len(weights) == 0 {
		if baseline == canary {
			rate = 0
			weights = map[string]float64{baseline: 1}
		} else {
			weights = map[string]float64{baseline: 1 - rate, canary: rate}
		}
	}
	normalized, err := NormalizeWeights(weights)
	if err != nil {
		return "", "", "", 0, nil, err
	}
	return routeKey, baseline, canary, rate, normalized, nil
}

func stageGrayPlan(ctx context.Context, tx *sql.Tx, version int64, model ModelVersion, plan MigrationPlan, now int64) error {
	routeKey, baseline, canary, rate, normalized, err := grayPlanForModel(model)
	if err != nil {
		return err
	}
	canaryFixed := uint64(0)
	for _, weight := range normalized.Weights {
		if weight.Target == canary {
			canaryFixed = weight.Fixed
		}
	}
	planJSON := map[string]any{"migration": plan, "gray": model.Gray, "normalization": normalized}
	if _, err := tx.ExecContext(ctx, `INSERT INTO gray_plans
		(route_key,snapshot_version,baseline_model_key,canary_model_key,canary_rate,scale,canary_fixed,
		normalization_json,plan_json,status,updated_at_ns)
		VALUES(?,?,?,?,?,?,?,?,?, 'staging',?)
		ON CONFLICT(route_key,snapshot_version) DO UPDATE SET
		baseline_model_key=excluded.baseline_model_key,canary_model_key=excluded.canary_model_key,
		canary_rate=excluded.canary_rate,scale=excluded.scale,canary_fixed=excluded.canary_fixed,
		normalization_json=excluded.normalization_json,plan_json=excluded.plan_json,status='staging',updated_at_ns=excluded.updated_at_ns`,
		routeKey, version, baseline, canary, rate, normalized.Scale, canaryFixed,
		string(canonicalJSON(normalized)), string(canonicalJSON(planJSON)), now); err != nil {
		return WrapInternal(err, map[string]any{"stage": "gray_plan"})
	}
	return nil
}

func scanStoredModel(scanner interface{ Scan(...any) error }) (*StoredModel, error) {
	var stored StoredModel
	var graphJSON, routeJSON, grayJSON string
	var modelKey string
	err := scanner.Scan(&modelKey, &stored.Model.Name, &stored.Model.Version, &stored.SnapshotVersion,
		&graphJSON, &routeJSON, &grayJSON, &stored.Signature, &stored.StructuralSignature)
	if err != nil {
		return nil, err
	}
	if expectedKey := stored.Model.Key(); expectedKey != modelKey {
		return nil, WrapInternal(errors.New("stored model key does not match name/version"), map[string]any{"stored_key": modelKey, "computed_key": expectedKey})
	}
	if err := json.Unmarshal([]byte(graphJSON), &stored.Model.Graph); err != nil {
		return nil, WrapInternal(err, map[string]any{"field": "graph_json"})
	}
	if routeJSON != "" {
		_ = json.Unmarshal([]byte(routeJSON), &stored.Model.Route)
	}
	if grayJSON != "" {
		_ = json.Unmarshal([]byte(grayJSON), &stored.Model.Gray)
	}
	return &stored, nil
}

func (h *Host) currentModelByName(ctx context.Context, name string) (*StoredModel, error) {
	row := h.db.QueryRowContext(ctx, `SELECT mv.model_key,mv.model_name,mv.model_version,mv.snapshot_version,
		mv.graph_json,mv.route_json,mv.gray_json,mv.signature,mv.structural_signature
		FROM current_models c JOIN model_versions mv ON mv.model_key=c.model_key
		WHERE c.model_name=?`, name)
	stored, err := scanStoredModel(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return stored, err
}

func (h *Host) modelByKey(ctx context.Context, modelKey string) (*StoredModel, error) {
	row := h.db.QueryRowContext(ctx, `SELECT model_key,model_name,model_version,snapshot_version,
		graph_json,route_json,gray_json,signature,structural_signature
		FROM model_versions WHERE model_key=?`, modelKey)
	stored, err := scanStoredModel(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewAPIError(CodeNotFound, "model version not found", map[string]any{"model_key": modelKey})
	}
	return stored, err
}
