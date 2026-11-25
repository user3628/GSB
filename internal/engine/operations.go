package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"mpckpt/internal/format"
	"mpckpt/internal/model"
	"mpckpt/internal/numeric"
)

func (e *Engine) CommitCheckpoint(req CheckpointRequest) (json.RawMessage, *model.BizError) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if strings.TrimSpace(req.CommitID) == "" {
		return nil, model.NewError(model.CodeSpecInvalid, "commit_id is required", map[string]any{"field": "commit_id"})
	}
	hash, berr := e.hashRequest(struct {
		Kind     string        `json:"kind"`
		Shards   []model.Shard `json:"shards"`
		Baseline []string      `json:"baseline"`
	}{"CHECKPOINT", req.Shards, req.Baseline})
	if berr != nil {
		return nil, berr
	}
	start, berr := e.beginCommit(req.CommitID, "CHECKPOINT", hash, "")
	if berr != nil {
		return nil, berr
	}
	if start.ReplayError != nil {
		return nil, start.ReplayError
	}
	if start.ReplayResponse != nil {
		return start.ReplayResponse, nil
	}
	if err := model.ValidateShards(req.Shards); err != nil {
		return nil, e.failure(req.CommitID, err)
	}
	strategy, berr := e.currentStrategy(context.Background())
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	current, berr := e.loadCurrentShards(context.Background(), e.store.DB())
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	currentBaseline, berr := e.loadCurrentBaseline(context.Background(), e.store.DB())
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	baseline := currentBaseline
	if len(req.Baseline) > 0 {
		baseline = append([]string(nil), req.Baseline...)
		sort.Strings(baseline)
	}
	incomingTasks := make([]WriteTask, 0, len(req.Shards))
	for _, shard := range req.Shards {
		incomingTasks = append(incomingTasks, WriteTask{
			ShardID: shard.ID, SizeBytes: shard.SizeBytes, Round: shard.Round,
			Priority: shard.Priority, DesiredPool: shard.Pool,
		})
	}
	selected, nextQueue, berr := planWrites(e.queue, incomingTasks, e.queueCap, strategy.WriteBudgetBytes, strategy.TimeBudgetMillis)
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	updates := make(map[string]model.Shard, len(req.Shards)+len(e.queue))
	for _, shard := range req.Shards {
		if selected[shard.ID] {
			shard.Status = shard.DesiredStatus()
		} else {
			shard.Status = model.StatusPending
		}
		updates[shard.ID] = shard
	}
	for _, task := range e.queue {
		if !selected[task.ShardID] {
			continue
		}
		if _, supplied := updates[task.ShardID]; supplied {
			continue
		}
		if shard, ok := current[task.ShardID]; ok {
			shard.Status = shard.DesiredStatus()
			updates[shard.ID] = shard
		}
	}
	requestedResident := map[string]bool{}
	for id, shard := range updates {
		if shard.Status == model.StatusResident {
			requestedResident[id] = true
		}
	}
	residency, berr := planResidency(current, updates, requestedResident, baseline, strategy.GPUCapBytes)
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	if berr := validateBaseline(baseline, residency.Final); berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	finalShards := sortedFinalShards(residency.Final)
	precisionReport, berr := numeric.Evaluate(finalShards, strategy, numeric.DefaultElementsPerShard)
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	baselineChanged := recoveryBaselineDigest(currentBaseline, current, strategy) != recoveryBaselineDigest(baseline, residency.Final, strategy)
	reasons := changedDerivations(current, residency.Final, false, baselineChanged)
	queuedIDs := make([]string, 0, len(nextQueue))
	for _, task := range nextQueue {
		queuedIDs = append(queuedIDs, task.ShardID)
	}
	response := map[string]any{
		"commit_id":             req.CommitID,
		"kind":                  "CHECKPOINT",
		"strategy_version":      strategy.Version,
		"shard_count":           len(finalShards),
		"written_shards":        sortedKeys(selected),
		"queued_shards":         queuedIDs,
		"evictions":             nonNilEvictions(residency.Evictions),
		"gpu_usage_bytes":       residency.Usage,
		"baseline":              baseline,
		"precision_summary":     precisionSummary(precisionReport),
		"derived_rebuilt_count": len(reasons),
	}
	rawResponse, _ := json.Marshal(response)
	tx, err := e.store.DB().BeginTx(context.Background(), nil)
	if err != nil {
		return nil, e.failure(req.CommitID, internalError("begin checkpoint transaction", err))
	}
	defer tx.Rollback()
	dataVersion, err := persistDataVersion(tx, e.store.Now, req.CommitID, "CHECKPOINT", strategy, residency.Final, baseline, precisionReport, reasons, nil, nil)
	if err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("persist checkpoint", err))
	}
	var responseMap map[string]any
	_ = json.Unmarshal(rawResponse, &responseMap)
	responseMap["dataset_version"] = dataVersion
	rawResponse, _ = json.Marshal(responseMap)
	if err := completeCommitTx(tx, req.CommitID, dataVersion, rawResponse); err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("complete checkpoint commit", err))
	}
	if err := tx.Commit(); err != nil {
		return nil, e.failure(req.CommitID, internalError("commit checkpoint transaction", err))
	}
	e.queue = nextQueue
	return rawResponse, nil
}

func (e *Engine) Import(req model.ImportRequest) (json.RawMessage, *model.BizError) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if strings.TrimSpace(req.CommitID) == "" {
		return nil, model.NewError(model.CodeSpecInvalid, "commit_id is required", map[string]any{"field": "commit_id"})
	}
	formatName := model.NormalizeFormat(req.Format)
	hash, berr := e.hashRequest(struct {
		Kind     string `json:"kind"`
		Format   string `json:"format"`
		Version  *int   `json:"version,omitempty"`
		DataHash string `json:"data_hash"`
	}{"IMPORT", formatName, req.Version, model.SHA256Hex(req.Data)})
	if berr != nil {
		return nil, berr
	}
	start, berr := e.beginCommit(req.CommitID, "IMPORT", hash, "")
	if berr != nil {
		return nil, berr
	}
	if start.ReplayError != nil {
		return nil, start.ReplayError
	}
	if start.ReplayResponse != nil {
		return start.ReplayResponse, nil
	}
	inspectedFormat, inspectedVersion, berr := format.Inspect(req.Data, formatName)
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	if req.Version != nil && *req.Version != inspectedVersion {
		return nil, e.failure(req.CommitID, model.NewError(model.CodeSpecInvalid, "declared format version differs from the payload", map[string]any{
			"declared_version": *req.Version, "payload_version": inspectedVersion,
		}))
	}
	verdict, berr := e.matrix.Decide(inspectedFormat, inspectedVersion)
	if berr != nil {
		e.persistDecisionStandalone(req.CommitID, inspectedFormat, inspectedVersion, string(verdict), []map[string]any{{"kind": "MATRIX_REJECT"}})
		return nil, e.failure(req.CommitID, berr)
	}
	manifest, berr := format.Decode(req.Data, inspectedFormat)
	if berr != nil {
		e.persistDecisionStandalone(req.CommitID, inspectedFormat, inspectedVersion, string(verdict), []map[string]any{{
			"kind": "DECODE_FAILED", "code": berr.Code,
		}})
		return nil, e.failure(req.CommitID, berr)
	}
	details := normalizeImported(&manifest, verdict)
	e.persistDecisionStandalone(req.CommitID, inspectedFormat, inspectedVersion, string(verdict), details)
	if manifest.LossScaleFixed <= 0 {
		return nil, e.failure(req.CommitID, model.NewError(model.CodeSpecInvalid, "manifest loss_scale_fixed must be positive", map[string]any{
			"field": "loss_scale_fixed", "value": manifest.LossScaleFixed,
		}))
	}
	if err := model.ValidateShards(manifest.Shards); err != nil {
		return nil, e.failure(req.CommitID, err)
	}
	strategy, berr := e.currentStrategy(context.Background())
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	current, berr := e.loadCurrentShards(context.Background(), e.store.DB())
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	baseline, berr := e.loadCurrentBaseline(context.Background(), e.store.DB())
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	updates := map[string]model.Shard{}
	requestedResident := map[string]bool{}
	for _, shard := range manifest.Shards {
		shard.Status = shard.DesiredStatus()
		updates[shard.ID] = shard
		if shard.Status == model.StatusResident {
			requestedResident[shard.ID] = true
		}
	}
	residency, berr := planResidency(current, updates, requestedResident, baseline, strategy.GPUCapBytes)
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	finalShards := sortedFinalShards(residency.Final)
	precisionReport, berr := numeric.Evaluate(finalShards, strategy, numeric.DefaultElementsPerShard)
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	baselineChanged := recoveryBaselineDigest(baseline, current, strategy) != recoveryBaselineDigest(baseline, residency.Final, strategy)
	reasons := changedDerivations(current, residency.Final, false, baselineChanged)
	for id := range updates {
		if _, ok := reasons[id]; !ok {
			reasons[id] = "IMPORT_REPLACED"
		}
	}
	importReport := map[string]any{
		"commit_id":      req.CommitID,
		"format":         inspectedFormat,
		"format_version": inspectedVersion,
		"verdict":        string(verdict),
		"details":        nonNilDetails(details),
		"record_count":   len(manifest.Shards),
	}
	tx, err := e.store.DB().BeginTx(context.Background(), nil)
	if err != nil {
		return nil, e.failure(req.CommitID, internalError("begin import transaction", err))
	}
	defer tx.Rollback()
	dataVersion, err := persistDataVersion(tx, e.store.Now, req.CommitID, "IMPORT", strategy, residency.Final, baseline, precisionReport, reasons, importReport, nil)
	if err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("persist import", err))
	}
	response := map[string]any{
		"commit_id":         req.CommitID,
		"kind":              "IMPORT",
		"dataset_version":   dataVersion,
		"strategy_version":  strategy.Version,
		"format":            inspectedFormat,
		"format_version":    inspectedVersion,
		"verdict":           string(verdict),
		"details":           nonNilDetails(details),
		"record_count":      len(manifest.Shards),
		"evictions":         nonNilEvictions(residency.Evictions),
		"precision_summary": precisionSummary(precisionReport),
	}
	rawResponse, _ := json.Marshal(response)
	if err := completeCommitTx(tx, req.CommitID, dataVersion, rawResponse); err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("complete import commit", err))
	}
	if err := tx.Commit(); err != nil {
		return nil, e.failure(req.CommitID, internalError("commit import transaction", err))
	}
	return rawResponse, nil
}

func (e *Engine) SubmitIncremental(req IncrementalRequest) (json.RawMessage, *model.BizError) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if strings.TrimSpace(req.CommitID) == "" {
		return nil, model.NewError(model.CodeSpecInvalid, "commit_id is required", map[string]any{"field": "commit_id"})
	}
	hash, berr := e.hashRequest(struct {
		Kind                  string                   `json:"kind"`
		Base                  int64                    `json:"base"`
		GeneratedAt           string                   `json:"generated_at"`
		LossScaleInitialFixed *int64                   `json:"loss_scale_initial_fixed"`
		Changes               []model.IncrementalPatch `json:"changes"`
	}{"INCREMENTAL", req.BaseVersion, req.GeneratedAt, req.LossScaleInitialFixed, req.Changes})
	if berr != nil {
		return nil, berr
	}
	start, berr := e.beginCommit(req.CommitID, "INCREMENTAL", hash, "")
	if berr != nil {
		return nil, berr
	}
	if start.ReplayError != nil {
		return nil, start.ReplayError
	}
	if start.ReplayResponse != nil {
		return start.ReplayResponse, nil
	}
	if len(req.Changes) == 0 {
		return nil, e.failure(req.CommitID, model.NewError(model.CodeSpecInvalid, "incremental fragment must contain at least one shard change", nil))
	}
	strategy, berr := e.currentStrategy(context.Background())
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	if req.BaseVersion != strategy.Version {
		return nil, e.failure(req.CommitID, model.NewError(model.CodeStaleBase, "incremental base_version is not the current strategy version", map[string]any{
			"base_version": req.BaseVersion, "current_version": strategy.Version,
		}))
	}
	current, berr := e.loadCurrentShards(context.Background(), e.store.DB())
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	baseline, berr := e.loadCurrentBaseline(context.Background(), e.store.DB())
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	newStrategy := strategy
	newStrategy.Version = strategy.Version + 1
	newStrategy.GeneratedAt = req.GeneratedAt
	if newStrategy.GeneratedAt == "" {
		newStrategy.GeneratedAt = e.now().Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	lossScaleChanged := false
	if req.LossScaleInitialFixed != nil && *req.LossScaleInitialFixed != strategy.LossScaleInitialFixed {
		newStrategy.LossScaleInitialFixed = *req.LossScaleInitialFixed
		lossScaleChanged = true
	}
	newStrategy.Checksum = newStrategy.ComputedChecksum()
	if err := model.ValidateStrategy(newStrategy); err != nil {
		return nil, e.failure(req.CommitID, err)
	}
	updates := map[string]model.Shard{}
	seen := map[string]bool{}
	for _, patch := range req.Changes {
		if patch.ShardID == "" {
			return nil, e.failure(req.CommitID, model.NewError(model.CodeSpecInvalid, "incremental shard_id is required", nil))
		}
		if seen[patch.ShardID] {
			return nil, e.failure(req.CommitID, model.NewError(model.CodeSpecInvalid, "incremental fragment contains duplicate shard_id", map[string]any{"shard_id": patch.ShardID}))
		}
		seen[patch.ShardID] = true
		base, ok := current[patch.ShardID]
		if !ok {
			return nil, e.failure(req.CommitID, model.NewError(model.CodeSpecInvalid, "incremental shard does not exist", map[string]any{"shard_id": patch.ShardID}))
		}
		updated := model.ApplyPatch(base, patch)
		if err := model.ValidateShard(updated); err != nil {
			return nil, e.failure(req.CommitID, err)
		}
		updated.Status = updated.DesiredStatus()
		updates[updated.ID] = updated
	}
	requestedResident := map[string]bool{}
	for id, shard := range updates {
		if shard.Status == model.StatusResident {
			requestedResident[id] = true
		}
	}
	residency, berr := planResidency(current, updates, requestedResident, baseline, newStrategy.GPUCapBytes)
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	finalShards := sortedFinalShards(residency.Final)
	precisionReport, berr := numeric.Evaluate(finalShards, newStrategy, numeric.DefaultElementsPerShard)
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	baselineChanged := recoveryBaselineDigest(baseline, current, strategy) != recoveryBaselineDigest(baseline, residency.Final, newStrategy)
	reasons := changedDerivations(current, residency.Final, lossScaleChanged, baselineChanged)
	for id := range updates {
		if _, ok := reasons[id]; !ok {
			reasons[id] = "INCREMENTAL_REBUILT"
		}
	}
	tx, err := e.store.DB().BeginTx(context.Background(), nil)
	if err != nil {
		return nil, e.failure(req.CommitID, internalError("begin incremental transaction", err))
	}
	defer tx.Rollback()
	strategyJSON, _ := json.Marshal(newStrategy)
	if _, err := tx.Exec(`INSERT INTO strategy_snapshots(
		version, generated_at, gpu_cap_bytes, write_budget_bytes, time_budget_ms,
		loss_scale_initial_fixed, loss_scale_growth, max_abs_error_fixed, max_rel_error_ppm,
		checksum, data_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		newStrategy.Version, newStrategy.GeneratedAt, newStrategy.GPUCapBytes, newStrategy.WriteBudgetBytes,
		newStrategy.TimeBudgetMillis, newStrategy.LossScaleInitialFixed, newStrategy.LossScaleGrowth,
		newStrategy.MaxAbsErrorFixed, newStrategy.MaxRelErrorPPM, newStrategy.Checksum, string(strategyJSON)); err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("persist incremental strategy", err))
	}
	if err := setMeta(tx, "current_strategy_version", fmt.Sprintf("%d", newStrategy.Version)); err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("update incremental strategy version", err))
	}
	dataVersion, err := persistDataVersion(tx, e.store.Now, req.CommitID, "INCREMENTAL", newStrategy, residency.Final, baseline, precisionReport, reasons, nil, nil)
	if err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("persist incremental fragment", err))
	}
	invalidated := make([]string, 0, len(reasons))
	for id := range reasons {
		invalidated = append(invalidated, id)
	}
	sort.Strings(invalidated)
	response := map[string]any{
		"commit_id":             req.CommitID,
		"kind":                  "INCREMENTAL",
		"dataset_version":       dataVersion,
		"strategy_version":      newStrategy.Version,
		"base_version":          req.BaseVersion,
		"affected_shards":       sortedKeys(seen),
		"derived_invalidated":   invalidated,
		"derived_rebuilt_count": len(reasons),
		"evictions":             nonNilEvictions(residency.Evictions),
		"precision_summary":     precisionSummary(precisionReport),
	}
	rawResponse, _ := json.Marshal(response)
	if err := completeCommitTx(tx, req.CommitID, dataVersion, rawResponse); err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("complete incremental commit", err))
	}
	if err := tx.Commit(); err != nil {
		return nil, e.failure(req.CommitID, internalError("commit incremental transaction", err))
	}
	return rawResponse, nil
}

func (e *Engine) Reside(req ResideRequest) (json.RawMessage, *model.BizError) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if strings.TrimSpace(req.CommitID) == "" {
		return nil, model.NewError(model.CodeSpecInvalid, "commit_id is required", map[string]any{"field": "commit_id"})
	}
	hash, berr := e.hashRequest(struct {
		Kind     string      `json:"kind"`
		Shard    model.Shard `json:"shard"`
		Baseline []string    `json:"baseline"`
	}{"RESIDE", req.Shard, req.Baseline})
	if berr != nil {
		return nil, berr
	}
	start, berr := e.beginCommit(req.CommitID, "RESIDE", hash, "")
	if berr != nil {
		return nil, berr
	}
	if start.ReplayError != nil {
		return nil, start.ReplayError
	}
	if start.ReplayResponse != nil {
		return start.ReplayResponse, nil
	}
	if err := model.ValidateShard(req.Shard); err != nil {
		return nil, e.failure(req.CommitID, err)
	}
	if req.Shard.Pool != model.PoolVRAM {
		return nil, e.failure(req.CommitID, model.NewError(model.CodeSpecInvalid, "reside requires a VRAM destination", map[string]any{
			"shard_id": req.Shard.ID, "pool": req.Shard.Pool,
		}))
	}
	strategy, berr := e.currentStrategy(context.Background())
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	current, berr := e.loadCurrentShards(context.Background(), e.store.DB())
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	baseline, berr := e.loadCurrentBaseline(context.Background(), e.store.DB())
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	if len(req.Baseline) > 0 {
		baseline = append([]string(nil), req.Baseline...)
		sort.Strings(baseline)
	}
	if berr := validateBaseline(baseline, current); berr != nil && req.Shard.ID != "" {
		if _, newShard := current[req.Shard.ID]; !newShard {
			filtered := baseline[:0]
			for _, id := range baseline {
				if id != req.Shard.ID {
					filtered = append(filtered, id)
				}
			}
			baseline = filtered
			berr = validateBaseline(baseline, current)
		}
		if berr != nil {
			return nil, e.failure(req.CommitID, berr)
		}
	}
	newShard := req.Shard
	newShard.Status = model.StatusResident
	updates := map[string]model.Shard{newShard.ID: newShard}
	residency, berr := planResidency(current, updates, map[string]bool{newShard.ID: true}, baseline, strategy.GPUCapBytes)
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	if berr := validateBaseline(baseline, residency.Final); berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	finalShards := sortedFinalShards(residency.Final)
	precisionReport, berr := numeric.Evaluate(finalShards, strategy, numeric.DefaultElementsPerShard)
	if berr != nil {
		return nil, e.failure(req.CommitID, berr)
	}
	baselineChanged := recoveryBaselineDigest(baseline, current, strategy) != recoveryBaselineDigest(baseline, residency.Final, strategy)
	reasons := changedDerivations(current, residency.Final, false, baselineChanged)
	tx, err := e.store.DB().BeginTx(context.Background(), nil)
	if err != nil {
		return nil, e.failure(req.CommitID, internalError("begin reside transaction", err))
	}
	defer tx.Rollback()
	dataVersion, err := persistDataVersion(tx, e.store.Now, req.CommitID, "RESIDE", strategy, residency.Final, baseline, precisionReport, reasons, nil, nil)
	if err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("persist residency", err))
	}
	response := map[string]any{
		"commit_id":             req.CommitID,
		"kind":                  "RESIDE",
		"dataset_version":       dataVersion,
		"strategy_version":      strategy.Version,
		"resided_shard":         newShard.ID,
		"evictions":             nonNilEvictions(residency.Evictions),
		"gpu_usage_bytes":       residency.Usage,
		"gpu_cap_bytes":         strategy.GPUCapBytes,
		"precision_summary":     precisionSummary(precisionReport),
		"derived_rebuilt_count": len(reasons),
	}
	rawResponse, _ := json.Marshal(response)
	if err := completeCommitTx(tx, req.CommitID, dataVersion, rawResponse); err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("complete reside commit", err))
	}
	if err := tx.Commit(); err != nil {
		return nil, e.failure(req.CommitID, internalError("commit reside transaction", err))
	}
	return rawResponse, nil
}

func normalizeImported(manifest *format.Manifest, verdict format.Verdict) []map[string]any {
	details := []map[string]any{}
	for index := range manifest.Shards {
		shard := &manifest.Shards[index]
		if shard.Round == 0 {
			shard.Round = 1
		}
		if shard.Pool == "" {
			shard.Pool = model.PoolPersistent
			details = append(details, map[string]any{
				"kind": "DEFAULT_POOL", "shard_id": shard.ID,
				"applied_pool": string(model.PoolPersistent), "source_version": manifest.Version,
			})
		}
	}
	if manifest.LossScalePrecision == model.LossScalePrecisionFloat32 {
		details = append(details, map[string]any{
			"kind": "LOSS_SCALE_PRECISION_UPCAST", "from": model.LossScalePrecisionFloat32,
			"to": model.LossScalePrecisionFixed64, "loss_scale_fixed": manifest.LossScaleFixed,
		})
		manifest.LossScalePrecision = model.LossScalePrecisionFixed64
	}
	if verdict == format.VerdictDegradeAccept && len(details) == 0 {
		details = append(details, map[string]any{
			"kind": "MATRIX_DEGRADE_ACCEPT", "format": manifest.Format, "source_version": manifest.Version,
		})
	}
	return details
}

func (e *Engine) persistDecisionStandalone(commitID, formatName string, formatVersion int, verdict string, details []map[string]any) {
	raw, _ := json.Marshal(details)
	tx, err := e.store.DB().BeginTx(context.Background(), nil)
	if err != nil {
		return
	}
	defer tx.Rollback()
	_, _ = tx.Exec(`DELETE FROM compatibility_decisions WHERE commit_id = ?`, commitID)
	if _, err := tx.Exec(`INSERT INTO compatibility_decisions(commit_id, format, format_version, verdict, details_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`, commitID, formatName, formatVersion, verdict, string(raw), e.store.Now()); err != nil {
		return
	}
	_ = tx.Commit()
}

func precisionSummary(report numeric.PrecisionReport) map[string]any {
	return map[string]any{
		"within_policy":             report.WithinPolicy,
		"max_abs_error_fixed":       report.MaxAbsErrorFixed,
		"max_rel_error_ppm":         report.MaxRelErrorPPM,
		"cumulative_rounding_error": report.CumulativeRoundingError,
		"report_checksum":           report.ReportChecksum,
	}
}

func sortedKeys(values map[string]bool) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func nonNilEvictions(values []Eviction) []Eviction {
	if values == nil {
		return []Eviction{}
	}
	return values
}

func nonNilDetails(values []map[string]any) []map[string]any {
	if values == nil {
		return []map[string]any{}
	}
	return values
}
