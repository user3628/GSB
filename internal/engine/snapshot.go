package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"checkpointengine/internal/model"
)

func (e *Engine) SubmitSnapshot(ctx context.Context, req model.SnapshotRequest) (*model.Snapshot, error) {
	snap, err := normalizeSnapshot(req, e.now())
	if err != nil {
		return nil, err
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	state, err := loadStateDB(ctx, e.store.DB)
	if err != nil {
		return nil, err
	}
	if snap.Version <= state.CurrentPolicyVersion {
		return nil, model.WithDetails(model.NewAPIError(model.CodeStaleSnapshot, "snapshot version is not strictly newer than the committed policy"),
			map[string]any{"submitted_version": snap.Version, "current_version": state.CurrentPolicyVersion})
	}
	if err := validateResidency(snap); err != nil {
		return nil, err
	}

	all, err := loadAllShards(ctx, e.store.DB)
	if err != nil {
		return nil, err
	}
	incoming := make([]model.Shard, 0)
	vramSet := stringSet(snap.VRAMResidency)
	persistedSet := stringSet(snap.PersistedResidency)
	for _, s := range all {
		target := s.DesiredPool
		if vramSet[s.ShardID] {
			target = model.PoolVRAM
		} else if persistedSet[s.ShardID] {
			target = model.PoolPersisted
		}
		if target != s.DesiredPool || (target == model.PoolVRAM && s.Location != model.PoolVRAM) || (target == model.PoolPersisted && s.Location == model.PoolVRAM) {
			s.DesiredPool = target
			incoming = append(incoming, s)
		}
	}
	placement, err := placeShards(all, incoming, snap.VRAMLimitBytes)
	if err != nil {
		return nil, err
	}
	// A policy can change the loss scale or the drift bounds. Recompute the
	// complete precision view under the new policy before it becomes visible.
	placement.Shards, err = resolveRelationships(placement.Shards)
	if err != nil {
		return nil, err
	}
	precisionResults, precisionReport, err := e.computeAndCheckPrecision(placement.Shards, snap, snap.LossScaleInitial, e.nowText())
	if err != nil {
		return nil, err
	}
	if !precisionReport.Passed {
		apiErr := model.NewAPIError(model.CodePrecisionDrift, "policy snapshot rejected because existing numeric pairs exceed its declared drift bounds")
		apiErr.Details = map[string]any{"precision": precisionReport}
		return nil, apiErr
	}

	newRevision := state.Revision + 1
	now := e.nowText()
	specs := []intentSpec{{Kind: "SNAPSHOT", SubmissionID: fmt.Sprintf("snapshot:%d", snap.Version), After: snap}}
	for _, ev := range placement.Evictions {
		before := placement.EvictionFrom[shardKey(ev.Round, ev.ShardID)]
		specs = append(specs, intentSpec{Kind: "EVICT", SubmissionID: fmt.Sprintf("snapshot:%d", snap.Version),
			Before: before, After: evictionIntentJSON{Round: ev.Round, ShardID: ev.ShardID, Before: before, AfterPool: model.PoolPersisted, Reason: ev.Reason}})
	}
	intentIDs, err := e.insertIntents(ctx, specs)
	if err != nil {
		return nil, err
	}
	if err := e.hitFailpoint("after_snapshot_intents"); err != nil {
		return nil, err
	}

	tx, err := e.store.DB.BeginTx(ctx, nil)
	if err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	defer tx.Rollback()

	canonical, err := json.Marshal(snap)
	if err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO strategy_snapshots(version, generated_at,
		vram_limit_bytes, write_budget_bytes_per_round, time_budget_ms_per_round,
		write_queue_capacity, loss_scale_initial, loss_scale_growth,
		precision_abs_limit_nanos, precision_rel_limit_ppm, write_preference,
		checksum, canonical_json, submitted_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		snap.Version, snap.GeneratedAt, snap.VRAMLimitBytes, snap.WriteBudgetBytesPerRound,
		snap.TimeBudgetMS, snap.WriteQueueCapacity, snap.LossScaleInitial, snap.LossScaleGrowth,
		snap.PrecisionAbsLimitNanos, snap.PrecisionRelLimitPPM, snap.WritePreference,
		snap.Checksum, string(canonical), now); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	for _, id := range snap.VRAMResidency {
		if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO snapshot_residency(policy_version, shard_id, target_pool) VALUES(?,?,?)`, snap.Version, id, model.PoolVRAM); err != nil {
			e.abortIntents(ctx, intentIDs)
			return nil, err
		}
	}
	for _, id := range snap.PersistedResidency {
		if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO snapshot_residency(policy_version, shard_id, target_pool) VALUES(?,?,?)`, snap.Version, id, model.PoolPersisted); err != nil {
			e.abortIntents(ctx, intentIDs)
			return nil, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE engine_state SET revision=?, current_policy_version=?, loss_scale=?, loss_scale_growth=? WHERE id=1`,
		newRevision, snap.Version, snap.LossScaleInitial, snap.LossScaleGrowth); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	for _, s := range placement.Shards {
		old, existed := findShard(all, s.Round, s.ShardID)
		if !existed {
			continue
		}
		if old.Location == s.Location && old.DesiredPool == s.DesiredPool {
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE shards SET location=?, desired_pool=?, revision=?, updated_at=? WHERE round=? AND shard_id=?`,
			s.Location, s.DesiredPool, newRevision, now, s.Round, s.ShardID); err != nil {
			e.abortIntents(ctx, intentIDs)
			return nil, err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM precision_results`); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if _, err := savePrecisionResultsTx(ctx, tx, precisionResults); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if _, err := rebuildPoolUsageTx(ctx, tx, newRevision, now); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if _, err := rebuildAllDerivedTx(ctx, tx, snap, newRevision, now); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if err := markIntentsApplied(ctx, tx, intentIDs, now); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	snap.StateRevision = newRevision
	return &snap, nil
}

func normalizeSnapshot(req model.SnapshotRequest, now time.Time) (model.Snapshot, error) {
	var s model.Snapshot
	if req.Version <= 0 {
		return s, model.NewAPIError(model.CodeSnapshotInvalid, "snapshot version must be a positive integer")
	}
	s.Version = req.Version
	if strings.TrimSpace(req.GeneratedAt) == "" {
		s.GeneratedAt = now.UTC().Format(time.RFC3339Nano)
	} else {
		t, err := time.Parse(time.RFC3339Nano, req.GeneratedAt)
		if err != nil {
			return s, model.WrapAPIError(model.CodeSnapshotInvalid, "generated_at must be RFC3339", err)
		}
		s.GeneratedAt = t.UTC().Format(time.RFC3339Nano)
	}
	var err error
	if s.VRAMLimitBytes, err = reqInt64(req.VRAMLimitBytes, 0); err != nil {
		return s, model.WrapAPIError(model.CodeSnapshotInvalid, "vram_limit_bytes must be a non-negative integer", err)
	}
	if s.VRAMLimitBytes < 0 {
		return s, model.NewAPIError(model.CodeSnapshotInvalid, "vram_limit_bytes must be non-negative")
	}
	if s.WriteBudgetBytesPerRound, err = reqInt64(req.WriteBudgetBytesPerRound, 0); err != nil || s.WriteBudgetBytesPerRound < 0 {
		return s, model.NewAPIError(model.CodeSnapshotInvalid, "write_budget_bytes_per_round must be a non-negative integer")
	}
	if s.TimeBudgetMS, err = reqInt64(req.TimeBudgetMS, 0); err != nil || s.TimeBudgetMS < 0 {
		return s, model.NewAPIError(model.CodeSnapshotInvalid, "time_budget_ms_per_round must be a non-negative integer")
	}
	s.WriteQueueCapacity = 256
	if req.WriteQueueCapacity != nil {
		if *req.WriteQueueCapacity < 0 {
			return s, model.NewAPIError(model.CodeSnapshotInvalid, "write_queue_capacity must be non-negative")
		}
		s.WriteQueueCapacity = *req.WriteQueueCapacity
	}
	s.LossScaleInitial = "1"
	if len(strings.TrimSpace(string(req.LossScaleInitial))) > 0 && string(req.LossScaleInitial) != "null" {
		if s.LossScaleInitial, err = model.DecimalValue(req.LossScaleInitial); err != nil {
			return s, model.WrapAPIError(model.CodeSnapshotInvalid, "invalid loss_scale_initial", err)
		}
	}
	s.LossScaleGrowth = "1"
	if len(strings.TrimSpace(string(req.LossScaleGrowth))) > 0 && string(req.LossScaleGrowth) != "null" {
		if s.LossScaleGrowth, err = model.DecimalValue(req.LossScaleGrowth); err != nil {
			return s, model.WrapAPIError(model.CodeSnapshotInvalid, "invalid loss_scale_growth", err)
		}
	}
	s.PrecisionAbsLimitNanos = 1_000_000_000
	if len(strings.TrimSpace(string(req.PrecisionAbsLimitNanos))) > 0 && string(req.PrecisionAbsLimitNanos) != "null" {
		if s.PrecisionAbsLimitNanos, err = reqInt64(req.PrecisionAbsLimitNanos, 0); err != nil || s.PrecisionAbsLimitNanos < 0 {
			return s, model.NewAPIError(model.CodeSnapshotInvalid, "precision_abs_limit_nanos must be a non-negative integer")
		}
	}
	s.PrecisionRelLimitPPM = 1_000_000
	if len(strings.TrimSpace(string(req.PrecisionRelLimitPPM))) > 0 && string(req.PrecisionRelLimitPPM) != "null" {
		if s.PrecisionRelLimitPPM, err = reqInt64(req.PrecisionRelLimitPPM, 0); err != nil || s.PrecisionRelLimitPPM < 0 {
			return s, model.NewAPIError(model.CodeSnapshotInvalid, "precision_rel_limit_ppm must be a non-negative integer")
		}
	}
	s.WritePreference = strings.ToUpper(strings.TrimSpace(req.WritePreference))
	if s.WritePreference == "" {
		s.WritePreference = model.WritePreferenceFP32First
	}
	if s.WritePreference != model.WritePreferenceFP32First && s.WritePreference != model.WritePreferenceFP16First {
		return s, model.NewAPIError(model.CodeSnapshotInvalid, "write_preference must be FP32_FIRST or FP16_FIRST")
	}
	if req.Residency != nil {
		s.VRAMResidency = append(s.VRAMResidency, req.Residency.VRAM...)
		s.PersistedResidency = append(s.PersistedResidency, req.Residency.Persisted...)
	}
	s.VRAMResidency = append(s.VRAMResidency, req.VRAMResidency...)
	s.PersistedResidency = append(s.PersistedResidency, req.PersistedResidency...)
	sort.Strings(s.VRAMResidency)
	sort.Strings(s.PersistedResidency)
	checksum, err := model.CanonicalSnapshotChecksum(s)
	if err != nil {
		return s, err
	}
	if req.Checksum != "" && !strings.EqualFold(strings.TrimSpace(req.Checksum), checksum) {
		return s, model.WithDetails(model.NewAPIError(model.CodeSnapshotInvalid, "snapshot checksum does not match canonical contents"),
			map[string]any{"expected_checksum": checksum, "submitted_checksum": req.Checksum})
	}
	s.Checksum = checksum
	return s, nil
}

func validateResidency(s model.Snapshot) error {
	vram := stringSet(s.VRAMResidency)
	for _, id := range s.PersistedResidency {
		if vram[id] {
			return model.WithDetails(model.NewAPIError(model.CodeSnapshotInvalid, "a shard cannot be declared in both residency pools"),
				map[string]any{"shard_id": id})
		}
	}
	return nil
}

func stringSet(values []string) map[string]bool {
	m := make(map[string]bool, len(values))
	for _, v := range values {
		if v != "" {
			m[v] = true
		}
	}
	return m
}

func reqInt64(raw json.RawMessage, def int64) (int64, error) {
	if len(strings.TrimSpace(string(raw))) == 0 || string(raw) == "null" {
		return def, nil
	}
	return model.DecimalToInt64(raw, "value")
}

func findShard(shards []model.Shard, round int, id string) (model.Shard, bool) {
	for _, s := range shards {
		if s.Round == round && s.ShardID == id {
			return s, true
		}
	}
	return model.Shard{}, false
}
