package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strconv"

	"mpckpt/internal/model"
	"mpckpt/internal/numeric"
)

type residencyPlan struct {
	Final     map[string]model.Shard
	Evictions []Eviction
	Undo      undoState
	Usage     int64
}

func planResidency(current map[string]model.Shard, updates map[string]model.Shard, requestedResident map[string]bool, baseline []string, capBytes int64) (residencyPlan, *model.BizError) {
	final := make(map[string]model.Shard, len(current)+len(updates))
	for id, shard := range current {
		final[id] = shard
	}
	for id, shard := range updates {
		final[id] = shard
	}
	protected := make(map[string]bool, len(baseline))
	for _, id := range baseline {
		protected[id] = true
	}
	usage := int64(0)
	for _, shard := range final {
		if shard.Status == model.StatusResident {
			usage += shard.SizeBytes
		}
	}
	if usage <= capBytes {
		return residencyPlan{Final: final, Usage: usage}, nil
	}
	candidates := make([]model.Shard, 0)
	for _, shard := range final {
		if shard.Status != model.StatusResident || protected[shard.ID] || requestedResident[shard.ID] {
			continue
		}
		candidates = append(candidates, shard)
	}
	candidatesBytes := int64(0)
	for _, shard := range candidates {
		candidatesBytes += shard.SizeBytes
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Precision != candidates[j].Precision {
			return candidates[i].Precision == model.PrecisionFP16
		}
		if candidates[i].LastAccess != candidates[j].LastAccess {
			return candidates[i].LastAccess < candidates[j].LastAccess
		}
		return candidates[i].ID < candidates[j].ID
	})
	var evictions []Eviction
	var undo undoState
	for _, candidate := range candidates {
		if usage <= capBytes {
			break
		}
		original := final[candidate.ID]
		eviction := Eviction{
			ShardID:       original.ID,
			FreedBytes:    original.SizeBytes,
			OriginalPool:  original.Pool,
			OriginalState: string(original.Status),
		}
		original.Pool = model.PoolPersistent
		original.Status = model.StatusPersisted
		final[original.ID] = original
		usage -= candidate.SizeBytes
		evictions = append(evictions, eviction)
		undo.Evictions = append(undo.Evictions, struct {
			ShardID string     `json:"shard_id"`
			Pool    model.Pool `json:"pool"`
			Status  string     `json:"status"`
		}{ShardID: candidate.ID, Pool: candidate.Pool, Status: string(candidate.Status)})
	}
	if usage > capBytes {
		return residencyPlan{}, model.NewError(model.CodePoolFull, "VRAM pool cannot accommodate resident shards", map[string]any{
			"required_bytes":            usage - capBytes,
			"additional_bytes_required": usage - capBytes,
			"reclaimable_bytes":         candidatesBytes,
			"gpu_cap_bytes":             capBytes,
			"usage_bytes":               usage,
			"protected_baseline":        baseline,
		})
	}
	return residencyPlan{Final: final, Evictions: evictions, Undo: undo, Usage: usage}, nil
}

func planWrites(queue, incoming []WriteTask, queueCap int, writeBudgetBytes, timeBudgetMillis int64) (map[string]bool, []WriteTask, *model.BizError) {
	tasksByID := make(map[string]WriteTask, len(queue)+len(incoming))
	for _, task := range queue {
		tasksByID[task.ShardID] = task
	}
	for _, task := range incoming {
		tasksByID[task.ShardID] = task
	}
	tasks := make([]WriteTask, 0, len(tasksByID))
	for _, task := range tasksByID {
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].Priority != tasks[j].Priority {
			return tasks[i].Priority > tasks[j].Priority
		}
		if tasks[i].Round != tasks[j].Round {
			return tasks[i].Round < tasks[j].Round
		}
		return tasks[i].ShardID < tasks[j].ShardID
	})
	selected := map[string]bool{}
	var nextQueue []WriteTask
	var usedBytes, usedMillis int64
	for _, task := range tasks {
		duration := int64(1 + task.SizeBytes/4096)
		if usedBytes+task.SizeBytes <= writeBudgetBytes && usedMillis+duration <= timeBudgetMillis {
			selected[task.ShardID] = true
			usedBytes += task.SizeBytes
			usedMillis += duration
			continue
		}
		nextQueue = append(nextQueue, task)
	}
	if len(nextQueue) > queueCap {
		ids := make([]string, 0, len(nextQueue))
		for _, task := range nextQueue {
			ids = append(ids, task.ShardID)
		}
		return nil, nil, model.NewError(model.CodeWriteQueueFull, "bounded write queue is full", map[string]any{
			"queue_capacity":     queueCap,
			"queued_count":       len(nextQueue),
			"queued_shards":      ids,
			"write_budget_bytes": writeBudgetBytes,
			"time_budget_ms":     timeBudgetMillis,
		})
	}
	return selected, nextQueue, nil
}

func baselineSet(baseline []string) map[string]bool {
	out := make(map[string]bool, len(baseline))
	for _, id := range baseline {
		out[id] = true
	}
	return out
}

func sortedFinalShards(final map[string]model.Shard) []model.Shard {
	shards := make([]model.Shard, 0, len(final))
	for _, shard := range final {
		shards = append(shards, shard)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i].ID < shards[j].ID })
	return shards
}

func validateBaseline(baseline []string, shards map[string]model.Shard) *model.BizError {
	seen := map[string]bool{}
	for _, id := range baseline {
		if _, ok := shards[id]; !ok {
			return model.NewError(model.CodeSpecInvalid, "baseline references an unknown shard", map[string]any{
				"shard_id": id,
			})
		}
		if seen[id] {
			return model.NewError(model.CodeSpecInvalid, "baseline contains a duplicate shard", map[string]any{"shard_id": id})
		}
		seen[id] = true
	}
	return nil
}

func changedDerivations(current, final map[string]model.Shard, lossScaleChanged, baselineChanged bool) map[string]string {
	reasons := make(map[string]string)
	for id, shard := range final {
		old, existed := current[id]
		switch {
		case !existed:
			reasons[id] = "NEW_SHARD"
		case lossScaleChanged:
			reasons[id] = "LOSS_SCALE_CHANGED"
		case old.Precision != shard.Precision:
			reasons[id] = "PRECISION_CHANGED"
		case old.Digest != shard.Digest:
			reasons[id] = "MASTER_DIGEST_CHANGED"
		case old.SourceDigest != shard.SourceDigest:
			reasons[id] = "SOURCE_DIGEST_CHANGED"
		case old.MomentumDigest != shard.MomentumDigest:
			reasons[id] = "MOMENTUM_CHANGED"
		case baselineChanged:
			reasons[id] = "RECOVERY_BASELINE_REBUILT"
		}
	}
	return reasons
}

func recoveryBaselineDigest(baseline []string, shards map[string]model.Shard, strategy model.StrategySnapshot) string {
	ordered := append([]string(nil), baseline...)
	sort.Strings(ordered)
	type baselineShard struct {
		ID             string          `json:"id"`
		Precision      model.Precision `json:"precision"`
		SizeBytes      int64           `json:"size_bytes"`
		Digest         string          `json:"digest"`
		SourceDigest   string          `json:"source_digest"`
		MomentumDigest string          `json:"momentum_digest"`
	}
	entries := make([]baselineShard, 0, len(ordered))
	for _, id := range ordered {
		shard, ok := shards[id]
		if !ok {
			entries = append(entries, baselineShard{ID: id, Digest: "<missing>"})
			continue
		}
		entries = append(entries, baselineShard{
			ID: shard.ID, Precision: shard.Precision, SizeBytes: shard.SizeBytes,
			Digest: shard.Digest, SourceDigest: shard.SourceDigest, MomentumDigest: shard.MomentumDigest,
		})
	}
	raw, _ := json.Marshal(struct {
		LossScaleFixed int64           `json:"loss_scale_fixed"`
		Shards         []baselineShard `json:"shards"`
	}{LossScaleFixed: strategy.LossScaleInitialFixed, Shards: entries})
	return model.SHA256Hex(raw)
}

func primaryDigest(shard model.Shard, strategy model.StrategySnapshot, baselineDigest string) string {
	raw, _ := json.Marshal(struct {
		ID           string          `json:"id"`
		Precision    model.Precision `json:"precision"`
		Digest       string          `json:"digest"`
		SourceDigest string          `json:"source_digest"`
		Momentum     string          `json:"momentum_digest"`
		LossScale    int64           `json:"loss_scale_fixed"`
		Baseline     string          `json:"baseline_digest"`
	}{
		ID: shard.ID, Precision: shard.Precision, Digest: shard.Digest,
		SourceDigest: shard.SourceDigest, Momentum: shard.MomentumDigest,
		LossScale: strategy.LossScaleInitialFixed, Baseline: baselineDigest,
	})
	return model.SHA256Hex(raw)
}

func persistDataVersion(
	tx *sql.Tx,
	storeNow func() string,
	commitID, kind string,
	strategy model.StrategySnapshot,
	final map[string]model.Shard,
	baseline []string,
	precisionReport numeric.PrecisionReport,
	reasons map[string]string,
	importReport map[string]any,
	compatibilityDecision map[string]any,
) (int64, error) {
	ctx := context.Background()
	current, err := metaInt64(ctx, tx, "current_dataset_version")
	if err != nil {
		return 0, err
	}
	dataVersion := current + 1
	now := storeNow()
	if _, err := tx.Exec(`INSERT INTO data_versions(version, strategy_version, commit_id, kind, created_at) VALUES (?, ?, ?, ?, ?)`,
		dataVersion, strategy.Version, commitID, kind, now); err != nil {
		return 0, err
	}
	if err := setMeta(tx, "current_dataset_version", strconv.FormatInt(dataVersion, 10)); err != nil {
		return 0, err
	}
	baselineDigest := recoveryBaselineDigest(baseline, final, strategy)
	baselineLookup := baselineSet(baseline)
	shardReports := make(map[string]numeric.ShardPrecisionReport, len(precisionReport.Shards))
	for _, shardReport := range precisionReport.Shards {
		shardReports[shardReport.ShardID] = shardReport
	}
	for _, shard := range sortedFinalShards(final) {
		if _, err := tx.Exec(`INSERT INTO shards(
			id, round_no, precision, size_bytes, digest, pool, last_access, source_digest,
			momentum_digest, priority, status, data_version, write_intent, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?)
			ON CONFLICT(id) DO UPDATE SET round_no=excluded.round_no, precision=excluded.precision,
			size_bytes=excluded.size_bytes, digest=excluded.digest, pool=excluded.pool,
			last_access=excluded.last_access, source_digest=excluded.source_digest,
			momentum_digest=excluded.momentum_digest, priority=excluded.priority,
			status=excluded.status, data_version=excluded.data_version, write_intent='', updated_at=excluded.updated_at`,
			shard.ID, shard.Round, string(shard.Precision), shard.SizeBytes, shard.Digest, string(shard.Pool),
			shard.LastAccess, shard.SourceDigest, shard.MomentumDigest, shard.Priority, string(shard.Status),
			dataVersion, now); err != nil {
			return 0, err
		}
		baselineRef := 0
		if baselineLookup[shard.ID] {
			baselineRef = 1
		}
		if _, err := tx.Exec(`INSERT INTO shard_versions(
			data_version, shard_id, round_no, precision, size_bytes, digest, pool, last_access,
			source_digest, momentum_digest, priority, status, baseline_ref)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			dataVersion, shard.ID, shard.Round, string(shard.Precision), shard.SizeBytes, shard.Digest,
			string(shard.Pool), shard.LastAccess, shard.SourceDigest, shard.MomentumDigest, shard.Priority,
			string(shard.Status), baselineRef); err != nil {
			return 0, err
		}
		if baselineRef != 0 {
			if _, err := tx.Exec(`INSERT INTO baseline_refs(data_version, shard_id) VALUES (?, ?)`, dataVersion, shard.ID); err != nil {
				return 0, err
			}
		}
		shardReport := shardReports[shard.ID]
		consistencyOK := shardReport.MaxAbsErrorFixed <= precisionReport.AllowedAbsErrorFixed && shardReport.MaxRelErrorPPM <= precisionReport.AllowedRelErrorPPM
		derived := primaryDigest(shard, strategy, baselineDigest)
		if _, err := tx.Exec(`INSERT INTO derived_state(data_version, shard_id, primary_digest, consistency_ok, precision_deviation_fixed, baseline_digest, rebuilt_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`, dataVersion, shard.ID, derived, boolInt(consistencyOK), shardReport.MaxAbsErrorFixed, baselineDigest, now); err != nil {
			return 0, err
		}
		if reason, ok := reasons[shard.ID]; ok {
			if _, err := tx.Exec(`INSERT INTO derivation_events(data_version, shard_id, reason, created_at) VALUES (?, ?, ?, ?)`,
				dataVersion, shard.ID, reason, now); err != nil {
				return 0, err
			}
		}
	}
	reportJSON, _ := json.Marshal(precisionReport)
	if _, err := tx.Exec(`INSERT INTO precision_reports(data_version, report_json, within_policy, created_at) VALUES (?, ?, ?, ?)`,
		dataVersion, string(reportJSON), boolInt(precisionReport.WithinPolicy), now); err != nil {
		return 0, err
	}
	if importReport != nil {
		details, _ := json.Marshal(importReport["details"])
		if _, err := tx.Exec(`INSERT INTO import_reports(data_version, commit_id, format, format_version, verdict, details_json, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`, dataVersion, commitID, importReport["format"], importReport["format_version"],
			importReport["verdict"], string(details), now); err != nil {
			return 0, err
		}
	}
	if compatibilityDecision != nil {
		details, _ := json.Marshal(compatibilityDecision["details"])
		if _, err := tx.Exec(`INSERT INTO compatibility_decisions(commit_id, format, format_version, verdict, details_json, created_at)
			VALUES (?, ?, ?, ?, ?, ?)`, commitID, compatibilityDecision["format"], compatibilityDecision["format_version"],
			compatibilityDecision["verdict"], string(details), now); err != nil {
			return 0, err
		}
	}
	return dataVersion, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
