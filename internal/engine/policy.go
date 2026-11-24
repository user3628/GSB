package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"ampcheckpoint/internal/domain"
)

type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (e *Engine) CurrentPolicy(ctx context.Context) (domain.Policy, error) {
	policy, err := loadCurrentPolicy(ctx, e.db)
	if isNotFound(err) {
		return domain.Policy{}, domain.BizError(domain.ErrNotFound, "no policy snapshot is currently committed", nil)
	}
	return policy, err
}

func (e *Engine) Policy(ctx context.Context, version int64) (domain.Policy, error) {
	return loadPolicy(ctx, e.db, version)
}

func (e *Engine) SubmitPolicy(ctx context.Context, input domain.PolicySnapshot) (domain.Policy, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	current, err := loadCurrentPolicy(ctx, e.db)
	currentVersion := int64(0)
	if err == nil {
		currentVersion = current.Version
	} else if !isNotFound(err) {
		return domain.Policy{}, err
	}
	if input.Version <= 0 {
		return domain.Policy{}, domain.BizError(domain.ErrInvalidRequest, "policy version must be positive", map[string]any{"version": input.Version})
	}
	if input.Version <= currentVersion {
		return domain.Policy{}, domain.BizError(domain.ErrStaleSnapshot, "policy snapshot version is not greater than the current version", map[string]any{"submitted_version": input.Version, "current_version": currentVersion})
	}
	if input.GeneratedAt.IsZero() {
		input.GeneratedAt = time.Now().UTC()
	} else {
		input.GeneratedAt = input.GeneratedAt.UTC()
	}
	if err := e.validatePolicy(input); err != nil {
		return domain.Policy{}, err
	}
	computed := domain.PolicyChecksum(input)
	if input.Checksum != "" && input.Checksum != computed {
		return domain.Policy{}, domain.BizError(domain.ErrChecksumMismatch, "policy snapshot checksum mismatch", map[string]any{"provided": input.Checksum, "computed": computed, "version": input.Version})
	}
	input.Checksum = computed
	policy := domain.Policy{PolicySnapshot: input, Source: "snapshot"}
	if err := e.persistPolicy(ctx, policy); err != nil {
		return domain.Policy{}, err
	}
	return policy, nil
}

func (e *Engine) SubmitDelta(ctx context.Context, delta domain.DeltaFragment) (domain.Policy, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	current, err := loadCurrentPolicy(ctx, e.db)
	if isNotFound(err) {
		return domain.Policy{}, domain.BizError(domain.ErrStaleBase, "no policy is currently committed", map[string]any{"base_version": delta.BaseVersion, "current_version": 0})
	}
	if err != nil {
		return domain.Policy{}, err
	}
	if delta.BaseVersion != current.Version {
		return domain.Policy{}, domain.BizError(domain.ErrStaleBase, "delta base version is not the current policy version", map[string]any{"base_version": delta.BaseVersion, "current_version": current.Version})
	}
	if delta.BaseVersion == math.MaxInt64 {
		return domain.Policy{}, domain.BizError(domain.ErrInvalidRequest, "delta base version cannot advance without int64 overflow", map[string]any{"base_version": delta.BaseVersion})
	}
	if delta.GeneratedAt.IsZero() {
		delta.GeneratedAt = time.Now().UTC()
	} else {
		delta.GeneratedAt = delta.GeneratedAt.UTC()
	}
	computedDeltaChecksum := domain.DeltaChecksum(delta)
	if delta.Checksum != "" && delta.Checksum != computedDeltaChecksum {
		return domain.Policy{}, domain.BizError(domain.ErrChecksumMismatch, "delta checksum mismatch", map[string]any{"provided": delta.Checksum, "computed": computedDeltaChecksum, "base_version": delta.BaseVersion})
	}
	if len(delta.Placements) == 0 && delta.LossScale == nil {
		return domain.Policy{}, domain.BizError(domain.ErrInvalidRequest, "delta must contain affected placements or loss scale fields", map[string]any{"base_version": delta.BaseVersion})
	}
	known := e.sample.KnownShardIDs()
	if err := validatePlacements(delta.Placements, known); err != nil {
		return domain.Policy{}, err
	}

	merged := current.PolicySnapshot
	merged.Version = delta.BaseVersion + 1
	merged.GeneratedAt = delta.GeneratedAt
	merged.Checksum = ""
	byShard := make(map[string]domain.Placement, len(merged.Placements)+len(delta.Placements))
	for _, p := range merged.Placements {
		byShard[p.ShardID] = p
	}
	for _, p := range delta.Placements {
		byShard[p.ShardID] = p
	}
	merged.Placements = merged.Placements[:0]
	for _, p := range byShard {
		merged.Placements = append(merged.Placements, p)
	}
	sort.Slice(merged.Placements, func(i, j int) bool { return merged.Placements[i].ShardID < merged.Placements[j].ShardID })

	if delta.LossScale != nil {
		if err := applyLossScalePatch(&merged, *delta.LossScale); err != nil {
			return domain.Policy{}, err
		}
	}
	if err := validatePolicyScalars(merged); err != nil {
		return domain.Policy{}, err
	}
	merged.Checksum = domain.PolicyChecksum(merged)
	policy := domain.Policy{PolicySnapshot: merged, Source: "delta", BaseVersion: delta.BaseVersion}
	if err := e.persistPolicy(ctx, policy); err != nil {
		return domain.Policy{}, err
	}
	return policy, nil
}

func applyLossScalePatch(p *domain.PolicySnapshot, patch domain.LossScalePatch) error {
	growth := p.GrowthFactorFixed
	threshold := p.OverflowThresholdFixed
	if patch.GrowthFactorFixed != nil {
		growth = *patch.GrowthFactorFixed
	}
	if patch.OverflowThresholdFixed != nil {
		threshold = *patch.OverflowThresholdFixed
	}
	current := p.LossScaleFixed
	action := strings.ToLower(strings.TrimSpace(patch.Action))
	attempted := current
	switch action {
	case "", "set":
		if patch.ValueFixed != nil {
			attempted = *patch.ValueFixed
		} else if action == "" {
			attempted = current
		} else {
			return domain.BizError(domain.ErrInvalidRequest, "set loss scale action requires value_fixed", map[string]any{"action": action})
		}
	case "grow":
		if current > 0 && growth > math.MaxInt64/current {
			return domain.BizError(domain.ErrLossScaleOverflow, "loss scale growth overflows int64", map[string]any{"current_fixed": current, "growth_factor_fixed": growth, "overflow_threshold_fixed": threshold})
		}
		attempted = (current * growth) / domain.FixedScale
	case "rollback":
		if growth <= 0 {
			return domain.BizError(domain.ErrInvalidRequest, "growth factor must be positive", map[string]any{"growth_factor_fixed": growth})
		}
		if current > math.MaxInt64/domain.FixedScale {
			return domain.BizError(domain.ErrLossScaleOverflow, "loss scale rollback overflows int64", map[string]any{"current_fixed": current, "growth_factor_fixed": growth})
		}
		attempted = (current * domain.FixedScale) / growth
		if attempted < 1 {
			attempted = 1
		}
	default:
		return domain.BizError(domain.ErrInvalidRequest, "unsupported loss scale action", map[string]any{"action": patch.Action, "allowed": []string{"grow", "rollback", "set"}})
	}
	if growth <= domain.FixedScale {
		return domain.BizError(domain.ErrInvalidRequest, "growth_factor_fixed must be greater than 1.0", map[string]any{"growth_factor_fixed": growth, "scale": domain.FixedScale})
	}
	if threshold <= 0 {
		return domain.BizError(domain.ErrLossScaleOverflow, "overflow threshold must be positive", map[string]any{"overflow_threshold_fixed": threshold})
	}
	if attempted <= 0 {
		return domain.BizError(domain.ErrLossScaleOverflow, "loss scale became non-positive", map[string]any{"current_fixed": current, "attempted_fixed": attempted})
	}
	if attempted > threshold {
		return domain.BizError(domain.ErrLossScaleOverflow, "loss scale exceeds the declared overflow threshold", map[string]any{"current_fixed": current, "growth_factor_fixed": growth, "attempted_fixed": attempted, "overflow_threshold_fixed": threshold})
	}
	p.LossScaleFixed = attempted
	p.GrowthFactorFixed = growth
	p.OverflowThresholdFixed = threshold
	return nil
}

func (e *Engine) validatePolicy(p domain.PolicySnapshot) error {
	if err := validatePolicyScalars(p); err != nil {
		return err
	}
	return validatePlacements(p.Placements, e.sample.KnownShardIDs())
}

func validatePolicyScalars(p domain.PolicySnapshot) error {
	if p.VRAMPoolBytes <= 0 {
		return domain.BizError(domain.ErrInvalidRequest, "vram_pool_bytes must be positive", map[string]any{"vram_pool_bytes": p.VRAMPoolBytes})
	}
	if p.WriteBudgetBytes < 0 {
		return domain.BizError(domain.ErrInvalidRequest, "write_budget_bytes must be non-negative", map[string]any{"write_budget_bytes": p.WriteBudgetBytes})
	}
	if p.RoundTimeBudgetMS <= 0 {
		return domain.BizError(domain.ErrInvalidRequest, "round_time_budget_ms must be positive", map[string]any{"round_time_budget_ms": p.RoundTimeBudgetMS})
	}
	if p.WriteQueueCapacity <= 0 {
		return domain.BizError(domain.ErrInvalidRequest, "write_queue_capacity must be positive", map[string]any{"write_queue_capacity": p.WriteQueueCapacity})
	}
	if p.TaskTimeMS <= 0 {
		return domain.BizError(domain.ErrInvalidRequest, "task_time_ms must be positive", map[string]any{"task_time_ms": p.TaskTimeMS})
	}
	if p.LossScaleFixed <= 0 || p.GrowthFactorFixed <= domain.FixedScale || p.OverflowThresholdFixed <= 0 {
		return domain.BizError(domain.ErrLossScaleOverflow, "invalid loss scale declaration", map[string]any{"loss_scale_fixed": p.LossScaleFixed, "growth_factor_fixed": p.GrowthFactorFixed, "overflow_threshold_fixed": p.OverflowThresholdFixed})
	}
	if p.LossScaleFixed > p.OverflowThresholdFixed {
		return domain.BizError(domain.ErrLossScaleOverflow, "initial loss scale exceeds overflow threshold", map[string]any{"loss_scale_fixed": p.LossScaleFixed, "overflow_threshold_fixed": p.OverflowThresholdFixed})
	}
	return nil
}

func validatePlacements(placements []domain.Placement, known map[string]struct{}) error {
	seen := make(map[string]struct{}, len(placements))
	for i, placement := range placements {
		if strings.TrimSpace(placement.ShardID) == "" {
			return domain.BizError(domain.ErrPartitionInvalid, "placement shard_id is required", map[string]any{"index": i})
		}
		if _, ok := known[placement.ShardID]; !ok {
			return domain.BizError(domain.ErrPartitionInvalid, "placement references an unknown shard", map[string]any{"index": i, "shard_id": placement.ShardID})
		}
		if _, duplicate := seen[placement.ShardID]; duplicate {
			return domain.BizError(domain.ErrPartitionInvalid, "duplicate shard placement", map[string]any{"index": i, "shard_id": placement.ShardID})
		}
		seen[placement.ShardID] = struct{}{}
		if err := domain.ValidatePrecisionPool(placement.Precision, placement.Pool); err != nil {
			return err
		}
		if placement.LastAccessNS < 0 {
			return domain.BizError(domain.ErrPartitionInvalid, "last_access_ns must be non-negative", map[string]any{"index": i, "shard_id": placement.ShardID, "last_access_ns": placement.LastAccessNS})
		}
	}
	return nil
}

func (e *Engine) persistPolicy(ctx context.Context, policy domain.Policy) error {
	intentTx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	opID, err := insertIntent(ctx, intentTx, "POLICY", "STAGED", 0, policy.Version, map[string]any{"policy_version": policy.Version, "source": policy.Source, "base_version": policy.BaseVersion})
	if err != nil {
		intentTx.Rollback()
		return err
	}
	if err := intentTx.Commit(); err != nil {
		return err
	}
	finalTx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		e.abortIntent(opID, err.Error())
		return err
	}
	if err := insertPolicyTx(ctx, finalTx, policy); err != nil {
		finalTx.Rollback()
		e.abortIntent(opID, err.Error())
		return err
	}
	if err := finishIntent(ctx, finalTx, opID, "COMMITTED", "COMMITTED", 0); err != nil {
		finalTx.Rollback()
		e.abortIntent(opID, err.Error())
		return err
	}
	if err := finalTx.Commit(); err != nil {
		e.abortIntent(opID, err.Error())
		return err
	}
	return nil
}

func insertPolicyTx(ctx context.Context, tx *sql.Tx, policy domain.Policy) error {
	placements, err := json.Marshal(policy.Placements)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO policy_versions(version, generated_at, vram_pool_bytes, write_budget_bytes, round_time_budget_ms, write_queue_capacity, task_time_ms, loss_scale_fixed, growth_factor_fixed, overflow_threshold_fixed, placements_json, checksum, source, base_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		policy.Version, policy.GeneratedAt.UTC().Format(time.RFC3339Nano), policy.VRAMPoolBytes, policy.WriteBudgetBytes,
		policy.RoundTimeBudgetMS, policy.WriteQueueCapacity, policy.TaskTimeMS, policy.LossScaleFixed,
		policy.GrowthFactorFixed, policy.OverflowThresholdFixed, string(placements), policy.Checksum, policy.Source, policy.BaseVersion)
	return err
}

func loadCurrentPolicy(ctx context.Context, q queryRower) (domain.Policy, error) {
	var version int64
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM policy_versions`).Scan(&version); err != nil {
		return domain.Policy{}, err
	}
	if version == 0 {
		return domain.Policy{}, sql.ErrNoRows
	}
	return loadPolicy(ctx, q, version)
}

func loadPolicy(ctx context.Context, q queryRower, version int64) (domain.Policy, error) {
	var p domain.Policy
	var generatedAt, placementsJSON string
	err := q.QueryRowContext(ctx, `SELECT version, generated_at, vram_pool_bytes, write_budget_bytes, round_time_budget_ms, write_queue_capacity, task_time_ms, loss_scale_fixed, growth_factor_fixed, overflow_threshold_fixed, placements_json, checksum, source, base_version FROM policy_versions WHERE version=?`, version).Scan(
		&p.Version, &generatedAt, &p.VRAMPoolBytes, &p.WriteBudgetBytes, &p.RoundTimeBudgetMS, &p.WriteQueueCapacity, &p.TaskTimeMS, &p.LossScaleFixed,
		&p.GrowthFactorFixed, &p.OverflowThresholdFixed, &placementsJSON, &p.Checksum, &p.Source, &p.BaseVersion)
	if err != nil {
		if err == sql.ErrNoRows {
			return domain.Policy{}, domain.BizError(domain.ErrNotFound, "policy version not found", map[string]any{"version": version})
		}
		return domain.Policy{}, err
	}
	p.GeneratedAt = parseTime(generatedAt)
	if err := json.Unmarshal([]byte(placementsJSON), &p.Placements); err != nil {
		return domain.Policy{}, fmt.Errorf("decode policy placements: %w", err)
	}
	if got := domain.PolicyChecksum(p.PolicySnapshot); got != p.Checksum {
		return domain.Policy{}, domain.BizError(domain.ErrChecksumMismatch, "stored policy checksum does not match its facts", map[string]any{"version": version, "stored": p.Checksum, "computed": got})
	}
	return p, nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if err == sql.ErrNoRows {
		return true
	}
	be, ok := err.(*domain.BusinessError)
	return ok && be.Code == domain.ErrNotFound
}
