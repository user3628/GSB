package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"ampcheckpoint/internal/domain"
)

func (e *Engine) CheckpointMeta(ctx context.Context, version int64) (domain.CheckpointMeta, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.CheckpointMeta{}, err
	}
	defer tx.Rollback()
	meta, err := loadCheckpointMeta(ctx, tx, version)
	if err != nil {
		return domain.CheckpointMeta{}, err
	}
	return meta, tx.Commit()
}

func (e *Engine) LatestCheckpointMeta(ctx context.Context) (domain.CheckpointMeta, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.CheckpointMeta{}, err
	}
	defer tx.Rollback()
	version, err := latestVersion(ctx, tx, "checkpoints")
	if err != nil {
		return domain.CheckpointMeta{}, err
	}
	if version == 0 {
		return domain.CheckpointMeta{}, domain.BizError(domain.ErrNotFound, "no checkpoint has been committed", nil)
	}
	meta, err := loadCheckpointMeta(ctx, tx, version)
	if err != nil {
		return domain.CheckpointMeta{}, err
	}
	return meta, tx.Commit()
}

func loadCheckpointMeta(ctx context.Context, q queryRower, version int64) (domain.CheckpointMeta, error) {
	var m domain.CheckpointMeta
	var committedAt string
	err := q.QueryRowContext(ctx, `SELECT version, policy_version, round, committed_at, vram_limit_bytes, checksum FROM checkpoints WHERE version=?`, version).Scan(
		&m.Version, &m.PolicyVersion, &m.Round, &committedAt, &m.VRAMLimitBytes, &m.Checksum)
	if err == sql.ErrNoRows {
		return domain.CheckpointMeta{}, domain.BizError(domain.ErrNotFound, "checkpoint version not found", map[string]any{"version": version})
	}
	if err != nil {
		return domain.CheckpointMeta{}, err
	}
	m.CommittedAt = parseTime(committedAt)
	return m, nil
}

func latestVersion(ctx context.Context, q queryRower, table string) (int64, error) {
	var version int64
	query := fmt.Sprintf(`SELECT COALESCE(MAX(version),0) FROM %s`, table)
	if err := q.QueryRowContext(ctx, query).Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func (e *Engine) Shards(ctx context.Context, version int64) (domain.ShardsResponse, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.ShardsResponse{}, err
	}
	defer tx.Rollback()
	meta, err := loadCheckpointMeta(ctx, tx, version)
	if err != nil {
		return domain.ShardsResponse{}, err
	}
	copies, err := loadCopies(ctx, tx, version)
	if err != nil {
		return domain.ShardsResponse{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.ShardsResponse{}, err
	}
	return domain.ShardsResponse{Version: meta.Version, PolicyVersion: meta.PolicyVersion, Round: meta.Round, Copies: copies}, nil
}

func loadCopies(ctx context.Context, q *sql.Tx, version int64) ([]domain.Copy, error) {
	rows, err := q.QueryContext(ctx, `SELECT copy_id, shard_id, round, precision, size_bytes, digest, master_copy_id, master_digest, momentum_digest, pool, status, last_access_ns, priority, loss_scale_fixed, restoration, write_state, values_fixed_json, fp16_bits_json FROM checkpoint_copies WHERE checkpoint_version=? ORDER BY copy_id`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Copy
	for rows.Next() {
		var c domain.Copy
		var precision, pool, status, valuesJSON, bitsJSON string
		if err := rows.Scan(&c.CopyID, &c.ShardID, &c.Round, &precision, &c.SizeBytes, &c.Digest, &c.MasterCopyID, &c.MasterDigest, &c.MomentumDigest, &pool, &status, &c.LastAccessNS, &c.Priority, &c.LossScaleFixed, &c.Restoration, &c.WriteState, &valuesJSON, &bitsJSON); err != nil {
			return nil, err
		}
		c.Precision = domain.Precision(precision)
		c.Pool = domain.Pool(pool)
		c.Status = domain.CopyStatus(status)
		if err := json.Unmarshal([]byte(valuesJSON), &c.ValuesFixed); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(bitsJSON), &c.FP16Bits); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (e *Engine) Pools(ctx context.Context, version int64) ([]domain.PoolSummary, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := loadCheckpointMeta(ctx, tx, version); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT pool, copy_count, total_bytes, limit_bytes, remaining_bytes FROM checkpoint_pools WHERE checkpoint_version=? ORDER BY pool`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.PoolSummary
	for rows.Next() {
		var p domain.PoolSummary
		var pool string
		if err := rows.Scan(&pool, &p.CopyCount, &p.TotalBytes, &p.LimitBytes, &p.RemainingBytes); err != nil {
			return nil, err
		}
		p.Pool = domain.Pool(pool)
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, tx.Commit()
}

func (e *Engine) Precision(ctx context.Context, version int64) (domain.PrecisionReport, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.PrecisionReport{}, err
	}
	defer tx.Rollback()
	meta, err := loadCheckpointMeta(ctx, tx, version)
	if err != nil {
		return domain.PrecisionReport{}, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT shard_id, fp16_copy_id, master_copy_id, element_count, loss_scale_fixed, restoration, max_abs_deviation_fixed, max_relative_deviation_ppm, cumulative_abs_deviation_fixed, consistency_ok, elements_json FROM checkpoint_precision WHERE checkpoint_version=? ORDER BY shard_id`, version)
	if err != nil {
		return domain.PrecisionReport{}, err
	}
	defer rows.Close()
	report := domain.PrecisionReport{Version: meta.Version, PolicyVersion: meta.PolicyVersion, Shards: []domain.PrecisionShardReport{}}
	for rows.Next() {
		var r domain.PrecisionShardReport
		var consistency int
		var elementsJSON string
		if err := rows.Scan(&r.ShardID, &r.FP16CopyID, &r.MasterCopyID, &r.ElementCount, &r.LossScaleFixed, &r.Restoration, &r.MaxAbsDeviationFixed, &r.MaxRelativeDeviationPPM, &r.CumulativeAbsDeviationFixed, &consistency, &elementsJSON); err != nil {
			return domain.PrecisionReport{}, err
		}
		r.ConsistencyOK = consistency != 0
		if err := json.Unmarshal([]byte(elementsJSON), &r.Elements); err != nil {
			return domain.PrecisionReport{}, err
		}
		report.Shards = append(report.Shards, r)
		if r.CumulativeAbsDeviationFixed > math.MaxInt64-report.CumulativeAbsDeviationFixed {
			return domain.PrecisionReport{}, domain.BizError(domain.ErrInternal, "precision cumulative error overflows int64", map[string]any{"shard_id": r.ShardID})
		}
		report.CumulativeAbsDeviationFixed += r.CumulativeAbsDeviationFixed
		if r.MaxAbsDeviationFixed > report.MaxAbsDeviationFixed {
			report.MaxAbsDeviationFixed = r.MaxAbsDeviationFixed
		}
		if r.MaxRelativeDeviationPPM > report.MaxRelativeDeviationPPM {
			report.MaxRelativeDeviationPPM = r.MaxRelativeDeviationPPM
		}
	}
	if err := rows.Err(); err != nil {
		return domain.PrecisionReport{}, err
	}
	report.CheckedShardCount = len(report.Shards)
	return report, tx.Commit()
}

func (e *Engine) Derived(ctx context.Context, version int64) ([]domain.DerivedCopy, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := loadCheckpointMeta(ctx, tx, version); err != nil {
		return nil, err
	}
	out, err := loadDerived(ctx, tx, version)
	if err != nil {
		return nil, err
	}
	return out, tx.Commit()
}

func loadDerived(ctx context.Context, q *sql.Tx, version int64) ([]domain.DerivedCopy, error) {
	rows, err := q.QueryContext(ctx, `SELECT copy_id, master_derived_digest, consistency_ok, vram_estimate_bytes, recovery_baseline_json, dependency_refs_json FROM checkpoint_derived WHERE checkpoint_version=? ORDER BY copy_id`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.DerivedCopy
	for rows.Next() {
		var d domain.DerivedCopy
		var consistency int
		var baselineJSON, depsJSON string
		if err := rows.Scan(&d.CopyID, &d.MasterDerivedDigest, &consistency, &d.VRAMEstimateBytes, &baselineJSON, &depsJSON); err != nil {
			return nil, err
		}
		d.ConsistencyOK = consistency != 0
		if err := json.Unmarshal([]byte(baselineJSON), &d.RecoveryBaseline); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(depsJSON), &d.DependencyRefs); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (e *Engine) Recovery(ctx context.Context, version int64) (domain.RecoveryReport, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.RecoveryReport{}, err
	}
	defer tx.Rollback()
	meta, err := loadCheckpointMeta(ctx, tx, version)
	if err != nil {
		return domain.RecoveryReport{}, err
	}
	derived, err := loadDerived(ctx, tx, version)
	if err != nil {
		return domain.RecoveryReport{}, err
	}
	var copyCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoint_copies WHERE checkpoint_version=?`, version).Scan(&copyCount); err != nil {
		return domain.RecoveryReport{}, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT pool, total_bytes FROM checkpoint_pools WHERE checkpoint_version=?`, version)
	if err != nil {
		return domain.RecoveryReport{}, err
	}
	report := domain.RecoveryReport{Version: meta.Version, PolicyVersion: meta.PolicyVersion, Round: meta.Round, CommittedOnly: true, CopyCount: copyCount, Checksum: meta.Checksum, Baselines: derived, AbortedOperations: []domain.AbortedOperation{}}
	for rows.Next() {
		var pool string
		var bytes int64
		if err := rows.Scan(&pool, &bytes); err != nil {
			rows.Close()
			return domain.RecoveryReport{}, err
		}
		switch domain.Pool(pool) {
		case domain.PoolVRAM:
			report.VRAMTotalBytes = bytes
		case domain.PoolPersistent:
			report.PersistentTotalBytes = bytes
		}
	}
	if err := rows.Close(); err != nil {
		return domain.RecoveryReport{}, err
	}
	aborted, err := loadAbortedOperations(ctx, tx, version)
	if err != nil {
		return domain.RecoveryReport{}, err
	}
	report.AbortedOperations = aborted
	return report, tx.Commit()
}

func loadAbortedOperations(ctx context.Context, q *sql.Tx, baseVersion int64) ([]domain.AbortedOperation, error) {
	rows, err := q.QueryContext(ctx, `SELECT op_id, phase, base_checkpoint_version, recovered_at, pending_shards_json, aborted_evictions_json FROM recovery_log WHERE base_checkpoint_version=? ORDER BY id`, baseVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.AbortedOperation
	for rows.Next() {
		var op domain.AbortedOperation
		var recoveredAt, pendingJSON, evictionsJSON string
		if err := rows.Scan(&op.OperationID, &op.Phase, &op.BaseCheckpointVersion, &recoveredAt, &pendingJSON, &evictionsJSON); err != nil {
			return nil, err
		}
		op.RecoveredAt = parseTime(recoveredAt)
		if err := json.Unmarshal([]byte(pendingJSON), &op.PendingShards); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(evictionsJSON), &op.AbortedEvictions); err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

func (e *Engine) CheckpointResult(ctx context.Context, version int64) (domain.CheckpointResult, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.CheckpointResult{}, err
	}
	defer tx.Rollback()
	var result domain.CheckpointResult
	var committedAt, writtenJSON, skippedJSON, queuedJSON, evictionsJSON string
	var budgetBytes, queueCap int64
	err = tx.QueryRowContext(ctx, `SELECT version, policy_version, round, committed_at, write_budget_bytes, write_used_bytes, write_remaining_bytes, time_budget_ms, time_used_ms, time_remaining_ms, write_queue_capacity, written_json, skipped_json, queued_json, evictions_json FROM checkpoints WHERE version=?`, version).Scan(
		&result.Version, &result.PolicyVersion, &result.Round, &committedAt, &budgetBytes, &result.Writes.UsedBytes, &result.Writes.RemainingBytes,
		&result.Writes.TimeBudgetMS, &result.Writes.TimeUsedMS, &result.Writes.TimeRemainingMS, &queueCap, &writtenJSON, &skippedJSON, &queuedJSON, &evictionsJSON)
	if err == sql.ErrNoRows {
		return domain.CheckpointResult{}, domain.BizError(domain.ErrNotFound, "checkpoint version not found", map[string]any{"version": version})
	}
	if err != nil {
		return domain.CheckpointResult{}, err
	}
	result.CommittedAt = parseTime(committedAt)
	result.Writes.BudgetBytes = budgetBytes
	result.Writes.QueueCapacity = int(queueCap)
	if err := json.Unmarshal([]byte(writtenJSON), &result.Writes.Written); err != nil {
		return domain.CheckpointResult{}, err
	}
	if err := json.Unmarshal([]byte(skippedJSON), &result.Writes.Skipped); err != nil {
		return domain.CheckpointResult{}, err
	}
	if err := json.Unmarshal([]byte(queuedJSON), &result.Writes.Queued); err != nil {
		return domain.CheckpointResult{}, err
	}
	if err := json.Unmarshal([]byte(evictionsJSON), &result.Evictions); err != nil {
		return domain.CheckpointResult{}, err
	}
	pools, err := loadPoolRows(ctx, tx, version)
	if err != nil {
		return domain.CheckpointResult{}, err
	}
	result.Pools = pools
	return result, tx.Commit()
}

func loadPoolRows(ctx context.Context, q *sql.Tx, version int64) ([]domain.PoolSummary, error) {
	rows, err := q.QueryContext(ctx, `SELECT pool, copy_count, total_bytes, limit_bytes, remaining_bytes FROM checkpoint_pools WHERE checkpoint_version=? ORDER BY pool`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.PoolSummary
	for rows.Next() {
		var p domain.PoolSummary
		var pool string
		if err := rows.Scan(&pool, &p.CopyCount, &p.TotalBytes, &p.LimitBytes, &p.RemainingBytes); err != nil {
			return nil, err
		}
		p.Pool = domain.Pool(pool)
		out = append(out, p)
	}
	return out, rows.Err()
}

func (e *Engine) Status(ctx context.Context) (map[string]any, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	policyVersion, err := latestVersion(ctx, tx, "policy_versions")
	if err != nil {
		return nil, err
	}
	checkpointVersion, err := latestVersion(ctx, tx, "checkpoints")
	if err != nil {
		return nil, err
	}
	var running int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operations WHERE status='RUNNING'`).Scan(&running); err != nil {
		return nil, err
	}
	var recovered int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM recovery_log`).Scan(&recovered); err != nil {
		return nil, err
	}
	status := map[string]any{
		"policy_version":             policyVersion,
		"checkpoint_version":         checkpointVersion,
		"running_intents":            running,
		"recovered_intents":          recovered,
		"recovery_only_committed":    true,
		"seed":                       e.sample.Seed(),
		"sample_dir":                 e.sample.Dir(),
		"supported_write_queue_full": true,
	}
	return status, tx.Commit()
}

func (e *Engine) AllRecoveryOperations(ctx context.Context) ([]domain.AbortedOperation, error) {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT op_id, phase, base_checkpoint_version, recovered_at, pending_shards_json, aborted_evictions_json FROM recovery_log ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.AbortedOperation
	for rows.Next() {
		var op domain.AbortedOperation
		var recoveredAt, pendingJSON, evictionsJSON string
		if err := rows.Scan(&op.OperationID, &op.Phase, &op.BaseCheckpointVersion, &recoveredAt, &pendingJSON, &evictionsJSON); err != nil {
			return nil, err
		}
		op.RecoveredAt = parseTime(recoveredAt)
		_ = json.Unmarshal([]byte(pendingJSON), &op.PendingShards)
		_ = json.Unmarshal([]byte(evictionsJSON), &op.AbortedEvictions)
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RecoveredAt.Before(out[j].RecoveredAt) })
	return out, rows.Err()
}
