package engine

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"ampcheckpoint/internal/domain"
	"ampcheckpoint/internal/mixed"
)

type priorCopy struct {
	CopyID       string
	ShardID      string
	Precision    domain.Precision
	Pool         domain.Pool
	Status       domain.CopyStatus
	LastAccessNS int64
}

type checkpointPlan struct {
	Copies      []domain.Copy
	Quant       map[string]mixed.Quantization
	Evictions   []domain.Eviction
	Writes      domain.WriteSummary
	Pools       []domain.PoolSummary
	Precision   []domain.PrecisionShardReport
	Derived     []domain.DerivedCopy
	Checksum    string
	InitialVRAM int64
}

func (e *Engine) CommitCheckpoint(ctx context.Context, req domain.CheckpointRequest) (domain.CheckpointResult, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	fault, err := normalizeFaultMode(req.FaultAfterIntent)
	if err != nil {
		return domain.CheckpointResult{}, err
	}
	if req.Round <= 0 {
		return domain.CheckpointResult{}, domain.BizError(domain.ErrInvalidRequest, "round must be positive", map[string]any{"round": req.Round})
	}
	policy, err := loadCurrentPolicy(ctx, e.db)
	if isNotFound(err) {
		return domain.CheckpointResult{}, domain.BizError(domain.ErrInvalidRequest, "a committed precision policy snapshot is required before checkpointing", map[string]any{"round": req.Round})
	}
	if err != nil {
		return domain.CheckpointResult{}, err
	}
	if req.WriteQueueCapacity != 0 {
		if req.WriteQueueCapacity < 0 {
			return domain.CheckpointResult{}, domain.BizError(domain.ErrInvalidRequest, "write_queue_capacity override must be positive", map[string]any{"write_queue_capacity": req.WriteQueueCapacity})
		}
		policy.WriteQueueCapacity = req.WriteQueueCapacity
	}
	if req.TaskTimeMS != 0 {
		if req.TaskTimeMS < 0 {
			return domain.CheckpointResult{}, domain.BizError(domain.ErrInvalidRequest, "task_time_ms override must be positive", map[string]any{"task_time_ms": req.TaskTimeMS})
		}
		policy.TaskTimeMS = req.TaskTimeMS
	}

	priorVersion, priorRound, prior, err := e.loadPriorCheckpoint(ctx)
	if err != nil {
		return domain.CheckpointResult{}, err
	}
	if priorVersion > 0 && req.Round <= priorRound {
		return domain.CheckpointResult{}, domain.BizError(domain.ErrInvalidRequest, "checkpoint round must advance beyond the latest committed round", map[string]any{"round": req.Round, "latest_round": priorRound, "latest_checkpoint_version": priorVersion})
	}
	shards, err := e.sample.Round(req.Round)
	if err != nil {
		return domain.CheckpointResult{}, err
	}
	plan, err := e.planCheckpoint(policy, req.Round, shards, prior)
	if err != nil {
		return domain.CheckpointResult{}, err
	}

	opID, err := e.stageCheckpointIntent(ctx, policy, req.Round, priorVersion, plan, fault)
	if err != nil {
		return domain.CheckpointResult{}, err
	}
	if fault != "" {
		return domain.CheckpointResult{
			PolicyVersion: policy.Version,
			Round:         req.Round,
			IntentID:      opID,
			FaultInjected: fault,
			Evictions:     plan.Evictions,
			Writes:        plan.Writes,
			Pools:         plan.Pools,
		}, nil
	}
	result, err := e.finalizeCheckpoint(ctx, opID, policy, req.Round, plan)
	if err != nil {
		e.abortIntent(opID, err.Error())
		return domain.CheckpointResult{}, err
	}
	return result, nil
}

func (e *Engine) loadPriorCheckpoint(ctx context.Context) (int64, int64, map[string]priorCopy, error) {
	var version int64
	if err := e.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM checkpoints`).Scan(&version); err != nil {
		return 0, 0, nil, err
	}
	if version == 0 {
		return 0, 0, map[string]priorCopy{}, nil
	}
	var round int64
	if err := e.db.QueryRowContext(ctx, `SELECT round FROM checkpoints WHERE version=?`, version).Scan(&round); err != nil {
		return 0, 0, nil, err
	}
	rows, err := e.db.QueryContext(ctx, `SELECT copy_id, shard_id, precision, pool, status, last_access_ns FROM checkpoint_copies WHERE checkpoint_version=?`, version)
	if err != nil {
		return 0, 0, nil, err
	}
	defer rows.Close()
	out := make(map[string]priorCopy)
	for rows.Next() {
		var c priorCopy
		var precision, pool, status string
		if err := rows.Scan(&c.CopyID, &c.ShardID, &precision, &pool, &status, &c.LastAccessNS); err != nil {
			return 0, 0, nil, err
		}
		c.Precision = domain.Precision(precision)
		c.Pool = domain.Pool(pool)
		c.Status = domain.CopyStatus(status)
		out[c.CopyID] = c
	}
	return version, round, out, rows.Err()
}

func (e *Engine) planCheckpoint(policy domain.Policy, round int64, shards []domain.LogicalShard, prior map[string]priorCopy) (checkpointPlan, error) {
	plan := checkpointPlan{Quant: make(map[string]mixed.Quantization), Evictions: []domain.Eviction{}}
	placementByShard := make(map[string]domain.Placement, len(policy.Placements))
	for _, placement := range policy.Placements {
		placementByShard[placement.ShardID] = placement
	}

	masterByShard := make(map[string]domain.Copy, len(shards))
	for _, shard := range shards {
		masterDigest := mixed.MasterDigest(shard.ShardID, round, shard.ValuesFixed)
		master := domain.Copy{
			CopyID:         "master:" + shard.ShardID,
			ShardID:        shard.ShardID,
			Round:          round,
			Precision:      domain.PrecisionFP32,
			SizeBytes:      shard.FP32SizeBytes,
			Digest:         masterDigest,
			MasterDigest:   masterDigest,
			MomentumDigest: shard.MomentumDigest,
			Pool:           domain.PoolPersistent,
			Status:         domain.StatusPersisted,
			Priority:       shard.Priority,
			LossScaleFixed: policy.LossScaleFixed,
			ValuesFixed:    shard.ValuesFixed,
		}
		if placement, ok := placementByShard[shard.ShardID]; ok && placement.LastAccessNS > 0 {
			master.LastAccessNS = placement.LastAccessNS
		} else {
			master.LastAccessNS = round * 1_000_000_000
		}
		masterByShard[shard.ShardID] = master
		plan.Copies = append(plan.Copies, master)
	}

	residents := make(map[string]struct{})
	var additions []string
	currentVRAM := int64(0)
	for _, shard := range shards {
		placement, desired := placementByShard[shard.ShardID]
		if !desired || placement.Precision != domain.PrecisionFP16 {
			continue
		}
		master := masterByShard[shard.ShardID]
		q, err := mixed.Quantize(shard.ShardID, round, policy.LossScaleFixed, shard.ValuesFixed)
		if err != nil {
			return checkpointPlan{}, domain.BizError(domain.ErrLossScaleOverflow, "FP16 quantization overflow while generating checkpoint", map[string]any{"shard_id": shard.ShardID, "loss_scale_fixed": policy.LossScaleFixed, "reason": err.Error()})
		}
		copyID := "fp16:" + shard.ShardID
		lastAccess := placement.LastAccessNS
		previous, hadPrevious := prior[copyID]
		if lastAccess == 0 && hadPrevious {
			lastAccess = previous.LastAccessNS
		}
		if lastAccess == 0 {
			lastAccess = round*1_000_000_000 + int64(len(plan.Copies))
		}
		c := domain.Copy{
			CopyID:         copyID,
			ShardID:        shard.ShardID,
			Round:          round,
			Precision:      domain.PrecisionFP16,
			SizeBytes:      shard.FP16SizeBytes,
			Digest:         q.Digest,
			MasterCopyID:   master.CopyID,
			MasterDigest:   master.Digest,
			MomentumDigest: shard.MomentumDigest,
			Pool:           domain.PoolPersistent,
			Status:         domain.StatusPersisted,
			LastAccessNS:   lastAccess,
			Priority:       shard.Priority,
			LossScaleFixed: policy.LossScaleFixed,
			Restoration:    domain.RestorationContract,
			ValuesFixed:    shard.ValuesFixed,
			FP16Bits:       q.Bits,
		}
		if placement.Pool == domain.PoolVRAM && hadPrevious && previous.Pool == domain.PoolVRAM && previous.Status == domain.StatusResident {
			c.Pool = domain.PoolVRAM
			c.Status = domain.StatusResident
			residents[copyID] = struct{}{}
			if err := domain.OverflowAdd(currentVRAM, c.SizeBytes); err != nil {
				return checkpointPlan{}, domain.BizError(domain.ErrPartitionInvalid, "VRAM byte accounting overflow", map[string]any{"copy_id": copyID, "current_bytes": currentVRAM, "size_bytes": c.SizeBytes})
			}
			currentVRAM += c.SizeBytes
		} else if placement.Pool == domain.PoolVRAM {
			additions = append(additions, copyID)
		} else if hadPrevious && previous.Pool == domain.PoolVRAM {
			plan.Evictions = append(plan.Evictions, domain.Eviction{
				CopyID:         copyID,
				ShardID:        shard.ShardID,
				FromPool:       domain.PoolVRAM,
				ToPool:         domain.PoolPersistent,
				PreviousStatus: previous.Status,
				NewStatus:      domain.StatusPersisted,
				Reason:         "POLICY_DEMOTE",
				Bytes:          c.SizeBytes,
			})
		}
		plan.Quant[copyID] = q
		plan.Copies = append(plan.Copies, c)
	}
	plan.InitialVRAM = currentVRAM

	copyIndex := make(map[string]int, len(plan.Copies))
	for i := range plan.Copies {
		copyIndex[plan.Copies[i].CopyID] = i
	}
	sort.Slice(additions, func(i, j int) bool {
		a, b := plan.Copies[copyIndex[additions[i]]], plan.Copies[copyIndex[additions[j]]]
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if a.LastAccessNS != b.LastAccessNS {
			return a.LastAccessNS > b.LastAccessNS
		}
		return a.CopyID < b.CopyID
	})
	for _, copyID := range additions {
		idx := copyIndex[copyID]
		c := &plan.Copies[idx]
		if c.SizeBytes > policy.VRAMPoolBytes {
			return checkpointPlan{}, vramFullError(policy.VRAMPoolBytes, currentVRAM, c.SizeBytes, 0)
		}
		if currentVRAM+c.SizeBytes > policy.VRAMPoolBytes {
			candidates := evictionCandidates(plan.Copies, residents)
			evicted := 0
			for currentVRAM+c.SizeBytes > policy.VRAMPoolBytes {
				if len(candidates) == 0 {
					return checkpointPlan{}, vramFullError(policy.VRAMPoolBytes, currentVRAM, c.SizeBytes, evicted)
				}
				candidateID := candidates[0]
				candidates = candidates[1:]
				candidateIdx := copyIndex[candidateID]
				candidate := &plan.Copies[candidateIdx]
				candidate.Pool = domain.PoolPersistent
				candidate.Status = domain.StatusPersisted
				delete(residents, candidateID)
				currentVRAM -= candidate.SizeBytes
				evicted++
				plan.Evictions = append(plan.Evictions, domain.Eviction{
					CopyID:         candidate.CopyID,
					ShardID:        candidate.ShardID,
					FromPool:       domain.PoolVRAM,
					ToPool:         domain.PoolPersistent,
					PreviousStatus: domain.StatusResident,
					NewStatus:      domain.StatusPersisted,
					Reason:         "LRU_VRAM_PRESSURE",
					Bytes:          candidate.SizeBytes,
				})
			}
		}
		c.Pool = domain.PoolVRAM
		c.Status = domain.StatusResident
		residents[copyID] = struct{}{}
		currentVRAM += c.SizeBytes
		if currentVRAM > policy.VRAMPoolBytes {
			return checkpointPlan{}, domain.BizError(domain.ErrInternal, "VRAM planning invariant violated", map[string]any{"limit_bytes": policy.VRAMPoolBytes, "used_bytes": currentVRAM})
		}
	}
	if currentVRAM > policy.VRAMPoolBytes {
		return checkpointPlan{}, domain.BizError(domain.ErrInternal, "VRAM limit exceeded after planning", map[string]any{"limit_bytes": policy.VRAMPoolBytes, "used_bytes": currentVRAM})
	}

	if err := validateGeneratedBatch(plan.Copies, plan.Quant); err != nil {
		return checkpointPlan{}, err
	}
	pools, err := summarizePools(plan.Copies, policy.VRAMPoolBytes)
	if err != nil {
		return checkpointPlan{}, err
	}
	plan.Pools = pools
	writeSummary, err := scheduleWrites(plan.Copies, policy)
	if err != nil {
		return checkpointPlan{}, err
	}
	plan.Writes = writeSummary
	writeState := make(map[string]string, len(writeSummary.Written))
	for _, task := range writeSummary.Written {
		writeState[task.CopyID] = "WRITTEN"
	}
	for _, task := range writeSummary.Skipped {
		writeState[task.CopyID] = "SKIPPED_" + task.Reason
	}
	for i := range plan.Copies {
		state := writeState[plan.Copies[i].CopyID]
		if state == "" {
			state = "NOT_SELECTED"
		}
		plan.Copies[i].WriteState = state
	}
	precision, err := buildPrecisionReports(plan.Copies, plan.Quant)
	if err != nil {
		return checkpointPlan{}, err
	}
	plan.Precision = precision
	plan.Derived = buildDerived(plan.Copies, precision, plan.Quant)
	plan.Checksum = checkpointChecksum(policy, round, plan)
	return plan, nil
}

func vramFullError(limit, current, required int64, evicted int) error {
	return domain.BizError(domain.ErrVRAMPoolFull, "VRAM pool cannot accommodate the requested shard after eviction", map[string]any{
		"limit_bytes":        limit,
		"current_bytes":      current,
		"required_bytes":     required,
		"evicted_candidates": evicted,
	})
}

func evictionCandidates(copies []domain.Copy, residents map[string]struct{}) []string {
	type candidate struct {
		id            string
		lastAccess    int64
		precisionRank int
		priority      int64
	}
	out := make([]candidate, 0, len(residents))
	for i := range copies {
		c := copies[i]
		if _, ok := residents[c.CopyID]; !ok || c.Pool != domain.PoolVRAM {
			continue
		}
		rank := 1
		if c.Precision == domain.PrecisionFP16 {
			rank = 0
		}
		out = append(out, candidate{id: c.CopyID, lastAccess: c.LastAccessNS, precisionRank: rank, priority: c.Priority})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].lastAccess != out[j].lastAccess {
			return out[i].lastAccess < out[j].lastAccess
		}
		if out[i].precisionRank != out[j].precisionRank {
			return out[i].precisionRank < out[j].precisionRank
		}
		if out[i].priority != out[j].priority {
			return out[i].priority < out[j].priority
		}
		return out[i].id < out[j].id
	})
	ids := make([]string, len(out))
	for i := range out {
		ids[i] = out[i].id
	}
	return ids
}

func validateGeneratedBatch(copies []domain.Copy, quant map[string]mixed.Quantization) error {
	batch := domain.PartitionBatch{Partitions: make([]domain.PartitionInput, 0, len(copies))}
	for _, c := range copies {
		input := domain.PartitionInput{
			CopyID:         c.CopyID,
			ShardID:        c.ShardID,
			Round:          c.Round,
			Precision:      c.Precision,
			SizeBytes:      c.SizeBytes,
			Digest:         c.Digest,
			MasterCopyID:   c.MasterCopyID,
			MasterDigest:   c.MasterDigest,
			Restoration:    c.Restoration,
			MomentumDigest: c.MomentumDigest,
		}
		if c.Precision == domain.PrecisionFP16 {
			q, ok := quant[c.CopyID]
			if !ok {
				return domain.BizError(domain.ErrPartitionInvalid, "missing FP16 quantization proof", map[string]any{"copy_id": c.CopyID})
			}
			input.RestoredDigest = q.RestoredDigest
		}
		batch.Partitions = append(batch.Partitions, input)
	}
	_, err := domain.ValidatePartitionBatch(batch)
	return err
}

func summarizePools(copies []domain.Copy, limit int64) ([]domain.PoolSummary, error) {
	byPool := map[domain.Pool]*domain.PoolSummary{
		domain.PoolVRAM:       {Pool: domain.PoolVRAM, LimitBytes: limit},
		domain.PoolPersistent: {Pool: domain.PoolPersistent},
	}
	for _, c := range copies {
		summary, ok := byPool[c.Pool]
		if !ok {
			return nil, domain.BizError(domain.ErrInternal, "unknown pool while summarizing", map[string]any{"pool": c.Pool, "copy_id": c.CopyID})
		}
		if err := domain.OverflowAdd(summary.TotalBytes, c.SizeBytes); err != nil {
			return nil, domain.BizError(domain.ErrInternal, "pool byte accounting overflow", map[string]any{"pool": c.Pool})
		}
		summary.TotalBytes += c.SizeBytes
		summary.CopyCount++
	}
	vram := byPool[domain.PoolVRAM]
	if vram.TotalBytes > limit {
		return nil, domain.BizError(domain.ErrVRAMPoolFull, "VRAM pool exceeds declared limit", map[string]any{"limit_bytes": limit, "current_bytes": vram.TotalBytes, "required_bytes": vram.TotalBytes - limit})
	}
	vram.RemainingBytes = limit - vram.TotalBytes
	pools := []domain.PoolSummary{*vram, *byPool[domain.PoolPersistent]}
	return pools, nil
}

func scheduleWrites(copies []domain.Copy, policy domain.Policy) (domain.WriteSummary, error) {
	tasks := make([]domain.WriteTask, 0, len(copies))
	for _, c := range copies {
		tasks = append(tasks, domain.WriteTask{CopyID: c.CopyID, ShardID: c.ShardID, Precision: c.Precision, Bytes: c.SizeBytes, Priority: c.Priority, Round: c.Round})
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].Priority != tasks[j].Priority {
			return tasks[i].Priority > tasks[j].Priority
		}
		if tasks[i].Round != tasks[j].Round {
			return tasks[i].Round < tasks[j].Round
		}
		if tasks[i].ShardID != tasks[j].ShardID {
			return tasks[i].ShardID < tasks[j].ShardID
		}
		return tasks[i].CopyID < tasks[j].CopyID
	})
	summary := domain.WriteSummary{
		BudgetBytes:   policy.WriteBudgetBytes,
		TimeBudgetMS:  policy.RoundTimeBudgetMS,
		QueueCapacity: policy.WriteQueueCapacity,
		Written:       []domain.WriteTask{},
		Skipped:       []domain.SkippedWrite{},
		Queued:        []domain.SkippedWrite{},
	}
	remainingBytes := policy.WriteBudgetBytes
	remainingMS := policy.RoundTimeBudgetMS
	priorityBlocked := false
	for _, task := range tasks {
		byteOK := task.Bytes <= remainingBytes
		timeOK := policy.TaskTimeMS <= remainingMS
		if !priorityBlocked && byteOK && timeOK {
			summary.Written = append(summary.Written, task)
			remainingBytes -= task.Bytes
			remainingMS -= policy.TaskTimeMS
			summary.UsedBytes += task.Bytes
			summary.TimeUsedMS += policy.TaskTimeMS
			continue
		}
		priorityBlocked = true
		reason := "WRITE_BUDGET"
		if byteOK && !timeOK {
			reason = "TIME_BUDGET"
		} else if !byteOK && !timeOK {
			reason = "WRITE_AND_TIME_BUDGET"
		}
		summary.Skipped = append(summary.Skipped, domain.SkippedWrite{WriteTask: task, Reason: reason})
	}
	summary.RemainingBytes = policy.WriteBudgetBytes - summary.UsedBytes
	summary.TimeRemainingMS = policy.RoundTimeBudgetMS - summary.TimeUsedMS
	if len(summary.Skipped) > policy.WriteQueueCapacity {
		skippedBytes := int64(0)
		for _, s := range summary.Skipped {
			skippedBytes += s.Bytes
		}
		return domain.WriteSummary{}, domain.BizError(domain.ErrWriteQueueFull, "bounded write wait queue cannot hold all budget-skipped shards", map[string]any{
			"queue_capacity":        policy.WriteQueueCapacity,
			"skipped_tasks":         len(summary.Skipped),
			"overflow_tasks":        len(summary.Skipped) - policy.WriteQueueCapacity,
			"skipped_bytes":         skippedBytes,
			"write_budget_bytes":    policy.WriteBudgetBytes,
			"write_used_bytes":      summary.UsedBytes,
			"time_budget_ms":        policy.RoundTimeBudgetMS,
			"first_skipped_copy_id": firstSkippedID(summary.Skipped),
		})
	}
	summary.Queued = append(summary.Queued, summary.Skipped...)
	return summary, nil
}

func firstSkippedID(skipped []domain.SkippedWrite) string {
	if len(skipped) == 0 {
		return ""
	}
	return skipped[0].CopyID
}

func buildPrecisionReports(copies []domain.Copy, quant map[string]mixed.Quantization) ([]domain.PrecisionShardReport, error) {
	reports := []domain.PrecisionShardReport{}
	for _, c := range copies {
		if c.Precision != domain.PrecisionFP16 {
			continue
		}
		q, ok := quant[c.CopyID]
		if !ok {
			return nil, domain.BizError(domain.ErrInternal, "missing quantization report", map[string]any{"copy_id": c.CopyID})
		}
		if len(c.ValuesFixed) != len(q.RestoredFixed) || len(c.ValuesFixed) != len(q.Bits) {
			return nil, domain.BizError(domain.ErrPartitionInvalid, "FP16 and FP32 element counts differ", map[string]any{"copy_id": c.CopyID, "fp32_elements": len(c.ValuesFixed), "fp16_elements": len(q.Bits)})
		}
		elements := make([]domain.ElementDeviation, len(c.ValuesFixed))
		for i := range c.ValuesFixed {
			elements[i] = domain.ElementDeviation{MasterFixed: c.ValuesFixed[i], FP16Bits: q.Bits[i], RestoredFixed: q.RestoredFixed[i], DeviationFixed: q.RestoredFixed[i] - c.ValuesFixed[i]}
		}
		consistency := q.Digest == c.Digest && q.RestoredDigest != ""
		reports = append(reports, domain.PrecisionShardReport{
			ShardID:                     c.ShardID,
			FP16CopyID:                  c.CopyID,
			MasterCopyID:                c.MasterCopyID,
			ElementCount:                len(elements),
			LossScaleFixed:              c.LossScaleFixed,
			Restoration:                 c.Restoration,
			MaxAbsDeviationFixed:        q.MaxAbsDeviation,
			MaxRelativeDeviationPPM:     q.MaxRelativePPM,
			CumulativeAbsDeviationFixed: q.CumulativeAbsDev,
			ConsistencyOK:               consistency,
			Elements:                    elements,
		})
	}
	sort.Slice(reports, func(i, j int) bool { return reports[i].ShardID < reports[j].ShardID })
	return reports, nil
}

func buildDerived(copies []domain.Copy, reports []domain.PrecisionShardReport, quant map[string]mixed.Quantization) []domain.DerivedCopy {
	consistency := make(map[string]bool, len(reports))
	for _, r := range reports {
		consistency[r.FP16CopyID] = r.ConsistencyOK
	}
	out := make([]domain.DerivedCopy, 0, len(copies))
	for _, c := range copies {
		baseline := domain.RecoveryBaseline{
			Precision:      c.Precision,
			MasterDigest:   c.MasterDigest,
			MomentumDigest: c.MomentumDigest,
			LossScaleFixed: c.LossScaleFixed,
			Restoration:    c.Restoration,
		}
		if c.Precision == domain.PrecisionFP16 {
			baseline.RestoredDigest = quant[c.CopyID].RestoredDigest
		}
		deps := []string{
			"precision:" + string(c.Precision),
			"master_digest:" + c.MasterDigest,
			"momentum_digest:" + c.MomentumDigest,
			"loss_scale_fixed:" + fmt.Sprintf("%d", c.LossScaleFixed),
		}
		if c.MasterCopyID != "" {
			deps = append(deps, "master_copy:"+c.MasterCopyID)
		}
		if baseline.RestoredDigest != "" {
			deps = append(deps, "restored_digest:"+baseline.RestoredDigest)
		}
		derivedDigest := sha256.Sum256([]byte(strings.Join(append([]string{"derived/v1", c.CopyID, c.MasterDigest, c.MomentumDigest, fmt.Sprintf("%d", c.LossScaleFixed), string(c.Precision)}, deps...), "|")))
		ok := true
		if c.Precision == domain.PrecisionFP16 {
			ok = consistency[c.CopyID]
		}
		vramEstimate := int64(0)
		if c.Pool == domain.PoolVRAM {
			vramEstimate = c.SizeBytes
		}
		out = append(out, domain.DerivedCopy{
			CopyID:              c.CopyID,
			MasterDerivedDigest: fmt.Sprintf("%x", derivedDigest[:]),
			ConsistencyOK:       ok,
			VRAMEstimateBytes:   vramEstimate,
			RecoveryBaseline:    baseline,
			DependencyRefs:      deps,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CopyID < out[j].CopyID })
	return out
}

func checkpointChecksum(policy domain.Policy, round int64, plan checkpointPlan) string {
	h := sha256.New()
	fmt.Fprintf(h, "checkpoint/v1|policy=%d|round=%d|vram=%d|write_used=%d|time_used=%d\n", policy.Version, round, plan.InitialVRAM, plan.Writes.UsedBytes, plan.Writes.TimeUsedMS)
	copies := append([]domain.Copy(nil), plan.Copies...)
	sort.Slice(copies, func(i, j int) bool { return copies[i].CopyID < copies[j].CopyID })
	for _, c := range copies {
		fmt.Fprintf(h, "copy=%s|%s|%s|%s|%s|%d|%s|%s\n", c.CopyID, c.Precision, c.Pool, c.Status, c.WriteState, c.SizeBytes, c.Digest, c.MasterDigest)
	}
	evictions := append([]domain.Eviction(nil), plan.Evictions...)
	sort.Slice(evictions, func(i, j int) bool { return evictions[i].CopyID < evictions[j].CopyID })
	for _, e := range evictions {
		fmt.Fprintf(h, "evict=%s|%s|%s|%s\n", e.CopyID, e.FromPool, e.ToPool, e.Reason)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (e *Engine) stageCheckpointIntent(ctx context.Context, policy domain.Policy, round, baseVersion int64, plan checkpointPlan, fault string) (string, error) {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	opID, err := insertIntent(ctx, tx, "CHECKPOINT", "STAGED", baseVersion, policy.Version, map[string]any{
		"round": round, "evictions": len(plan.Evictions), "copies": len(plan.Copies), "write_used_bytes": plan.Writes.UsedBytes,
	})
	if err != nil {
		return "", err
	}
	for _, c := range plan.Copies {
		detail, _ := json.Marshal(map[string]any{"size_bytes": c.SizeBytes, "precision": c.Precision, "write_state": c.WriteState})
		if _, err := tx.ExecContext(ctx, `INSERT INTO operation_shards(op_id, copy_id, shard_id, target_pool, target_status, staging_state, detail_json) VALUES(?,?,?,?,?,?,?)`,
			opID, c.CopyID, c.ShardID, c.Pool, c.Status, "PENDING", string(detail)); err != nil {
			return "", err
		}
	}
	for _, ev := range plan.Evictions {
		detail, _ := json.Marshal(ev)
		if _, err := tx.ExecContext(ctx, `INSERT INTO operation_evictions(op_id, copy_id, from_pool, to_pool, state, detail_json) VALUES(?,?,?,?,?,?)`,
			opID, ev.CopyID, ev.FromPool, ev.ToPool, "PENDING", string(detail)); err != nil {
			return "", err
		}
	}
	if fault != "" {
		phase := "WRITING"
		if fault == "evict" {
			phase = "EVICTING"
		}
		if _, err := tx.ExecContext(ctx, `UPDATE operations SET phase=?, updated_at=? WHERE op_id=?`, phase, time.Now().UTC().Format(time.RFC3339Nano), opID); err != nil {
			return "", err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return opID, nil
}

func (e *Engine) finalizeCheckpoint(ctx context.Context, opID string, policy domain.Policy, round int64, plan checkpointPlan) (domain.CheckpointResult, error) {
	committedAt := time.Now().UTC()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.CheckpointResult{}, err
	}
	defer tx.Rollback()
	var version int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM checkpoints`).Scan(&version); err != nil {
		return domain.CheckpointResult{}, err
	}
	evictionsJSON, _ := json.Marshal(plan.Evictions)
	writtenJSON, _ := json.Marshal(plan.Writes.Written)
	skippedJSON, _ := json.Marshal(plan.Writes.Skipped)
	queuedJSON, _ := json.Marshal(plan.Writes.Queued)
	if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoints(version, policy_version, round, committed_at, vram_limit_bytes, write_budget_bytes, write_used_bytes, write_remaining_bytes, time_budget_ms, time_used_ms, time_remaining_ms, write_queue_capacity, written_json, skipped_json, queued_json, evictions_json, checksum) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		version, policy.Version, round, committedAt.Format(time.RFC3339Nano), policy.VRAMPoolBytes, policy.WriteBudgetBytes, plan.Writes.UsedBytes, plan.Writes.RemainingBytes,
		policy.RoundTimeBudgetMS, plan.Writes.TimeUsedMS, plan.Writes.TimeRemainingMS, policy.WriteQueueCapacity,
		string(writtenJSON), string(skippedJSON), string(queuedJSON), string(evictionsJSON), plan.Checksum); err != nil {
		return domain.CheckpointResult{}, err
	}
	for _, c := range plan.Copies {
		valuesJSON, _ := json.Marshal(c.ValuesFixed)
		bitsJSON, _ := json.Marshal(c.FP16Bits)
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoint_copies(checkpoint_version, copy_id, shard_id, round, precision, size_bytes, digest, master_copy_id, master_digest, momentum_digest, pool, status, last_access_ns, priority, loss_scale_fixed, restoration, write_state, values_fixed_json, fp16_bits_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			version, c.CopyID, c.ShardID, c.Round, c.Precision, c.SizeBytes, c.Digest, c.MasterCopyID, c.MasterDigest, c.MomentumDigest,
			c.Pool, c.Status, c.LastAccessNS, c.Priority, c.LossScaleFixed, c.Restoration, c.WriteState, string(valuesJSON), string(bitsJSON)); err != nil {
			return domain.CheckpointResult{}, err
		}
	}
	for _, d := range plan.Derived {
		baselineJSON, _ := json.Marshal(d.RecoveryBaseline)
		depsJSON, _ := json.Marshal(d.DependencyRefs)
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoint_derived(checkpoint_version, copy_id, master_derived_digest, consistency_ok, vram_estimate_bytes, recovery_baseline_json, dependency_refs_json) VALUES(?,?,?,?,?,?,?)`,
			version, d.CopyID, d.MasterDerivedDigest, boolInt(d.ConsistencyOK), d.VRAMEstimateBytes, string(baselineJSON), string(depsJSON)); err != nil {
			return domain.CheckpointResult{}, err
		}
	}
	for _, p := range plan.Pools {
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoint_pools(checkpoint_version, pool, copy_count, total_bytes, limit_bytes, remaining_bytes) VALUES(?,?,?,?,?,?)`,
			version, p.Pool, p.CopyCount, p.TotalBytes, p.LimitBytes, p.RemainingBytes); err != nil {
			return domain.CheckpointResult{}, err
		}
	}
	for _, report := range plan.Precision {
		elementsJSON, _ := json.Marshal(report.Elements)
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoint_precision(checkpoint_version, shard_id, fp16_copy_id, master_copy_id, element_count, loss_scale_fixed, restoration, max_abs_deviation_fixed, max_relative_deviation_ppm, cumulative_abs_deviation_fixed, consistency_ok, elements_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			version, report.ShardID, report.FP16CopyID, report.MasterCopyID, report.ElementCount, report.LossScaleFixed, report.Restoration,
			report.MaxAbsDeviationFixed, report.MaxRelativeDeviationPPM, report.CumulativeAbsDeviationFixed, boolInt(report.ConsistencyOK), string(elementsJSON)); err != nil {
			return domain.CheckpointResult{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operation_shards SET staging_state='COMMITTED' WHERE op_id=?`, opID); err != nil {
		return domain.CheckpointResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operation_evictions SET state='COMMITTED' WHERE op_id=?`, opID); err != nil {
		return domain.CheckpointResult{}, err
	}
	if err := finishIntent(ctx, tx, opID, "COMMITTED", "COMMITTED", version); err != nil {
		return domain.CheckpointResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.CheckpointResult{}, err
	}
	return domain.CheckpointResult{
		Version:       version,
		PolicyVersion: policy.Version,
		Round:         round,
		CommittedAt:   committedAt,
		IntentID:      opID,
		Evictions:     plan.Evictions,
		Writes:        plan.Writes,
		Pools:         plan.Pools,
	}, nil
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
