package scheduler

import (
	"fmt"

	"localinfer/workbench/internal/domain"
)

func (b *planBuilder) addRequest(req domain.TraceRequest, isLate bool) error {
	model := b.catalog.ByID[req.ModelID]
	if isLate && !b.currentIsLate() {
		b.closeCurrent("LATE_AFTER_SEAL_NEW_BATCH", "late request cannot mutate an existing sealed batch")
	} else if !isLate && b.currentIsLate() {
		b.closeCurrent("LATE_SEAL_BOUNDARY", "a non-late request cannot join a late-sealed batch")
	}
	if b.current != nil && b.current.batch.Priority != req.Priority {
		b.closeCurrent("PRIORITY_CHANGE", fmt.Sprintf("priority changes from %d to %d; priorities never share a batch", b.current.batch.Priority, req.Priority))
	}
	if b.current != nil && b.current.batch.TotalSamples > 0 && !b.opts.AllowMixedModels && !b.hasModel(req.ModelID) {
		b.closeCurrent("MODEL_ISOLATION", "mixed-model batching disabled; model order is preserved")
	}
	remaining := req.Samples
	firstChunk := true
	for remaining > 0 {
		if b.current == nil {
			reason := "INITIAL"
			detail := "first batch for this arrival group"
			if isLate {
				reason = "LATE_AFTER_SEAL_NEW_BATCH"
				detail = "late request placed in a new immutable batch"
			} else if b.pendingSplit != nil {
				reason = b.pendingSplit.reason
				detail = b.pendingSplit.detail
			}
			b.startBatch(req, reason, detail, isLate)
		}
		if b.current.batch.Priority != req.Priority {
			b.closeCurrent("PRIORITY_CHANGE", "priority change")
			continue
		}
		if b.current.batch.TotalSamples > 0 && !b.hasModel(req.ModelID) && !b.opts.AllowMixedModels {
			b.closeCurrent("MODEL_ISOLATION", "mixed-model batch is not declared")
			continue
		}
		if !b.hasModel(req.ModelID) && b.current.batch.TotalSamples > 0 {
			currentLimit := b.current.batch.LatencyLimitUS
			if req.LatencyLimitUS < currentLimit {
				currentLimit = req.LatencyLimitUS
			}
			currentLatency, _, err := b.current.latency.Result()
			if err != nil {
				return domain.NewBizError(domain.CodePrecisionBreach, "batch latency overflows", map[string]any{"request_id": req.RequestID})
			}
			addLatency, err := checkedMul(remaining, model.CostPerSample)
			if err != nil {
				return domain.NewBizError(domain.CodePrecisionBreach, "batch latency overflows", map[string]any{"request_id": req.RequestID})
			}
			combinedLatency, addErr := checkedAdd(currentLatency, addLatency)
			if addErr != nil {
				return domain.NewBizError(domain.CodePrecisionBreach, "combined latency overflows int64", map[string]any{"request_id": req.RequestID})
			}
			if combinedLatency > currentLimit {
				b.plan.MixRejections = append(b.plan.MixRejections, domain.MixRejection{
					RequestID: req.RequestID, ExistingModels: append([]string(nil), b.current.batch.Models...),
					CandidateModel: req.ModelID, Reason: "MIXED_MODEL_LATENCY_LIMIT",
					Detail: fmt.Sprintf("combined latency %dus would exceed shared limit %dus; existing batch was sealed unchanged", combinedLatency, currentLimit),
				})
				rootSeq := b.current.batch.RootSequence
				b.closeCurrent("MIXED_MODEL_LATENCY_LIMIT", "candidate request would push mixed batch over its latency cap")
				b.pendingSplit = &splitOrigin{rootSeq: rootSeq, index: -1, reason: "MIXED_MODEL_LATENCY_LIMIT", detail: "new same-root batch after rejecting model mixture"}
				continue
			}
		}

		limit := req.LatencyLimitUS
		if b.current.batch.TotalSamples > 0 && b.current.batch.LatencyLimitUS < limit {
			limit = b.current.batch.LatencyLimitUS
		}
		currentLatency, _, err := b.current.latency.Result()
		if err != nil {
			return domain.NewBizError(domain.CodePrecisionBreach, "batch latency overflows", map[string]any{"request_id": req.RequestID})
		}
		modelRemaining := model.MaxBatchSample - b.current.modelCounts[req.ModelID]
		vramRemaining := (b.availableVRAM - b.usedVRAM) / model.VRAMPerSample
		latencyRemaining := int64(0)
		if limit >= currentLatency {
			latencyRemaining = (limit - currentLatency) / model.CostPerSample
		}
		capacity := remaining
		var hits []string
		if capacity > modelRemaining {
			capacity = modelRemaining
			hits = append(hits, "MODEL_MAX_SAMPLES")
		}
		if capacity > vramRemaining {
			capacity = vramRemaining
			hits = append(hits, "NODE_VRAM")
		}
		if capacity > latencyRemaining {
			capacity = latencyRemaining
			hits = append(hits, "LATENCY_LIMIT")
		}
		if capacity <= 0 {
			if b.current.batch.TotalSamples == 0 {
				// A single sample cannot meet the declared cap. Keep the batch
				// observable and explicitly failed rather than silently passing it.
				capacity = remaining
				if capacity > modelRemaining {
					capacity = modelRemaining
				}
				if capacity > vramRemaining {
					capacity = vramRemaining
				}
				if capacity <= 0 {
					return domain.NewBizError(domain.CodeInsufficientCap, "cannot place even one sample in available capacity; no batch was committed", map[string]any{
						"request_id": req.RequestID, "model_id": req.ModelID, "available_vram_bytes": b.availableVRAM - b.usedVRAM,
						"vram_per_sample": model.VRAMPerSample, "planning_preserved": true,
					})
				}
				hits = append(hits, "LATENCY_LIMIT_UNSATISFIABLE")
			} else {
				b.closeCurrent(joinHits(hits), "request cannot fit in the open batch")
				continue
			}
		}
		term, err := checkedMul(capacity, model.CostPerSample)
		if err != nil {
			return domain.NewBizError(domain.CodePrecisionBreach, "batch latency term overflows", map[string]any{"request_id": req.RequestID})
		}
		addVRAM, err := checkedMul(capacity, model.VRAMPerSample)
		if err != nil {
			return domain.NewBizError(domain.CodeInsufficientCap, "batch VRAM term overflows", map[string]any{"request_id": req.RequestID})
		}
		newUsed, addErr := checkedAdd(b.usedVRAM, addVRAM)
		if addErr != nil || newUsed > b.availableVRAM {
			return domain.NewBizError(domain.CodeInsufficientCap, "batch VRAM exceeds available capacity", map[string]any{"request_id": req.RequestID})
		}
		b.current.latency.Add(term)
		b.current.vram += addVRAM
		b.usedVRAM = newUsed
		b.current.modelCounts[req.ModelID] += capacity
		allocationReason := "FULL_REMAINDER"
		if capacity < remaining {
			allocationReason = joinHits(hits)
		}
		if firstChunk && len(b.current.batch.Members) == 0 {
			allocationReason = "BATCH_HEAD"
		}
		b.current.batch.Members = append(b.current.batch.Members, domain.PlannedMember{
			RequestID: req.RequestID, Sequence: req.Sequence, ModelID: req.ModelID,
			AllocatedSamples: capacity, OriginalSamples: req.Samples, AllocationReason: allocationReason,
		})
		b.current.batch.TotalSamples += capacity
		b.current.batch.VRAMBytes += addVRAM
		if b.current.batch.LatencyLimitUS == 0 || req.LatencyLimitUS < b.current.batch.LatencyLimitUS {
			b.current.batch.LatencyLimitUS = req.LatencyLimitUS
		}
		b.current.batch.Models = sortedModels(b.current.modelCounts)
		remaining -= capacity
		firstChunk = false
		if remaining > 0 {
			reason := joinHits(hits)
			if reason == "" {
				reason = "CAPACITY_REALLOCATION"
			}
			rootSeq := b.current.batch.RootSequence
			b.closeCurrent(reason, fmt.Sprintf("request %s split: %d samples remain after deterministic capacity allocation", req.RequestID, remaining))
			b.pendingSplit = &splitOrigin{rootSeq: rootSeq, index: -1, reason: reason, detail: "same-root child batch created to conserve samples"}
		}
	}
	return nil
}
