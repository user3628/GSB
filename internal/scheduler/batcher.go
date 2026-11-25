package scheduler

import (
	"fmt"

	"gpusched/internal/domain"
)

// PlannedBatch is an in-memory batch produced by the organizer; the service
// persists it inside a transaction together with request/node/quota changes.
type PlannedBatch struct {
	ModelID      string
	NodeID       string
	GroupID      string
	Priority     int
	MemberIDs    []string
	TotalSamples int64
	TotalMemory  int64
	EstLatencyMs int64
}

// Organize groups admitted requests into batches.
//
// Rules (per the spec):
//   - same model id, same assigned node, adjacent in submission (seq) order;
//   - priority is a hard divider: different priorities are never mixed;
//   - total memory must not exceed the node's current free memory;
//   - total samples must not exceed the model's per-batch sample cap;
//   - when adding a request would exceed any member's latency limit, the batch
//     is split and the request starts a new batch (submission order preserved);
//   - a single request that alone violates a limit is emitted alone and flagged
//     via LatencyViolation so the caller can decide to downgrade it.
func Organize(
	requests []domain.Request,
	nodeFreeMemory map[string]int64,
	maxSamplesPerModel map[string]int64,
	baseMs int64,
	perSample float64,
) ([]PlannedBatch, []string) {
	type seg struct {
		groupID  string
		nodeID   string
		modelID  string
		priority int
		reqs     []domain.Request
	}

	// 1. Stable partition keyed by (node, model); priority breaks segments so
	// different priorities never land in one batch. A break is also forced
	// whenever the key changes, so only *adjacent* requests in submission order
	// are grouped (a different request in between splits the run).
	segments := []seg{}
	var current *seg
	for _, r := range requests {
		if current == nil || current.nodeID != r.AssignedNodeID ||
			current.modelID != r.ModelID || current.priority != r.Priority {
			segments = append(segments, seg{
				groupID: r.GroupID, nodeID: r.AssignedNodeID,
				modelID: r.ModelID, priority: r.Priority,
			})
			current = &segments[len(segments)-1]
		}
		current.reqs = append(current.reqs, r)
	}

	batches := []PlannedBatch{}
	latencyViolations := []string{}
	batchSeq := 0

	for _, s := range segments {
		cap := nodeFreeMemory[s.nodeID]
		modelCap := int64(0)
		if m, ok := maxSamplesPerModel[s.modelID]; ok {
			modelCap = m
		}

		cur := PlannedBatch{
			ModelID: s.modelID, NodeID: s.nodeID, GroupID: s.groupID, Priority: s.priority,
		}
		flush := func() {
			if len(cur.MemberIDs) == 0 {
				return
			}
			cur.EstLatencyMs = EstimateLatencyMs(baseMs, perSample, cur.TotalSamples)
			batchSeq++
			batches = append(batches, cur)
			cur = PlannedBatch{
				ModelID: s.modelID, NodeID: s.nodeID, GroupID: s.groupID, Priority: s.priority,
			}
		}

		for _, r := range s.reqs {
			wouldMem := cur.TotalMemory + r.MemoryNeed
			wouldSamples := cur.TotalSamples + r.SamplesPerBatch
			wouldLatency := EstimateLatencyMs(baseMs, perSample, wouldSamples)

			memExceeded := wouldMem > cap
			samplesExceeded := modelCap > 0 && wouldSamples > modelCap
			latencyExceeded := wouldLatency > r.LatencyLimitMs

			if len(cur.MemberIDs) > 0 && (memExceeded || samplesExceeded || latencyExceeded) {
				// Split: close current batch, this request opens the next one.
				// Submission order is preserved because we never reorder.
				flush()
			}

			cur.MemberIDs = append(cur.MemberIDs, r.ID)
			cur.TotalMemory += r.MemoryNeed
			cur.TotalSamples += r.SamplesPerBatch

			// A single request that alone blows its own latency budget cannot be
			// split further; record it rather than silently dropping it.
			soloLatency := EstimateLatencyMs(baseMs, perSample, r.SamplesPerBatch)
			if soloLatency > r.LatencyLimitMs {
				latencyViolations = append(latencyViolations,
					fmt.Sprintf("%s:solo_latency=%d>limit=%d", r.ID, soloLatency, r.LatencyLimitMs))
			}
			if modelCap > 0 && r.SamplesPerBatch > modelCap {
				latencyViolations = append(latencyViolations,
					fmt.Sprintf("%s:samples=%d>model_cap=%d", r.ID, r.SamplesPerBatch, modelCap))
			}
		}
		flush()
	}

	return batches, latencyViolations
}
