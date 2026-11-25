package pipeline

import (
	"fmt"
	"sort"
)

// Size thresholds are fixed constants so the same inputs always select the
// same strategy on every host.
const (
	// smallVoxelLimit: below this many voxels a full recompute is cheap enough
	// that cache machinery is pure overhead.
	smallVoxelLimit int64 = 20000
	// largeVoxelLimit: above this many voxels full recompute is prohibited.
	largeVoxelLimit int64 = 200000
	// largeShardLimit: above this many shards full recompute is prohibited.
	largeShardLimit = 16
	// smallShardLimit: below this many shards sparse bookkeeping is not worth it.
	smallShardLimit = 4
)

// Plan selects among FULL_RECOMPUTE, BLOCK_CACHE and SPARSE_INCREMENTAL using
// deterministic scores. Every candidate's score, eligibility and rejection
// reason is returned so the decision is fully auditable (requirement 3).
func Plan(in PlanInputs) PlanRecord {
	eligible := map[Strategy]bool{}
	rejected := map[Strategy]string{}
	scores := map[Strategy]float64{}

	// Hard eligibility rules first.
	smallScene := in.VoxelCount <= smallVoxelLimit && in.ShardCount <= smallShardLimit
	largeScene := in.VoxelCount >= largeVoxelLimit || in.ShardCount >= largeShardLimit

	eligible[StrategyFull] = !largeScene
	if largeScene {
		rejected[StrategyFull] = "large scene: per-view full recompute exceeds global budget"
	}
	eligible[StrategyBlockCache] = true
	eligible[StrategySparse] = !smallScene
	if smallScene {
		rejected[StrategySparse] = "small scene: sparse incremental bookkeeping is not cost effective"
	}

	// Deterministic cost model (lower estimated cost is better; scores are
	// normalized to a 0..100 fitness scale where 100 is best).
	// Full cost grows linearly with views and voxels.
	fullCost := float64(in.ViewCount) * float64(max64(in.VoxelCount, 1))
	// Block cache pays only for cache-miss blocks; camera change range drives
	// the expected miss fraction.
	missFraction := clamp(in.CameraChangeRange, 0, 1)
	blockCost := fullCost * (0.25 + 0.75*missFraction)
	// Sparse cost scales with the changed fraction only.
	sparseCost := fullCost * (0.1 + 0.9*missFraction)

	scores[StrategyFull] = fitness(fullCost, in.Budget)
	scores[StrategyBlockCache] = fitness(blockCost, in.Budget)
	scores[StrategySparse] = fitness(sparseCost, in.Budget)
	for k := range scores {
		scores[k] = round6(scores[k])
	}

	type cand struct {
		s     Strategy
		score float64
	}
	var ranked []cand
	for s, ok := range eligible {
		if ok {
			ranked = append(ranked, cand{s, scores[s]})
		}
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		return ranked[i].s < ranked[j].s
	})
	selected := ranked[0].s
	if !eligible[selected] {
		selected = StrategyBlockCache
	}
	for s, ok := range eligible {
		if !ok {
			continue
		}
		if s == selected {
			continue
		}
		if _, exists := rejected[s]; !exists {
			rejected[s] = fmt.Sprintf("lower fitness score %.3f vs selected %.3f", scores[s], scores[selected])
		}
	}

	reason := fmt.Sprintf(
		"selected %s with fitness %.3f; shards=%d views=%d voxels=%d camera_change=%.3f budget=%.0f",
		selected, scores[selected], in.ShardCount, in.ViewCount, in.VoxelCount,
		in.CameraChangeRange, in.Budget)

	return PlanRecord{
		Selected:       selected,
		Reason:         reason,
		Scores:         scores,
		Eligible:       eligible,
		RejectedReason: rejected,
		Inputs:         in,
	}
}

// fitness maps an estimated cost and a budget to a 0..100 fitness value.
func fitness(cost, budget float64) float64 {
	if budget <= 0 {
		budget = 1
	}
	ratio := cost / budget
	if ratio <= 0 {
		return 100
	}
	f := 100 / (1 + ratio)
	if f < 0 {
		f = 0
	}
	if f > 100 {
		f = 100
	}
	return f
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
