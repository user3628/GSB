package scheduler

import (
	"fmt"
	"sort"

	"localinfer/workbench/internal/domain"
)

const quantileMethod = "nearest-rank"

var declaredQuantiles = []int{50, 95, 99}

type costBucket struct {
	costUS int64
	count  int64
}

// computeQuantiles treats each allocated sample as one latency observation
// whose value is the per-sample compute cost. This makes the sample size
// explicit and allows mixed-model batches to have a non-degenerate spread.
func computeQuantiles(batch domain.PlannedBatch, catalog *domain.ModelCatalog, decimals int) []domain.QuantileResult {
	buckets := make(map[int64]int64)
	var sampleCount int64
	for _, m := range batch.Members {
		cost := catalog.ByID[m.ModelID].CostPerSample
		buckets[cost] += m.AllocatedSamples
		sampleCount += m.AllocatedSamples
	}
	sorted := make([]costBucket, 0, len(buckets))
	for cost, count := range buckets {
		sorted = append(sorted, costBucket{costUS: cost, count: count})
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].costUS < sorted[j].costUS })

	results := make([]domain.QuantileResult, 0, len(declaredQuantiles))
	for _, p := range declaredQuantiles {
		minSamples := minSamplesForQuantile(p)
		result := domain.QuantileResult{
			Quantile: p, Method: quantileMethod, SampleCount: int(sampleCount), MinSamples: minSamples,
		}
		if sampleCount < int64(minSamples) {
			result.Available = false
			result.ErrorCode = domain.CodeInsufficientSample
			result.Reason = fmt.Sprintf("nearest-rank P%d requires at least %d samples for declared tail resolution; batch has %d", p, minSamples, sampleCount)
			results = append(results, result)
			continue
		}
		n := int64(sampleCount)
		rank := (int64(p)*n + 99) / 100 // ceil(p*N/100), 1-based
		if rank < 1 {
			rank = 1
		}
		var seen int64
		var selected int64
		for _, bucket := range sorted {
			seen += bucket.count
			if seen >= rank {
				selected = bucket.costUS
				break
			}
		}
		result.Available = true
		result.Rank = int(rank)
		result.LatencyUS = selected
		result.LatencyMS = formatMS(selected, decimals)
		results = append(results, result)
	}
	return results
}

// minSamplesForQuantile declares the resolution required for a tail quantile.
// This is intentionally stricter than the bare nearest-rank lower bound: a P99
// conclusion is not emitted from a batch with fewer than 100 observations.
func minSamplesForQuantile(p int) int {
	if p >= 100 || p <= 0 {
		return 0
	}
	return (100 + (100 - p) - 1) / (100 - p)
}

func Quantile(result []domain.QuantileResult, p int) (domain.QuantileResult, bool) {
	for _, r := range result {
		if r.Quantile == p {
			return r, true
		}
	}
	return domain.QuantileResult{}, false
}
