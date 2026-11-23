package engine

import (
	"fmt"
	"math/bits"
	"sort"

	"h2diag/internal/model"
)

type allocationResult struct {
	Allocations []int64
	Sampling    model.SamplingInfo
	Exact       bool
}

func allocateRound(entries []allocEntry, budget int64, config model.SampleConfig) allocationResult {
	exact := exactAllocate(budget, entries)
	info := model.SamplingInfo{
		Active:          false,
		EligibleStreams: len(entries),
		Threshold:       config.SchedulingThreshold,
		SamplingRatePPM: model.FixedScalePPM,
		SamplingRate:    "100% (exact full weighted allocation)",
		BucketMethod:    "none",
		Confidence:      "exact",
		ExactOrder:      orderForAllocation(entries, exact),
		EstimatedOrder:  orderForAllocation(entries, exact),
	}
	if config.Approximation != "bucketed" || len(entries) <= config.SchedulingThreshold ||
		len(entries) <= config.SamplingFallbackBelow {
		if config.Approximation == "bucketed" && len(entries) > config.SchedulingThreshold {
			info.FallbackReason = fmt.Sprintf("eligible streams %d <= sampling_fallback_below %d", len(entries), config.SamplingFallbackBelow)
		} else {
			info.FallbackReason = "exact path selected by configuration or threshold"
		}
		return allocationResult{Allocations: exact, Sampling: info, Exact: true}
	}

	estimated, sampleCount := bucketedEstimateAllocate(entries, budget, config.SchedulingThreshold)
	info.Active = true
	info.SamplingRatePPM = int64(sampleCount) * model.FixedScalePPM / int64(len(entries))
	info.SamplingRate = fmt.Sprintf("%d/%d streams", sampleCount, len(entries))
	info.BucketMethod = "log2(weight) buckets; deterministic stride sample"
	info.Confidence = fmt.Sprintf("low-to-medium: %d stratified samples over %d streams; estimates are diagnostic, not a statistical guarantee", sampleCount, len(entries))
	info.EstimatedOrder = orderForAllocation(entries, estimated)
	if equalOrders(info.ExactOrder, info.EstimatedOrder) {
		info.DifferenceReason = "exact and bucketed-estimate paths produced the same order in this round"
	} else {
		info.DifferenceReason = "order differs because bucketed path extrapolates per-log2(weight) demand from a stride sample before capped allocation; exact_order is the per-stream audit baseline"
	}
	return allocationResult{Allocations: estimated, Sampling: info, Exact: false}
}

func bucketedEstimateAllocate(entries []allocEntry, budget int64, sampleTarget int) ([]int64, int) {
	allocations := make([]int64, len(entries))
	if len(entries) == 0 || budget <= 0 {
		return allocations, 0
	}
	sortedIndices := make([]int, len(entries))
	for i := range entries {
		sortedIndices[i] = i
	}
	sort.Slice(sortedIndices, func(i, j int) bool {
		return entries[sortedIndices[i]].StreamID < entries[sortedIndices[j]].StreamID
	})
	if sampleTarget <= 0 {
		sampleTarget = 16
	}
	stride := (len(entries) + sampleTarget - 1) / sampleTarget
	if stride < 1 {
		stride = 1
	}
	type bucket struct {
		key          int
		count        int
		sampleCount  int
		sampleWeight int64
		sampleDemand int64
		actualDemand int64
		indices      []int
	}
	bucketsByKey := make(map[int]*bucket)
	sampleCount := 0
	for samplePos, sortedPos := range sortedIndices {
		index := sortedIndices[sortedPos]
		entry := entries[index]
		key := bits.Len64(uint64(entry.Weight))
		current := bucketsByKey[key]
		if current == nil {
			current = &bucket{key: key}
			bucketsByKey[key] = current
		}
		current.count++
		current.actualDemand += entry.Demand
		current.indices = append(current.indices, index)
		if samplePos%stride == 0 {
			current.sampleCount++
			current.sampleWeight += entry.Weight
			current.sampleDemand += entry.Demand
			sampleCount++
		}
	}
	keys := make([]int, 0, len(bucketsByKey))
	for key := range bucketsByKey {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	bucketEntries := make([]allocEntry, 0, len(keys))
	for _, key := range keys {
		current := bucketsByKey[key]
		estimatedWeight := current.sampleWeight
		estimatedDemand := current.sampleDemand
		if current.sampleCount > 0 {
			estimatedWeight = current.sampleWeight * int64(current.count) / int64(current.sampleCount)
			estimatedDemand = current.sampleDemand * int64(current.count) / int64(current.sampleCount)
		}
		if estimatedWeight <= 0 {
			estimatedWeight = 1
		}
		if estimatedDemand > current.actualDemand {
			estimatedDemand = current.actualDemand
		}
		bucketEntries = append(bucketEntries, allocEntry{
			StreamID: int64(key),
			Weight:   estimatedWeight,
			Demand:   estimatedDemand,
		})
	}
	bucketAlloc := exactAllocate(budget, bucketEntries)
	for i, key := range keys {
		if bucketAlloc[i] <= 0 {
			continue
		}
		current := bucketsByKey[key]
		members := make([]allocEntry, 0, len(current.indices))
		memberPositions := make([]int, 0, len(current.indices))
		for _, index := range current.indices {
			members = append(members, entries[index])
			memberPositions = append(memberPositions, index)
		}
		memberAlloc := exactAllocate(bucketAlloc[i], members)
		for j, amount := range memberAlloc {
			allocations[memberPositions[j]] = amount
		}
	}
	return allocations, sampleCount
}

func orderForAllocation(entries []allocEntry, allocations []int64) []int64 {
	indices := make([]int, 0, len(entries))
	for i, amount := range allocations {
		if amount > 0 {
			indices = append(indices, i)
		}
	}
	sort.Slice(indices, func(i, j int) bool {
		left, right := indices[i], indices[j]
		if allocations[left] != allocations[right] {
			return allocations[left] > allocations[right]
		}
		if entries[left].Weight != entries[right].Weight {
			return entries[left].Weight > entries[right].Weight
		}
		return entries[left].StreamID < entries[right].StreamID
	})
	order := make([]int64, 0, len(indices))
	for _, index := range indices {
		order = append(order, entries[index].StreamID)
	}
	return order
}

func equalOrders(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
