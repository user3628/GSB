package engine

import (
	"math/big"
	"sort"

	"h2diag/internal/model"
)

type allocEntry struct {
	StreamID int64
	Weight   int64
	Demand   int64
}

type exactQuota struct {
	index     int
	base      int64
	remainder *big.Int
}

func exactAllocate(budget int64, entries []allocEntry) []int64 {
	allocations := make([]int64, len(entries))
	if budget <= 0 {
		return allocations
	}
	active := make([]bool, len(entries))
	for i, entry := range entries {
		active[i] = entry.Demand > 0
	}
	remaining := budget
	for remaining > 0 {
		totalWeight := new(big.Int)
		activeCount := 0
		for i, entry := range entries {
			if !active[i] {
				continue
			}
			totalWeight.Add(totalWeight, big.NewInt(entry.Weight))
			activeCount++
		}
		if activeCount == 0 || totalWeight.Sign() == 0 {
			break
		}

		quotas := make([]exactQuota, 0, activeCount)
		capped := false
		baseTotal := int64(0)
		for i, entry := range entries {
			if !active[i] {
				continue
			}
			numerator := new(big.Int).Mul(big.NewInt(remaining), big.NewInt(entry.Weight))
			base, remainder := new(big.Int), new(big.Int)
			base.QuoRem(numerator, totalWeight, remainder)
			baseInt := base.Int64()
			capacity := entry.Demand - allocations[i]
			if baseInt >= capacity {
				baseInt = capacity
				capped = true
			}
			allocations[i] += baseInt
			baseTotal += baseInt
			quotas = append(quotas, exactQuota{index: i, base: baseInt, remainder: remainder})
		}
		remaining -= baseTotal
		if capped {
			for i, entry := range entries {
				if allocations[i] >= entry.Demand {
					active[i] = false
				}
			}
			continue
		}
		sort.SliceStable(quotas, func(i, j int) bool {
			cmp := quotas[i].remainder.Cmp(quotas[j].remainder)
			if cmp != 0 {
				return cmp > 0
			}
			return entries[quotas[i].index].StreamID < entries[quotas[j].index].StreamID
		})
		for _, quota := range quotas {
			if remaining == 0 {
				break
			}
			if quota.remainder.Sign() == 0 || allocations[quota.index] >= entries[quota.index].Demand {
				continue
			}
			allocations[quota.index]++
			remaining--
		}
		break
	}
	return allocations
}

func normalizeWeights(entries []allocEntry, scale int64) ([]model.WeightBit, int64) {
	bits := make([]model.WeightBit, len(entries))
	if len(entries) == 0 || scale <= 0 {
		return bits, 0
	}
	totalWeight := new(big.Int)
	for _, entry := range entries {
		totalWeight.Add(totalWeight, big.NewInt(entry.Weight))
	}
	if totalWeight.Sign() == 0 {
		return bits, 0
	}
	type residual struct {
		index     int
		remainder *big.Int
	}
	residuals := make([]residual, 0, len(entries))
	truncatedTotal := int64(0)
	for i, entry := range entries {
		numerator := new(big.Int).Mul(big.NewInt(entry.Weight), big.NewInt(scale))
		quotient, remainder := new(big.Int), new(big.Int)
		quotient.QuoRem(numerator, totalWeight, remainder)
		truncated := quotient.Int64()
		bits[i] = model.WeightBit{
			StreamID:           entry.StreamID,
			Weight:             entry.Weight,
			ExactNumerator:     numerator.String(),
			TruncatedSharePPM:  truncated,
			RemainderNumerator: remainder.String(),
			NormalizedSharePPM: truncated,
		}
		truncatedTotal += truncated
		residuals = append(residuals, residual{index: i, remainder: remainder})
	}
	leftover := scale - truncatedTotal
	sort.SliceStable(residuals, func(i, j int) bool {
		cmp := residuals[i].remainder.Cmp(residuals[j].remainder)
		if cmp != 0 {
			return cmp > 0
		}
		return entries[residuals[i].index].StreamID < entries[residuals[j].index].StreamID
	})
	for i := 0; i < len(residuals) && leftover > 0; i++ {
		if residuals[i].remainder.Sign() == 0 {
			continue
		}
		index := residuals[i].index
		bits[index].AdjustmentPPM = 1
		bits[index].NormalizedSharePPM++
		leftover--
	}
	return bits, scale - truncatedTotal
}

func totalDemand(entries []allocEntry) int64 {
	var total int64
	for _, entry := range entries {
		if entry.Demand > 0 {
			total += entry.Demand
		}
	}
	return total
}
