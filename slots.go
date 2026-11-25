package prefixcache

import (
	"fmt"
	"sort"
)

type candidateInfo struct {
	PrefixKey         string
	BlockFingerprints []string
	BlockCount        int
	PrefixChars       int
	Owners            []string
}

type requestBuild struct {
	plan        RequestPlan
	allocations []PrefixAllocation
}

type allocationResult struct {
	Slots       []Slot
	Builds      []requestBuild
	Evictions   []Eviction
	ReusedCount int
}

// AllocateSlots performs deterministic shortest-prefix filtering, per-request
// truncation, occupancy-budget selection, and direct-mapped collision handling.
// priorSlots may be reused only when the caller's earliest invalidated stage
// does not include slotting.
func AllocateSlots(version, allocationSequence int64, policy Policy, plans []RequestPlan, priorSlots []Slot, reuseAllowed bool) (allocationResult, error) {
	sortedPlans := append([]RequestPlan(nil), plans...)
	sort.Slice(sortedPlans, func(i, j int) bool { return sortedPlans[i].RequestID < sortedPlans[j].RequestID })

	evictions := make([]Eviction, 0)
	builds := make([]requestBuild, 0, len(sortedPlans))
	unique := make(map[string]*candidateInfo)
	for _, plan := range sortedPlans {
		build := requestBuild{plan: plan, allocations: make([]PrefixAllocation, 0, len(plan.Prefixes))}
		eligible := make([]int, 0, len(plan.Prefixes))
		for _, prefix := range plan.Prefixes {
			allocation := PrefixAllocation{
				PrefixKey:   prefix.PrefixKey,
				BlockCount:  prefix.BlockCount,
				PrefixChars: prefix.PrefixChars,
			}
			switch {
			case !prefix.Cacheable:
				allocation.Status = "uncacheable"
				allocation.Reason = prefix.RejectReason
			case prefix.PrefixChars < policy.MinPrefixLen:
				allocation.Status = "below_min_prefix"
				allocation.Reason = fmt.Sprintf("prefix_chars=%d min_prefix_len=%d", prefix.PrefixChars, policy.MinPrefixLen)
			default:
				allocation.Status = "eligible"
				eligible = append(eligible, len(build.allocations))
			}
			build.allocations = append(build.allocations, allocation)
		}

		if len(eligible) > policy.MaxPrefixesPerRequest {
			dropCount := len(eligible) - policy.MaxPrefixesPerRequest
			for _, index := range eligible[:dropCount] {
				build.allocations[index].Status = "truncated_max_prefixes"
				build.allocations[index].Reason = fmt.Sprintf("kept longest %d prefixes per request", policy.MaxPrefixesPerRequest)
				prefix := build.allocations[index]
				evictions = append(evictions, Eviction{
					Version:     version,
					RequestID:   plan.RequestID,
					SlotNo:      -1,
					PrefixKey:   prefix.PrefixKey,
					PrefixChars: prefix.PrefixChars,
					Reason:      "max_prefixes_per_request",
					Threshold:   policy.MergeThreshold,
					Note:        "truncated: only the longest declared number of prefixes per request are considered",
				})
			}
			eligible = eligible[dropCount:]
		}

		for _, index := range eligible {
			allocation := &build.allocations[index]
			prefix := findPrefix(plan, allocation.PrefixKey)
			if prefix == nil {
				return allocationResult{}, newError(CodeInternal, "prefix lookup failed")
			}
			candidate, ok := unique[prefix.PrefixKey]
			if !ok {
				candidate = &candidateInfo{
					PrefixKey:         prefix.PrefixKey,
					BlockFingerprints: append([]string(nil), prefix.Fingerprints...),
					BlockCount:        prefix.BlockCount,
					PrefixChars:       prefix.PrefixChars,
				}
				unique[prefix.PrefixKey] = candidate
			}
			candidate.Owners = append(candidate.Owners, plan.RequestID)
		}
		builds = append(builds, build)
	}

	candidates := make([]*candidateInfo, 0, len(unique))
	for _, candidate := range unique {
		sort.Strings(candidate.Owners)
		candidate.Owners = dedupeStrings(candidate.Owners)
		candidates = append(candidates, candidate)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].PrefixChars != candidates[j].PrefixChars {
			return candidates[i].PrefixChars > candidates[j].PrefixChars
		}
		if candidates[i].BlockCount != candidates[j].BlockCount {
			return candidates[i].BlockCount > candidates[j].BlockCount
		}
		return candidates[i].PrefixKey < candidates[j].PrefixKey
	})

	if len(candidates) > 0 && policy.SlotCount == 0 {
		return allocationResult{}, slotConflictError(candidates[0].Owners[0], 0, "slot_count is zero while cacheable prefixes exist")
	}

	winners := candidates
	if len(winners) > policy.OccupancyBudget {
		for _, candidate := range winners[policy.OccupancyBudget:] {
			for _, requestID := range candidate.Owners {
				evictions = append(evictions, Eviction{
					Version:     version,
					RequestID:   requestID,
					SlotNo:      slotForPrefix(candidate.PrefixKey, policy.SlotCount),
					PrefixKey:   candidate.PrefixKey,
					PrefixChars: candidate.PrefixChars,
					Reason:      "occupancy_budget",
					Threshold:   policy.MergeThreshold,
					Note:        "not retained: occupancy_budget requires prefix_chars descending",
				})
			}
			setAllocationState(builds, candidate.PrefixKey, "evicted_budget", nil, "occupancy_budget", 0)
		}
		winners = winners[:policy.OccupancyBudget]
	}

	priorByKey := make(map[string]Slot, len(priorSlots))
	for _, slot := range priorSlots {
		priorByKey[slot.PrefixKey] = slot
	}

	slotsByNo := make(map[int]*Slot, len(winners))
	keyToSlot := make(map[string]int, len(winners))
	for rank, candidate := range winners {
		desired := slotForPrefix(candidate.PrefixKey, policy.SlotCount)
		occupiedAt := allocationSequence*1_000_000 + int64(rank+1)
		reusedFrom := int64(0)
		if reuseAllowed {
			if prior, ok := priorByKey[candidate.PrefixKey]; ok && prior.SlotNo == desired {
				occupiedAt = prior.OccupiedAt
				reusedFrom = prior.Version
			}
		}

		current, occupied := slotsByNo[desired]
		if !occupied {
			slotsByNo[desired] = newSlot(version, desired, candidate, occupiedAt, reusedFrom)
			keyToSlot[candidate.PrefixKey] = desired
			setAllocationState(builds, candidate.PrefixKey, "kept", &desired, "", reusedFrom)
			continue
		}
		if current.PrefixKey == candidate.PrefixKey {
			current.Owners = dedupeStrings(append(current.Owners, candidate.Owners...))
			sort.Strings(current.Owners)
			setAllocationState(builds, candidate.PrefixKey, "kept", &desired, "", current.ReusedFrom)
			continue
		}

		_, similarity := leadingBlockSimilarity(candidate.BlockFingerprints, current.BlockFingerprints)
		candidateWins, note := conflictWinner(candidate, occupiedAt, current, policy.MergeThreshold)
		if candidateWins {
			for _, requestID := range current.Owners {
				evictions = append(evictions, Eviction{
					Version:          version,
					RequestID:        requestID,
					SlotNo:           desired,
					PrefixKey:        current.PrefixKey,
					PrefixChars:      current.PrefixChars,
					Reason:           "slot_conflict",
					WinnerPrefixKey:  candidate.PrefixKey,
					WinnerRequestID:  firstOwner(candidate.Owners),
					Similarity:       similarity,
					Threshold:        policy.MergeThreshold,
					ThresholdMargin:  similarity - policy.MergeThreshold,
					OccupiedAt:       current.OccupiedAt,
					WinnerOccupiedAt: occupiedAt,
					Note:             note,
				})
			}
			setAllocationState(builds, current.PrefixKey, "evicted_conflict", nil, "slot_conflict", 0)
			delete(keyToSlot, current.PrefixKey)
			slotsByNo[desired] = newSlot(version, desired, candidate, occupiedAt, reusedFrom)
			keyToSlot[candidate.PrefixKey] = desired
			setAllocationState(builds, candidate.PrefixKey, "kept", &desired, "", reusedFrom)
		} else {
			for _, requestID := range candidate.Owners {
				evictions = append(evictions, Eviction{
					Version:          version,
					RequestID:        requestID,
					SlotNo:           desired,
					PrefixKey:        candidate.PrefixKey,
					PrefixChars:      candidate.PrefixChars,
					Reason:           "slot_conflict",
					WinnerPrefixKey:  current.PrefixKey,
					WinnerRequestID:  firstOwner(current.Owners),
					Similarity:       similarity,
					Threshold:        policy.MergeThreshold,
					ThresholdMargin:  similarity - policy.MergeThreshold,
					OccupiedAt:       occupiedAt,
					WinnerOccupiedAt: current.OccupiedAt,
					Note:             note,
				})
			}
			setAllocationState(builds, candidate.PrefixKey, "evicted_conflict", nil, "slot_conflict", 0)
		}
	}

	slots := make([]Slot, 0, len(slotsByNo))
	seenKeys := make(map[string]int, len(slotsByNo))
	for _, slot := range slotsByNo {
		sort.Strings(slot.Owners)
		if existing, ok := seenKeys[slot.PrefixKey]; ok && existing != slot.SlotNo {
			return allocationResult{}, slotConflictError(firstOwner(slot.Owners), slot.SlotNo, "the same prefix was assigned to multiple slots")
		}
		seenKeys[slot.PrefixKey] = slot.SlotNo
		slots = append(slots, *slot)
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i].SlotNo < slots[j].SlotNo })
	if len(slots) > policy.OccupancyBudget {
		return allocationResult{}, slotConflictError(firstOwner(slots[0].Owners), slots[0].SlotNo, "occupancy exceeds the declared budget")
	}
	for _, build := range builds {
		for _, allocation := range build.allocations {
			if allocation.Status != "kept" || allocation.SlotNo == nil {
				continue
			}
			mapped, ok := keyToSlot[allocation.PrefixKey]
			if !ok || mapped != *allocation.SlotNo {
				return allocationResult{}, slotConflictError(build.plan.RequestID, *allocation.SlotNo, "prefix allocation and slot map disagree")
			}
		}
	}

	sort.Slice(evictions, func(i, j int) bool {
		if evictions[i].Reason != evictions[j].Reason {
			return evictions[i].Reason < evictions[j].Reason
		}
		if evictions[i].RequestID != evictions[j].RequestID {
			return evictions[i].RequestID < evictions[j].RequestID
		}
		if evictions[i].SlotNo != evictions[j].SlotNo {
			return evictions[i].SlotNo < evictions[j].SlotNo
		}
		return evictions[i].PrefixKey < evictions[j].PrefixKey
	})

	reused := 0
	for _, slot := range slots {
		if slot.ReusedFrom != 0 {
			reused++
		}
	}
	return allocationResult{Slots: slots, Builds: builds, Evictions: evictions, ReusedCount: reused}, nil
}

func newSlot(version int64, slotNo int, candidate *candidateInfo, occupiedAt, reusedFrom int64) *Slot {
	return &Slot{
		Version:           version,
		SlotNo:            slotNo,
		PrefixKey:         candidate.PrefixKey,
		BlockFingerprints: append([]string(nil), candidate.BlockFingerprints...),
		BlockCount:        candidate.BlockCount,
		PrefixChars:       candidate.PrefixChars,
		OccupiedAt:        occupiedAt,
		ReusedFrom:        reusedFrom,
		Owners:            append([]string(nil), candidate.Owners...),
	}
}

func conflictWinner(candidate *candidateInfo, candidateOccupiedAt int64, incumbent *Slot, threshold float64) (bool, string) {
	if candidate.PrefixChars != incumbent.PrefixChars {
		if candidate.PrefixChars > incumbent.PrefixChars {
			return true, "candidate retained: longer reusable prefix"
		}
		return false, "incumbent retained: longer reusable prefix"
	}
	common, similarity := leadingBlockSimilarity(candidate.BlockFingerprints, incumbent.BlockFingerprints)
	if common > 0 && similarity >= threshold {
		return false, "incumbent retained: equal length and merge-compatible occupancy"
	}
	if candidateOccupiedAt != incumbent.OccupiedAt {
		if candidateOccupiedAt < incumbent.OccupiedAt {
			return true, "candidate retained: older deterministic occupancy"
		}
		return false, "incumbent retained: older deterministic occupancy"
	}
	if candidate.PrefixKey < incumbent.PrefixKey {
		return true, "candidate retained: lexicographic prefix key tie-break"
	}
	return false, "incumbent retained: lexicographic prefix key tie-break"
}

func findPrefix(plan RequestPlan, key string) *PrefixCandidate {
	for i := range plan.Prefixes {
		if plan.Prefixes[i].PrefixKey == key {
			return &plan.Prefixes[i]
		}
	}
	return nil
}

func setAllocationState(builds []requestBuild, key, status string, slotNo *int, reason string, reusedFrom int64) {
	for bi := range builds {
		for ai := range builds[bi].allocations {
			allocation := &builds[bi].allocations[ai]
			if allocation.PrefixKey != key {
				continue
			}
			allocation.Status = status
			allocation.Reason = reason
			allocation.ReusedFrom = reusedFrom
			if slotNo == nil {
				allocation.SlotNo = nil
			} else {
				value := *slotNo
				allocation.SlotNo = &value
			}
		}
	}
}

func firstOwner(owners []string) string {
	if len(owners) == 0 {
		return ""
	}
	return owners[0]
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}
