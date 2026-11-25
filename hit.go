package prefixcache

import "sort"

func leadingBlockSimilarity(query, cached []string) (common int, similarity float64) {
	limit := len(query)
	if len(cached) < limit {
		limit = len(cached)
	}
	for common < limit && query[common] == cached[common] {
		common++
	}
	denominator := len(query)
	if len(cached) > denominator {
		denominator = len(cached)
	}
	if denominator == 0 {
		return 0, 0
	}
	return common, float64(common) / float64(denominator)
}

type queryCandidate struct {
	candidate PrefixCandidate
	blocks    []string
}

func queryCandidates(plan RequestPlan, policy Policy) []queryCandidate {
	out := make([]queryCandidate, 0)
	for _, candidate := range plan.Prefixes {
		if !candidate.Cacheable || candidate.PrefixChars < policy.MinPrefixLen {
			continue
		}
		out = append(out, queryCandidate{candidate: candidate, blocks: append([]string(nil), candidate.Fingerprints...)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].candidate.PrefixChars != out[j].candidate.PrefixChars {
			return out[i].candidate.PrefixChars > out[j].candidate.PrefixChars
		}
		return out[i].candidate.PrefixKey < out[j].candidate.PrefixKey
	})
	if len(out) > policy.MaxPrefixesPerRequest {
		out = out[:policy.MaxPrefixesPerRequest]
	}
	return out
}

// DecideHit evaluates only cacheable query prefixes against occupied slots. It
// searches from the longest reusable prefix down and returns the first hit.
func DecideHit(version, observedVersion int64, requestID string, plan RequestPlan, policy Policy, slots []Slot) HitResult {
	result := HitResult{
		Version:         version,
		ObservedVersion: observedVersion,
		RequestID:       requestID,
		Decision:        DecisionNotReusable,
		Evidence: SimilarityEvidence{
			Threshold:       policy.MergeThreshold,
			ThresholdMargin: -policy.MergeThreshold,
		},
	}

	candidates := queryCandidates(plan, policy)
	if len(candidates) == 0 {
		result.Reasons = append(result.Reasons, "no_cacheable_prefix_meeting_min_prefix_len")
		return result
	}

	bestSimilarity := -1.0
	bestCommon := 0
	bestCachedBlocks := 0
	for _, query := range candidates {
		result.QueryChars = query.candidate.PrefixChars
		result.QueryPrefixKey = query.candidate.PrefixKey

		var bestMerged *Slot
		bestMergedSimilarity := -1.0
		bestMergedCommon := 0
		for i := range slots {
			slot := &slots[i]
			if slot.PrefixKey == query.candidate.PrefixKey {
				slotNo := slot.SlotNo
				result.Decision = DecisionExact
				result.SlotNo = &slotNo
				result.SlotPrefixKey = slot.PrefixKey
				if len(slot.Owners) > 0 {
					result.SlotRequestID = slot.Owners[0]
				}
				result.CachedChars = slot.PrefixChars
				result.Evidence = SimilarityEvidence{
					Exact:              true,
					CommonPrefixBlocks: len(query.blocks),
					QueryBlocks:        len(query.blocks),
					CachedBlocks:       slot.BlockCount,
					Similarity:         1,
					Threshold:          policy.MergeThreshold,
					ThresholdMargin:    1 - policy.MergeThreshold,
				}
				result.Reasons = append(result.Reasons, "block_fingerprints_equal")
				return result
			}

			common, similarity := leadingBlockSimilarity(query.blocks, slot.BlockFingerprints)
			margin := similarity - policy.MergeThreshold
			if common > bestCommon || (common == bestCommon && similarity > bestSimilarity) {
				bestCommon = common
				bestSimilarity = similarity
				bestCachedBlocks = slot.BlockCount
				result.Evidence = SimilarityEvidence{
					CommonPrefixBlocks: common,
					QueryBlocks:        len(query.blocks),
					CachedBlocks:       slot.BlockCount,
					Similarity:         similarity,
					Threshold:          policy.MergeThreshold,
					ThresholdMargin:    margin,
				}
			}
			if common == 0 || similarity < policy.MergeThreshold {
				continue
			}
			if bestMerged == nil ||
				similarity > bestMergedSimilarity ||
				(similarity == bestMergedSimilarity && common > bestMergedCommon) ||
				(similarity == bestMergedSimilarity && common == bestMergedCommon && slot.PrefixChars > bestMerged.PrefixChars) ||
				(similarity == bestMergedSimilarity && common == bestMergedCommon && slot.PrefixChars == bestMerged.PrefixChars && slot.SlotNo < bestMerged.SlotNo) {
				bestMerged = slot
				bestMergedSimilarity = similarity
				bestMergedCommon = common
			}
		}
		if bestMerged != nil {
			slotNo := bestMerged.SlotNo
			result.Decision = DecisionMerged
			result.SlotNo = &slotNo
			result.SlotPrefixKey = bestMerged.PrefixKey
			if len(bestMerged.Owners) > 0 {
				result.SlotRequestID = bestMerged.Owners[0]
			}
			result.CachedChars = bestMerged.PrefixChars
			result.Evidence = SimilarityEvidence{
				CommonPrefixBlocks: bestMergedCommon,
				QueryBlocks:        len(query.blocks),
				CachedBlocks:       bestMerged.BlockCount,
				Similarity:         bestMergedSimilarity,
				Threshold:          policy.MergeThreshold,
				ThresholdMargin:    bestMergedSimilarity - policy.MergeThreshold,
			}
			result.Reasons = append(result.Reasons, "leading_block_similarity_meets_threshold")
			return result
		}
	}

	result.QueryPrefixKey = ""
	if bestSimilarity >= 0 {
		result.Evidence.Similarity = bestSimilarity
		result.Evidence.CommonPrefixBlocks = bestCommon
		result.Evidence.CachedBlocks = bestCachedBlocks
		result.Evidence.ThresholdMargin = bestSimilarity - policy.MergeThreshold
		result.Reasons = append(result.Reasons, "best_similarity_below_threshold")
	} else {
		result.Reasons = append(result.Reasons, "no_occupied_slot_comparable")
	}
	return result
}
