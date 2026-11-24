package rewardpack

import (
	"math"
	"sort"
)

const (
	factorLength      = "length"
	factorOverlap     = "overlap"
	factorConsistency = "consistency"
)

// ScorePair decomposes preference strength into three multiplicative factors
// evaluated in log space. strength = exp(sum(log_contribution)).
func ScorePair(n NormalizedPair, a Annotation, cfg Config) (ScoreResult, error) {
	lengthLog := -cfg.Scoring.LengthPenalty * math.Abs(math.Log(float64(1+n.Chosen.TokenCount)/float64(1+n.Rejected.TokenCount)))
	if !finite(lengthLog) {
		return ScoreResult{}, &BusinessError{Code: CodeNumeric, PairID: n.PairID, Factor: factorLength, Message: "length log contribution is NaN or Inf"}
	}
	lengthValue, reason, err := flooredFactor(factorLength, n.PairID, math.Exp(lengthLog), lengthLog, cfg.Scoring.FactorFloors.Length)
	if err != nil {
		return ScoreResult{}, err
	}

	overlapLog, overlapFallbackReason := overlapContribution(n, cfg)
	overlapValue, overlapReason, err := flooredFactor(factorOverlap, n.PairID, math.Exp(overlapLog), overlapLog, cfg.Scoring.FactorFloors.Overlap)
	if err != nil {
		return ScoreResult{}, err
	}
	if overlapFallbackReason != "" {
		overlapReason = overlapFallbackReason
	}

	consistency := a.Consistency
	if math.IsNaN(consistency) || math.IsInf(consistency, 0) {
		return ScoreResult{}, &BusinessError{Code: CodeNumeric, PairID: n.PairID, Factor: factorConsistency, Message: "annotation consistency is NaN or Inf"}
	}
	consistencyReason := ""
	if consistency < 0 || consistency > 1 {
		consistencyReason = "annotation_consistency_out_of_range"
		consistency = cfg.Scoring.FactorFloors.Consistency
	}
	if consistency <= 0 {
		consistency = cfg.Scoring.FactorFloors.Consistency
		if consistencyReason == "" {
			consistencyReason = "annotation_consistency_non_positive"
		}
	}
	consistencyLog := math.Log(consistency)
	consistencyValue, consistencyFloorReason, err := flooredFactor(factorConsistency, n.PairID, consistency, consistencyLog, cfg.Scoring.FactorFloors.Consistency)
	if err != nil {
		return ScoreResult{}, err
	}
	if consistencyReason == "" {
		consistencyReason = consistencyFloorReason
	}

	factors := []Factor{
		{Name: factorLength, Value: lengthValue, LogContribution: math.Log(lengthValue), Fallback: reason != "", FallbackReason: reason},
		{Name: factorOverlap, Value: overlapValue, LogContribution: math.Log(overlapValue), Fallback: overlapReason != "", FallbackReason: overlapReason},
		{Name: factorConsistency, Value: consistencyValue, LogContribution: math.Log(consistencyValue), Fallback: consistencyReason != "", FallbackReason: consistencyReason},
	}
	logStrength := 0.0
	for _, factor := range factors {
		if !finite(factor.Value) || factor.Value <= 0 || !finite(factor.LogContribution) {
			return ScoreResult{}, &BusinessError{Code: CodeNumeric, PairID: n.PairID, Factor: factor.Name, Message: "factor value or log contribution is NaN, Inf or non-positive"}
		}
		logStrength += factor.LogContribution
	}
	if !finite(logStrength) {
		return ScoreResult{}, &BusinessError{Code: CodeNumeric, PairID: n.PairID, Factor: "strength", Message: "final log strength is NaN or Inf"}
	}
	strength := math.Exp(logStrength)
	if !finite(strength) {
		return ScoreResult{}, &BusinessError{Code: CodeNumeric, PairID: n.PairID, Factor: "strength", Message: "final strength is NaN or Inf"}
	}
	rounded, text := roundSignificant(strength, cfg.Scoring.StrengthSignificantDigits)
	result := ScoreResult{
		PairID:          n.PairID,
		Source:          n.Source,
		Lang:            n.Lang,
		Factors:         factors,
		LogStrength:     logStrength,
		Strength:        rounded,
		StrengthText:    text,
		EffectiveDigits: cfg.Scoring.StrengthSignificantDigits,
		TokenCost:       n.TokenCost,
		InputDigest:     hashBytes([]byte("score\x00"), []byte(n.Digest), []byte(cfg.ScoringDigest())),
	}
	result.Digest = digestJSON(result)
	return result, nil
}

func overlapContribution(n NormalizedPair, cfg Config) (float64, string) {
	promptWeight := weightedTerms(n.Prompt, cfg)
	if promptWeight <= 0 || !finite(promptWeight) {
		return math.Log(cfg.Scoring.FactorFloors.Overlap), "prompt_has_no_weighted_terms"
	}
	chosenOverlap := weightedIntersection(n.Prompt, n.Chosen, cfg) / promptWeight
	rejectedOverlap := weightedIntersection(n.Prompt, n.Rejected, cfg) / promptWeight
	direction := chosenOverlap - rejectedOverlap
	if !finite(direction) {
		return math.Log(cfg.Scoring.FactorFloors.Overlap), "overlap_direction_non_finite"
	}
	direction = clamp(direction*cfg.Scoring.OverlapScale, -1, 1)
	return direction, ""
}

func flooredFactor(name, pairID string, value, logValue, floor float64) (float64, string, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || math.IsNaN(logValue) || math.IsInf(logValue, 0) {
		return 0, "", &BusinessError{Code: CodeNumeric, PairID: pairID, Factor: name, Message: "factor is NaN or Inf"}
	}
	if value <= 0 {
		return floor, "factor_non_positive", nil
	}
	if value < floor {
		return floor, "factor_below_configured_floor", nil
	}
	return value, "", nil
}

func weightedTerms(features TextFeatures, cfg Config) float64 {
	total := 0.0
	for _, term := range features.Terms {
		total += float64(term.Count) * termWeight(term.Term, cfg)
	}
	return total
}

func weightedIntersection(prompt, response TextFeatures, cfg Config) float64 {
	promptCounts := make(map[string]int, len(prompt.Terms))
	for _, term := range prompt.Terms {
		promptCounts[term.Term] = term.Count
	}
	total := 0.0
	for _, term := range response.Terms {
		if count := promptCounts[term.Term]; count > 0 {
			common := count
			if term.Count < common {
				common = term.Count
			}
			total += float64(common) * termWeight(term.Term, cfg)
		}
	}
	return total
}

func termWeight(term string, cfg Config) float64 {
	if weight, ok := cfg.Scoring.TermWeights[term]; ok {
		return weight
	}
	return cfg.Scoring.DefaultTermWeight
}

// SortScores sorts by pair id, the canonical artifact order.
func SortScores(scores []ScoreResult) {
	sort.Slice(scores, func(i, j int) bool { return scores[i].PairID < scores[j].PairID })
}
