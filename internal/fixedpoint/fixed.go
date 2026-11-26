package fixedpoint

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
)

const (
	Scale         int64 = 1_000_000
	PPM           int64 = 1_000_000
	HalfUpLabel         = "round_half_up_to_Q6"
	ToleranceCeil       = "round_half_up_relative_tolerance"
)

type Fraction struct {
	Numerator   string `json:"numerator"`
	Denominator string `json:"denominator"`
}

func NewFraction(value *big.Rat) Fraction {
	if value == nil {
		return Fraction{Numerator: "0", Denominator: "1"}
	}
	normalized := new(big.Rat).Set(value)
	if normalized.Sign() == 0 {
		return Fraction{Numerator: "0", Denominator: "1"}
	}
	return Fraction{Numerator: normalized.Num().String(), Denominator: normalized.Denom().String()}
}

type Step struct {
	Name                   string   `json:"name"`
	ExactFormula           string   `json:"exactFormula"`
	RoundedQ               int64    `json:"roundedQ"`
	RoundingMode           string   `json:"roundingMode"`
	LocalErrorInQUnits     Fraction `json:"localErrorInQUnits"`
	CumulativeErrorInScore Fraction `json:"cumulativeErrorInScoreQUnits"`
}

type Weights struct {
	RuleCountQ int64 `json:"ruleCountWeightQ"`
	ConflictQ  int64 `json:"conflictWeightQ"`
	MergeQ     int64 `json:"mergeWeightQ"`
}

func DefaultWeights() Weights {
	return Weights{RuleCountQ: 200_000, ConflictQ: 500_000, MergeQ: 300_000}
}

type Input struct {
	Namespace       string `json:"namespace"`
	RuleCount       int    `json:"ruleCount"`
	TotalPairs      int    `json:"totalPairs"`
	ConflictPairs   int    `json:"conflictPairs"`
	RawIntervals    int    `json:"rawIntervals"`
	MergedIntervals int    `json:"mergedIntervals"`
	RuleRefCount    int64  `json:"ruleRefCount"`
	SnapshotVersion int64  `json:"snapshotVersion"`
}

type Result struct {
	Namespace               string   `json:"namespace"`
	SnapshotVersion         int64    `json:"snapshotVersion"`
	Scale                   int64    `json:"scale"`
	Weights                 Weights  `json:"weights"`
	RuleCount               int      `json:"ruleCount"`
	CompiledRuleCountTermQ  int64    `json:"compiledRuleCountTermQ"`
	TotalPairs              int      `json:"totalPairs"`
	ConflictPairs           int      `json:"conflictPairs"`
	ConflictRatioQ          int64    `json:"conflictRatioQ"`
	ConflictFreeTermQ       int64    `json:"conflictFreeTermQ"`
	RawIntervals            int      `json:"rawIntervals"`
	MergedIntervals         int      `json:"mergedIntervals"`
	MergeRateQ              int64    `json:"mergeRateQ"`
	RuleContributionQ       int64    `json:"ruleContributionQ"`
	ConflictContributionQ   int64    `json:"conflictContributionQ"`
	MergeContributionQ      int64    `json:"mergeContributionQ"`
	ScoreQ                  int64    `json:"scoreQ"`
	ScoreDecimal            string   `json:"scoreDecimal"`
	RoundingSteps           []Step   `json:"roundingSteps"`
	CumulativeRoundingError Fraction `json:"cumulativeRoundingError"`
	IdealScoreQ             Fraction `json:"idealScoreQExact"`
}

func RoundRatToInt(value *big.Rat) int64 {
	absoluteNumerator := new(big.Int).Abs(value.Num())
	denominator := value.Denom()
	scaledNumerator := new(big.Int).Mul(absoluteNumerator, big.NewInt(2))
	scaledNumerator.Add(scaledNumerator, denominator)
	rounded := new(big.Int).Quo(scaledNumerator, new(big.Int).Mul(denominator, big.NewInt(2)))
	if value.Sign() < 0 {
		rounded.Neg(rounded)
	}
	return rounded.Int64()
}

func roundRatioToQ(numerator, denominator int64) (int64, *big.Rat) {
	exact := new(big.Rat).SetFrac64(numerator, denominator)
	exactQ := new(big.Rat).Mul(exact, big.NewRat(Scale, 1))
	rounded := RoundRatToInt(exactQ)
	localError := new(big.Rat).Sub(big.NewRat(rounded, 1), exactQ)
	return rounded, localError
}

func roundIntegerRat(value *big.Rat) (int64, *big.Rat) {
	rounded := RoundRatToInt(value)
	localError := new(big.Rat).Sub(big.NewRat(rounded, 1), value)
	return rounded, localError
}

func decimalFromQ(value int64) string {
	if value < 0 {
		return "-" + decimalFromQ(-value)
	}
	return fmt.Sprintf("%d.%06d", value/Scale, value%Scale)
}

func Compute(input Input) Result {
	weights := DefaultWeights()
	ref := input.RuleRefCount
	if ref <= 0 {
		ref = 10
	}
	steps := make([]Step, 0, 8)
	cumulative := new(big.Rat)

	ruleNumerator := int64(input.RuleCount)
	if ruleNumerator > ref {
		ruleNumerator = ref
	}
	ruleTermQ, ruleErr := roundRatioToQ(ruleNumerator, ref)
	ruleWeight := new(big.Rat).SetFrac64(weights.RuleCountQ, Scale)
	cumulative.Add(cumulative, new(big.Rat).Mul(ruleWeight, ruleErr))
	steps = append(steps, Step{
		Name: "compiled_rule_count_term", ExactFormula: fmt.Sprintf("min(ruleCount,%d)/%d * %d", ref, ref, Scale),
		RoundedQ: ruleTermQ, RoundingMode: HalfUpLabel, LocalErrorInQUnits: NewFraction(ruleErr), CumulativeErrorInScore: NewFraction(cumulative),
	})

	conflictRatioQ := int64(0)
	var conflictErr *big.Rat
	if input.TotalPairs > 0 {
		conflictRatioQ, conflictErr = roundRatioToQ(int64(input.ConflictPairs), int64(input.TotalPairs))
	} else {
		conflictErr = new(big.Rat)
	}
	conflictWeight := new(big.Rat).SetFrac64(weights.ConflictQ, Scale)
	// conflictFreeQ = Q6 - conflictRatioQ, so its rounding error has the opposite sign.
	cumulative.Add(cumulative, new(big.Rat).Mul(conflictWeight, new(big.Rat).Neg(conflictErr)))
	steps = append(steps, Step{
		Name: "conflict_pair_ratio", ExactFormula: fmt.Sprintf("conflictPairs/max(totalPairs,1) * %d", Scale),
		RoundedQ: conflictRatioQ, RoundingMode: HalfUpLabel, LocalErrorInQUnits: NewFraction(conflictErr), CumulativeErrorInScore: NewFraction(cumulative),
	})
	conflictFreeQ := Scale - conflictRatioQ

	mergeRateQ := int64(0)
	mergeErr := new(big.Rat)
	if input.RawIntervals > 0 {
		mergeRateQ, mergeErr = roundRatioToQ(int64(input.RawIntervals-input.MergedIntervals), int64(input.RawIntervals))
		mergeWeight := new(big.Rat).SetFrac64(weights.MergeQ, Scale)
		cumulative.Add(cumulative, new(big.Rat).Mul(mergeWeight, mergeErr))
	}
	steps = append(steps, Step{
		Name: "port_interval_merge_rate", ExactFormula: "(rawIntervals-mergedIntervals)/rawIntervals * Q6; zero raw intervals defaults to 0",
		RoundedQ: mergeRateQ, RoundingMode: HalfUpLabel, LocalErrorInQUnits: NewFraction(mergeErr), CumulativeErrorInScore: NewFraction(cumulative),
	})

	ruleExactContribution := new(big.Rat).SetFrac64(weights.RuleCountQ*ruleTermQ, Scale)
	ruleContributionQ, contributionErr := roundIntegerRat(ruleExactContribution)
	cumulative.Add(cumulative, contributionErr)
	steps = append(steps, contributionStep("weighted_rule_count", "ruleWeightQ*ruleCountTermQ/Q6", ruleContributionQ, contributionErr, cumulative))

	conflictExactContribution := new(big.Rat).SetFrac64(weights.ConflictQ*conflictFreeQ, Scale)
	conflictContributionQ, contributionErr := roundIntegerRat(conflictExactContribution)
	cumulative.Add(cumulative, contributionErr)
	steps = append(steps, contributionStep("weighted_conflict_free", "conflictWeightQ*(Q6-conflictRatioQ)/Q6", conflictContributionQ, contributionErr, cumulative))

	mergeExactContribution := new(big.Rat).SetFrac64(weights.MergeQ*mergeRateQ, Scale)
	mergeContributionQ, contributionErr := roundIntegerRat(mergeExactContribution)
	cumulative.Add(cumulative, contributionErr)
	steps = append(steps, contributionStep("weighted_port_merge", "mergeWeightQ*mergeRateQ/Q6", mergeContributionQ, contributionErr, cumulative))

	scoreQ := ruleContributionQ + conflictContributionQ + mergeContributionQ

	ideal := idealScore(input, ref, weights)
	idealQ := new(big.Rat).Mul(ideal, big.NewRat(Scale, 1))
	finalError := new(big.Rat).Sub(big.NewRat(scoreQ, 1), idealQ)

	return Result{
		Namespace: input.Namespace, SnapshotVersion: input.SnapshotVersion, Scale: Scale, Weights: weights,
		RuleCount: input.RuleCount, CompiledRuleCountTermQ: ruleTermQ,
		TotalPairs: input.TotalPairs, ConflictPairs: input.ConflictPairs, ConflictRatioQ: conflictRatioQ, ConflictFreeTermQ: conflictFreeQ,
		RawIntervals: input.RawIntervals, MergedIntervals: input.MergedIntervals, MergeRateQ: mergeRateQ,
		RuleContributionQ: ruleContributionQ, ConflictContributionQ: conflictContributionQ, MergeContributionQ: mergeContributionQ,
		ScoreQ: scoreQ, ScoreDecimal: decimalFromQ(scoreQ), RoundingSteps: steps,
		CumulativeRoundingError: NewFraction(finalError), IdealScoreQ: NewFraction(idealQ),
	}
}

func contributionStep(name, formula string, rounded int64, localErr, cumulative *big.Rat) Step {
	return Step{Name: name, ExactFormula: formula, RoundedQ: rounded, RoundingMode: HalfUpLabel, LocalErrorInQUnits: NewFraction(localErr), CumulativeErrorInScore: NewFraction(cumulative)}
}

func idealScore(input Input, ref int64, weights Weights) *big.Rat {
	ruleN := int64(input.RuleCount)
	if ruleN > ref {
		ruleN = ref
	}
	ruleTerm := new(big.Rat).SetFrac64(ruleN, ref)
	conflictRatio := new(big.Rat)
	if input.TotalPairs > 0 {
		conflictRatio.SetFrac64(int64(input.ConflictPairs), int64(input.TotalPairs))
	}
	conflictFree := new(big.Rat).Sub(big.NewRat(1, 1), conflictRatio)
	mergeRate := new(big.Rat)
	if input.RawIntervals > 0 {
		mergeRate.SetFrac64(int64(input.RawIntervals-input.MergedIntervals), int64(input.RawIntervals))
	}
	result := new(big.Rat)
	result.Add(result, new(big.Rat).Mul(new(big.Rat).SetFrac64(weights.RuleCountQ, Scale), ruleTerm))
	result.Add(result, new(big.Rat).Mul(new(big.Rat).SetFrac64(weights.ConflictQ, Scale), conflictFree))
	result.Add(result, new(big.Rat).Mul(new(big.Rat).SetFrac64(weights.MergeQ, Scale), mergeRate))
	return result
}

type ThresholdDecision struct {
	ScoreQ               int64  `json:"scoreQ"`
	ThresholdQ           int64  `json:"thresholdQ"`
	RelativeTolerancePpm int64  `json:"relativeTolerancePpm"`
	ToleranceQ           int64  `json:"toleranceQ"`
	DifferenceQ          int64  `json:"differenceQ"`
	State                string `json:"state"`
	Boundary             string `json:"boundary"`
	Comparison           string `json:"comparison"`
}

func CompareThreshold(scoreQ, thresholdQ, tolerancePpm int64) ThresholdDecision {
	if tolerancePpm < 0 {
		tolerancePpm = 0
	}
	if tolerancePpm > PPM {
		tolerancePpm = PPM
	}
	toleranceExact := new(big.Rat).SetFrac64(thresholdQ*tolerancePpm, PPM)
	toleranceQ, _ := roundIntegerRat(toleranceExact)
	difference := scoreQ - thresholdQ
	decision := ThresholdDecision{
		ScoreQ: scoreQ, ThresholdQ: thresholdQ, RelativeTolerancePpm: tolerancePpm,
		ToleranceQ: toleranceQ, DifferenceQ: difference,
		Comparison: "all comparisons use integer Q6 values or cross-multiplied PPM rationals",
	}
	switch {
	case difference >= 0:
		decision.State = "exceeds_threshold"
		if difference == 0 {
			decision.Boundary = "exact_threshold"
		} else {
			decision.Boundary = "above_threshold"
		}
	case thresholdQ-scoreQ <= toleranceQ:
		decision.State = "within_relative_tolerance"
		deficit := thresholdQ - scoreQ
		if deficit == toleranceQ {
			decision.Boundary = "inside_tolerance_at_boundary"
		} else {
			decision.Boundary = "inside_tolerance"
		}
	default:
		decision.State = "below_threshold"
		deficit := thresholdQ - scoreQ
		if toleranceQ >= 0 && deficit == toleranceQ+1 {
			decision.Boundary = "outside_tolerance_one_q_unit"
		} else {
			decision.Boundary = "outside_tolerance"
		}
	}
	return decision
}

func TolerantTieLess(namespaceA string, scoreAQ int64, namespaceB string, scoreBQ int64, tolerancePpm int64) bool {
	if tolerancePpm < 0 {
		tolerancePpm = 0
	}
	if tolerancePpm > PPM {
		tolerancePpm = PPM
	}
	diff := scoreAQ - scoreBQ
	if diff < 0 {
		diff = -diff
	}
	maxScore := scoreAQ
	if scoreBQ > maxScore {
		maxScore = scoreBQ
	}
	withinTolerance := diff*PPM <= maxScore*tolerancePpm
	if withinTolerance {
		return namespaceA < namespaceB
	}
	return scoreAQ > scoreBQ
}

type RankedNamespace struct {
	Namespace string `json:"namespace"`
	ScoreQ    int64  `json:"scoreQ"`
	TieReason string `json:"tieReason"`
}

func Rank(results []Result, tolerancePpm int64) []RankedNamespace {
	ranked := make([]RankedNamespace, 0, len(results))
	for _, result := range results {
		ranked = append(ranked, RankedNamespace{Namespace: result.Namespace, ScoreQ: result.ScoreQ})
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		a, b := ranked[i], ranked[j]
		diff := a.ScoreQ - b.ScoreQ
		if diff < 0 {
			diff = -diff
		}
		maxScore := a.ScoreQ
		if b.ScoreQ > maxScore {
			maxScore = b.ScoreQ
		}
		if diff == 0 {
			ranked[i].TieReason = "exact_score_tie_namespace_lexicographic"
			ranked[j].TieReason = "exact_score_tie_namespace_lexicographic"
			return a.Namespace < b.Namespace
		}
		if diff*PPM <= maxScore*tolerancePpm {
			ranked[i].TieReason = "within_relative_tolerance_namespace_lexicographic"
			ranked[j].TieReason = "within_relative_tolerance_namespace_lexicographic"
			return a.Namespace < b.Namespace
		}
		ranked[i].TieReason = "score_order"
		ranked[j].TieReason = "score_order"
		return a.ScoreQ > b.ScoreQ
	})
	return ranked
}

func ParseDecimalToQ(value string) (int64, error) {
	r, ok := new(big.Rat).SetString(value)
	if !ok {
		return 0, fmt.Errorf("invalid fixed-point decimal %q", value)
	}
	return RoundRatToInt(new(big.Rat).Mul(r, big.NewRat(Scale, 1))), nil
}

func MustJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf(`{"error":"%s"}`, err.Error())
	}
	return string(data)
}
