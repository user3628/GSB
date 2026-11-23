package fixed

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"sort"

	"recoveryengine/internal/model"
)

const (
	Scale                   = int64(1_000_000)
	RoundingModeHalfEven    = "half_even"
	DecisionMet             = "met"
	DecisionWithinTolerance = "met_within_tolerance"
	DecisionBelow           = "below"
)

type Step struct {
	ComponentID               string `json:"component_id"`
	ProbeState                string `json:"probe_state"`
	ProbeScoreMicro           int64  `json:"probe_score_micro"`
	CPUHeadroomMicro          int64  `json:"cpu_headroom_micro"`
	MemoryHeadroomMicro       int64  `json:"memory_headroom_micro"`
	ResourceHeadroomMicro     int64  `json:"resource_headroom_micro"`
	HealthWeightPermille      int64  `json:"health_weight_permille"`
	ResourceWeightPermille    int64  `json:"resource_weight_permille"`
	HealthNumerator           int64  `json:"health_numerator"`
	ResourceNumerator         int64  `json:"resource_numerator"`
	LocalExactNumerator       int64  `json:"local_exact_numerator"`
	LocalScoreMicro           int64  `json:"local_score_micro"`
	LocalRemainder            int64  `json:"local_rounding_remainder"`
	LocalRoundingMode         string `json:"local_rounding_mode"`
	ConsistencyWeightPermille int64  `json:"consistency_weight_permille"`
	AggregateTerm             int64  `json:"aggregate_term"`
}

type Result struct {
	ScoreMicro                   int64    `json:"score_micro"`
	ScorePermille                int64    `json:"score_permille"`
	ScorePermilleRemainder       int64    `json:"score_permille_rounding_remainder"`
	ThresholdPermille            int64    `json:"threshold_permille"`
	ThresholdMicro               int64    `json:"threshold_micro"`
	RelativeTolerancePPM         int64    `json:"relative_tolerance_ppm"`
	ToleranceMicro               int64    `json:"tolerance_micro"`
	Decision                     string   `json:"decision"`
	ThresholdMet                 bool     `json:"threshold_met"`
	DeltaToThresholdMicro        int64    `json:"delta_to_threshold_micro"`
	TieBreak                     string   `json:"tie_break"`
	TieComponents                []string `json:"tie_components"`
	Steps                        []Step   `json:"steps"`
	ExactNumerator               int64    `json:"exact_numerator"`
	Denominator                  int64    `json:"denominator"`
	CompensatedSum               int64    `json:"compensated_sum"`
	Compensation                 int64    `json:"compensation"`
	AggregateRoundingMode        string   `json:"aggregate_rounding_mode"`
	AggregateRemainderNumerator  int64    `json:"aggregate_rounding_remainder_numerator"`
	CumulativeRoundingErrorMicro int64    `json:"cumulative_rounding_error_micro"`
	RoundingErrorPPB             int64    `json:"rounding_error_ppb"`
	Digest                       string   `json:"digest"`
}

type compensatedSum struct {
	sum          int64
	compensation int64
}

func (s *compensatedSum) Add(value int64) {
	temporary := s.sum + value
	if abs(s.sum) >= abs(value) {
		s.compensation += (s.sum - temporary) + value
	} else {
		s.compensation += (value - temporary) + s.sum
	}
	s.sum = temporary
}

func (s *compensatedSum) Total() int64 {
	return s.sum + s.compensation
}

func Compute(snapshot model.Snapshot) Result {
	components := snapshot.SortedComponents()
	steps := make([]Step, 0, len(components))
	var aggregate compensatedSum
	var weightSum int64
	for _, component := range components {
		observation := snapshot.Observations[component.ID]
		probeScore := probeScoreMicro(observation.State)
		cpuHeadroom := headroomMicro(component.Resources.CPUMillicores, component.Capacity.CPUMillicores)
		memoryHeadroom := headroomMicro(component.Resources.MemoryBytes, component.Capacity.MemoryBytes)
		resourceHeadroom := cpuHeadroom
		if memoryHeadroom < resourceHeadroom {
			resourceHeadroom = memoryHeadroom
		}
		healthNumerator := probeScore * component.HealthWeightPermille
		resourceNumerator := resourceHeadroom * component.ResourceWeightPermille
		localExact := healthNumerator + resourceNumerator
		localScore, localRemainder := divRoundHalfEven(localExact, 1000)
		aggregateTerm := localExact * component.ConsistencyWeightPermille
		aggregate.Add(aggregateTerm)
		weightSum += component.ConsistencyWeightPermille
		steps = append(steps, Step{
			ComponentID:               component.ID,
			ProbeState:                observation.State,
			ProbeScoreMicro:           probeScore,
			CPUHeadroomMicro:          cpuHeadroom,
			MemoryHeadroomMicro:       memoryHeadroom,
			ResourceHeadroomMicro:     resourceHeadroom,
			HealthWeightPermille:      component.HealthWeightPermille,
			ResourceWeightPermille:    component.ResourceWeightPermille,
			HealthNumerator:           healthNumerator,
			ResourceNumerator:         resourceNumerator,
			LocalExactNumerator:       localExact,
			LocalScoreMicro:           localScore,
			LocalRemainder:            localRemainder,
			LocalRoundingMode:         RoundingModeHalfEven,
			ConsistencyWeightPermille: component.ConsistencyWeightPermille,
			AggregateTerm:             aggregateTerm,
		})
	}

	exactNumerator := aggregate.Total()
	denominator := int64(1000) * weightSum
	scoreMicro := int64(0)
	remainder := int64(0)
	if denominator > 0 {
		scoreMicro, remainder = divRoundHalfEven(exactNumerator, denominator)
	}
	if scoreMicro < 0 {
		scoreMicro = 0
	}
	if scoreMicro > Scale {
		scoreMicro = Scale
	}
	scorePermille, permilleRemainder := divRoundHalfEven(scoreMicro, 1000)
	thresholdMicro := snapshot.ConsistencyConfig.ThresholdPermille * 1000
	toleranceMicro := int64(0)
	if thresholdMicro > 0 {
		toleranceMicro, _ = divRoundHalfEven(thresholdMicro*snapshot.ConsistencyConfig.RelativeTolerancePPM, 1_000_000)
	}
	decision := DecisionBelow
	met := false
	switch {
	case scoreMicro >= thresholdMicro:
		decision = DecisionMet
		met = true
	case scoreMicro+toleranceMicro >= thresholdMicro:
		decision = DecisionWithinTolerance
		met = true
	}
	tieComponents := make([]string, 0)
	for _, step := range steps {
		delta := step.LocalScoreMicro - thresholdMicro
		if delta < 0 {
			delta = -delta
		}
		if delta <= toleranceMicro {
			tieComponents = append(tieComponents, step.ComponentID)
		}
	}
	sort.Strings(tieComponents)
	result := Result{
		ScoreMicro:                   scoreMicro,
		ScorePermille:                scorePermille,
		ScorePermilleRemainder:       permilleRemainder,
		ThresholdPermille:            snapshot.ConsistencyConfig.ThresholdPermille,
		ThresholdMicro:               thresholdMicro,
		RelativeTolerancePPM:         snapshot.ConsistencyConfig.RelativeTolerancePPM,
		ToleranceMicro:               toleranceMicro,
		Decision:                     decision,
		ThresholdMet:                 met,
		DeltaToThresholdMicro:        scoreMicro - thresholdMicro,
		TieBreak:                     "component_id_lexicographic_ascending",
		TieComponents:                tieComponents,
		Steps:                        steps,
		ExactNumerator:               exactNumerator,
		Denominator:                  denominator,
		CompensatedSum:               aggregate.sum,
		Compensation:                 aggregate.compensation,
		AggregateRoundingMode:        RoundingModeHalfEven,
		AggregateRemainderNumerator:  remainder,
		CumulativeRoundingErrorMicro: roundedMicro(remainder, denominator),
		RoundingErrorPPB:             roundedPPB(remainder, denominator),
	}
	result.Digest = resultDigest(result)
	return result
}

func probeScoreMicro(state string) int64 {
	switch state {
	case "healthy":
		return 1_000_000
	case "degraded":
		return 500_000
	case "unhealthy":
		return 0
	default:
		return 250_000
	}
}

func headroomMicro(required, capacity int64) int64 {
	if required < 0 || capacity < 0 || required > capacity {
		return 0
	}
	if capacity == 0 {
		return Scale
	}
	numerator := new(big.Int).Mul(big.NewInt(capacity-required), big.NewInt(Scale))
	quotient := new(big.Int).Quo(numerator, big.NewInt(capacity))
	if !quotient.IsInt64() {
		return Scale
	}
	value := quotient.Int64()
	if value < 0 {
		return 0
	}
	if value > Scale {
		return Scale
	}
	return value
}

func divRoundHalfEven(numerator, denominator int64) (int64, int64) {
	if denominator == 0 {
		return 0, numerator
	}
	negative := numerator < 0
	if negative {
		numerator = -numerator
	}
	quotient := numerator / denominator
	remainder := numerator % denominator
	twice := remainder * 2
	if twice > denominator || (twice == denominator && quotient%2 != 0) {
		quotient++
	}
	if negative {
		return -quotient, -remainder
	}
	return quotient, remainder
}

func roundedMicro(remainder, denominator int64) int64 {
	if denominator == 0 {
		return 0
	}
	return remainder / denominator
}

func roundedPPB(remainder, denominator int64) int64 {
	if denominator == 0 {
		return 0
	}
	value, _ := divRoundHalfEven(remainder*1_000_000_000, denominator)
	return value
}

func resultDigest(result Result) string {
	result.Digest = ""
	payload, _ := json.Marshal(result)
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func abs(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
