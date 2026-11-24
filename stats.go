package abeval

import (
	"fmt"
	"math"
	"sort"
)

var defaultQuantileProbs = []float64{0.5, 0.95, 0.99}

func QuantileNearestRank(sortedValues []int64, probability float64) (int64, bool) {
	if len(sortedValues) == 0 {
		return 0, false
	}
	if probability <= 0 {
		return sortedValues[0], true
	}
	if probability >= 1 {
		return sortedValues[len(sortedValues)-1], true
	}
	rank := int(math.Ceil(probability * float64(len(sortedValues))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sortedValues) {
		rank = len(sortedValues)
	}
	return sortedValues[rank-1], true
}

func quantileProbsFor(cfg Config) []float64 {
	probs := append([]float64(nil), defaultQuantileProbs...)
	if cfg.Latency.Quantile > 0 {
		probs = append(probs, cfg.Latency.Quantile)
	}
	sort.Float64s(probs)
	deduped := probs[:0]
	var last float64 = -1
	for _, probability := range probs {
		if probability != last {
			deduped = append(deduped, probability)
			last = probability
		}
	}
	return deduped
}

func ComputeGroupStats(events []Event, group string, probs []float64) GroupStats {
	stats := GroupStats{Group: group}
	latencies := make([]int64, 0, len(events))
	for _, event := range events {
		if event.Group != group {
			continue
		}
		stats.Exposures++
		stats.Clicks += event.Clicked
		latencies = append(latencies, event.LatencyUS)
	}
	if stats.Exposures > 0 {
		stats.CTR = roundFloat(float64(stats.Clicks)/float64(stats.Exposures), 9)
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	stats.Quantiles = make([]Quantile, 0, len(probs))
	for _, probability := range probs {
		value, available := QuantileNearestRank(latencies, probability)
		stats.Quantiles = append(stats.Quantiles, Quantile{
			P:         probability,
			ValueUS:   value,
			Available: available,
		})
	}
	return stats
}

func FindGroupStats(groups []GroupStats, group string) (GroupStats, bool) {
	for _, stats := range groups {
		if stats.Group == group {
			return stats, true
		}
	}
	return GroupStats{}, false
}

func findQuantile(stats GroupStats, probability float64) (Quantile, bool) {
	for _, quantile := range stats.Quantiles {
		if quantile.P == probability {
			return quantile, quantile.Available
		}
	}
	return Quantile{}, false
}

func BuildStageMetrics(cfg Config, events []Event, stageIndex int) (StageMetrics, error) {
	if stageIndex < 1 || stageIndex > len(cfg.Stages) {
		return StageMetrics{}, NewError(CodeInvalidConfig, "stage_index is outside configured stages").
			WithStage("build_stage_metrics").
			WithField("stage_index").
			WithDetail("stage_index", stageIndex)
	}

	probs := quantileProbsFor(cfg)
	stageEvents := make([]Event, 0)
	cumulativeEvents := make([]Event, 0)
	for _, event := range events {
		eventStage, err := StageIndexAt(event.TimestampMS, cfg)
		if err != nil {
			return StageMetrics{}, err
		}
		if eventStage == stageIndex {
			stageEvents = append(stageEvents, event)
		}
		if eventStage <= stageIndex {
			cumulativeEvents = append(cumulativeEvents, event)
		}
	}

	metrics := StageMetrics{
		StageIndex:           stageIndex,
		StageEventCount:      len(stageEvents),
		CumulativeEventCount: len(cumulativeEvents),
		StageGroups:          make([]GroupStats, 0, len(cfg.Groups)),
		CumulativeGroups:     make([]GroupStats, 0, len(cfg.Groups)),
	}
	for _, group := range cfg.Groups {
		metrics.StageGroups = append(metrics.StageGroups, ComputeGroupStats(stageEvents, group, probs))
		metrics.CumulativeGroups = append(metrics.CumulativeGroups, ComputeGroupStats(cumulativeEvents, group, probs))
	}

	control, _ := FindGroupStats(metrics.CumulativeGroups, cfg.ControlGroup)
	treatment, _ := FindGroupStats(metrics.CumulativeGroups, cfg.TreatmentGroup)
	required := cfg.Stages[stageIndex-1].MinExposuresPerGroup
	if required == 0 {
		required = cfg.MinExposuresPerGroup
	}

	comparison := CTRComparison{
		ControlGroup:     cfg.ControlGroup,
		TreatmentGroup:   cfg.TreatmentGroup,
		ControlCTR:       control.CTR,
		TreatmentCTR:     treatment.CTR,
		RequiredPerGroup: required,
		MeetsMinSample:   control.Exposures >= required && treatment.Exposures >= required,
	}
	comparison.AbsoluteLift = roundFloat(treatment.CTR-control.CTR, 9)
	if control.CTR > 0 {
		comparison.RelativeLift = roundFloat((treatment.CTR-control.CTR)/control.CTR, 9)
	}

	rawControlCTR := 0.0
	if control.Exposures > 0 {
		rawControlCTR = float64(control.Clicks) / float64(control.Exposures)
	}
	rawTreatmentCTR := 0.0
	if treatment.Exposures > 0 {
		rawTreatmentCTR = float64(treatment.Clicks) / float64(treatment.Exposures)
	}
	if control.Exposures > 0 && treatment.Exposures > 0 {
		variance := rawControlCTR*(1-rawControlCTR)/float64(control.Exposures) +
			rawTreatmentCTR*(1-rawTreatmentCTR)/float64(treatment.Exposures)
		comparison.StandardError = roundFloat(math.Sqrt(variance), 9)
		margin := cfg.CTR.ZScore * math.Sqrt(variance)
		rawLift := rawTreatmentCTR - rawControlCTR
		comparison.CILevel = roundFloat(2*normalCDF(cfg.CTR.ZScore)-1, 6)
		comparison.CILower = roundFloat(rawLift-margin, 9)
		comparison.CIUpper = roundFloat(rawLift+margin, 9)
	}
	metrics.CTR = comparison

	latencyP := cfg.Latency.Quantile
	controlQuantile, controlOK := findQuantile(control, latencyP)
	treatmentQuantile, treatmentOK := findQuantile(treatment, latencyP)
	latency := LatencyComparison{
		QuantileP:       latencyP,
		MaxQuantileUS:   cfg.Latency.MaxQuantileUS,
		MaxRegressionUS: cfg.Latency.MaxRegressionUS,
		Available:       controlOK && treatmentOK,
	}
	if latency.Available {
		latency.ControlUS = controlQuantile.ValueUS
		latency.TreatmentUS = treatmentQuantile.ValueUS
		latency.DeltaUS = treatmentQuantile.ValueUS - controlQuantile.ValueUS
		latency.AbsoluteSatisfied = treatmentQuantile.ValueUS <= cfg.Latency.MaxQuantileUS
		latency.RegressionSatisfied = latency.DeltaUS <= cfg.Latency.MaxRegressionUS
	}
	metrics.Latency = latency
	return metrics, nil
}

func normalCDF(z float64) float64 {
	return 0.5 * (1 + math.Erf(z/math.Sqrt2))
}

func formatFloat(value float64) string {
	return fmt.Sprintf("%.6f", value)
}

func formatInt64(value int64) string {
	return fmt.Sprintf("%d", value)
}
