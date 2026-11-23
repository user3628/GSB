package service

import (
	"math"
	"math/big"
	"sort"

	"releasegate/domain"
	"releasegate/numeric"
)

const stableMergeOrder = "latency_us,stage,timestamp_ms,instance_id,group,batch_id,seq"

func ComputeSnapshot(def domain.ReleaseDefinition, releaseID string, version int64, stageNumber int, createdAtMS int64, observations []domain.Observation) (domain.StatsSnapshot, error) {
	stageDef, ok := def.Stage(stageNumber)
	if !ok {
		return domain.StatsSnapshot{}, domain.WithContext(domain.NewError(domain.CodeReleaseInvalid, "statistics requested for unknown stage"), "stage", stageNumber)
	}
	zPPM, err := numeric.ZScorePPM(def.ConfidenceLevelPPM)
	if err != nil {
		return domain.StatsSnapshot{}, err
	}
	stageMetrics, err := computeMetrics(def, stageDef, stageNumber, []int{stageNumber}, "stage", observations)
	if err != nil {
		return domain.StatsSnapshot{}, err
	}
	stageEstimate, stageContract, err := computeEstimateAndContract(def, releaseID, version, stageDef, stageMetrics, zPPM, "stage")
	if err != nil {
		return domain.StatsSnapshot{}, err
	}

	var cumulativeStages []int
	for _, sd := range def.Stages {
		if sd.Number <= stageNumber {
			cumulativeStages = append(cumulativeStages, sd.Number)
		}
	}
	sort.Ints(cumulativeStages)
	cumulativeMetrics, err := computeMetrics(def, stageDef, stageNumber, cumulativeStages, "cumulative", observations)
	if err != nil {
		return domain.StatsSnapshot{}, err
	}
	cumulativeEstimate, cumulativeContract, err := computeEstimateAndContract(def, releaseID, version, stageDef, cumulativeMetrics, zPPM, "cumulative")
	if err != nil {
		return domain.StatsSnapshot{}, err
	}
	return domain.StatsSnapshot{
		ReleaseID:               releaseID,
		Version:                 version,
		Stage:                   stageNumber,
		CreatedAtMS:             createdAtMS,
		QuantileMethod:          def.QuantileMethod,
		StageMetrics:            stageMetrics,
		CumulativeMetrics:       cumulativeMetrics,
		StageEstimate:           stageEstimate,
		CumulativeEstimate:      cumulativeEstimate,
		StageErrorContract:      stageContract,
		CumulativeErrorContract: cumulativeContract,
	}, nil
}

func computeMetrics(def domain.ReleaseDefinition, stageDef domain.StageDefinition, stageNumber int, included []int, scope string, observations []domain.Observation) (domain.StageMetrics, error) {
	includedSet := make(map[int]struct{}, len(included))
	for _, n := range included {
		includedSet[n] = struct{}{}
	}
	counters := map[string]*numeric.CompensatedCount{
		domain.GroupCanary: {},
		domain.GroupStable: {},
	}
	successes := map[string]int64{domain.GroupCanary: 0, domain.GroupStable: 0}
	latencies := map[string][]domain.Observation{domain.GroupCanary: {}, domain.GroupStable: {}}
	for _, o := range observations {
		if _, ok := includedSet[o.Stage]; !ok {
			continue
		}
		c := counters[o.Group]
		if c == nil {
			continue
		}
		if err := c.Add(1); err != nil {
			return domain.StageMetrics{}, domain.NewError(domain.CodeReleaseInvalid, "sample count overflow")
		}
		if o.Success {
			if successes[o.Group] == math.MaxInt64 {
				return domain.StageMetrics{}, domain.NewError(domain.CodeReleaseInvalid, "success count overflow")
			}
			successes[o.Group]++
		}
		latencies[o.Group] = append(latencies[o.Group], o)
	}

	canary, err := buildGroupMetrics(def, stageDef, domain.GroupCanary, counters[domain.GroupCanary], successes[domain.GroupCanary], latencies[domain.GroupCanary])
	if err != nil {
		return domain.StageMetrics{}, err
	}
	stable, err := buildGroupMetrics(def, stageDef, domain.GroupStable, counters[domain.GroupStable], successes[domain.GroupStable], latencies[domain.GroupStable])
	if err != nil {
		return domain.StageMetrics{}, err
	}
	return domain.StageMetrics{
		Scope:                   scope,
		Stage:                   stageNumber,
		StagesIncluded:          append([]int(nil), included...),
		TargetTrafficPerMille:   stageDef.TargetTrafficPerMille,
		MinObservationSamples:   stageDef.MinObservationSamples,
		LatencyQuantilePerMille: stageDef.LatencyQuantilePerMille,
		Canary:                  canary,
		Stable:                  stable,
		CountCompensation:       counters[domain.GroupCanary].Compensation + counters[domain.GroupStable].Compensation,
		CountMethod:             "exact_integer_counts_with_neumaier_compensation_slot",
		MergedLatencySamples:    int64(len(latencies[domain.GroupCanary]) + len(latencies[domain.GroupStable])),
		QuantileMergeOrder:      stableMergeOrder,
	}, nil
}

func buildGroupMetrics(def domain.ReleaseDefinition, stageDef domain.StageDefinition, group string, count *numeric.CompensatedCount, successes int64, observations []domain.Observation) (domain.GroupMetrics, error) {
	if count.Sum < 2 {
		return domain.GroupMetrics{}, domain.WithContext(domain.ClassifyInsufficient("at least two samples per group are required for a confidence interval"), "group", group)
	}
	if successes > count.Sum {
		return domain.GroupMetrics{}, domain.ClassifyInsufficient("success count exceeds sample count")
	}
	sort.Slice(observations, func(i, j int) bool {
		return observationLess(observations[i], observations[j])
	})
	values := make([]int64, len(observations))
	for i, o := range observations {
		values[i] = o.LatencyUS
	}
	if _, err := domain.CheckedLatencySum(values); err != nil {
		return domain.GroupMetrics{}, domain.NewError(domain.CodeReleaseInvalid, "cumulative latency sum overflows int64")
	}
	rate, rateStep, err := numeric.SuccessRatePPM(successes, count.Sum)
	if err != nil {
		return domain.GroupMetrics{}, domain.ClassifyInsufficient("success rate unavailable")
	}
	quantile, quantileStep, err := numeric.Quantile(values, stageDef.LatencyQuantilePerMille, def.QuantileMethod)
	if err != nil {
		return domain.GroupMetrics{}, domain.ClassifyInsufficient("requested latency quantile unavailable")
	}
	rateAudit := numeric.NewAuditBuilder()
	rateAudit.Add(rateStep)
	quantileAudit := numeric.NewAuditBuilder()
	quantileAudit.Add(quantileStep)
	return domain.GroupMetrics{
		Samples:                 count.Sum,
		Successes:               successes,
		Failures:                count.Sum - successes,
		LatencySamples:          int64(len(values)),
		SuccessRatePPM:          rate,
		SuccessRate:             numeric.FormatPPM(rate),
		SuccessRateRoundingStep: rateStep,
		SuccessRateAudit:        rateAudit.Build(),
		LatencyQuantileMethod:   def.QuantileMethod,
		LatencyQuantileUS:       quantile,
		QuantileRoundingStep:    quantileStep,
		QuantileAudit:           quantileAudit.Build(),
		CountCompensation:       count.Compensation,
	}, nil
}

func observationLess(a, b domain.Observation) bool {
	if a.LatencyUS != b.LatencyUS {
		return a.LatencyUS < b.LatencyUS
	}
	if a.Stage != b.Stage {
		return a.Stage < b.Stage
	}
	if a.TimestampMS != b.TimestampMS {
		return a.TimestampMS < b.TimestampMS
	}
	if a.InstanceID != b.InstanceID {
		return a.InstanceID < b.InstanceID
	}
	if a.Group != b.Group {
		return a.Group < b.Group
	}
	if a.BatchID != b.BatchID {
		return a.BatchID < b.BatchID
	}
	return a.Seq < b.Seq
}

func computeEstimateAndContract(def domain.ReleaseDefinition, releaseID string, version int64, stageDef domain.StageDefinition, metrics domain.StageMetrics, zPPM int64, scope string) (domain.DifferenceEstimate, domain.ErrorContract, error) {
	c := metrics.Canary
	s := metrics.Stable
	absDiff, absStep, err := numeric.DifferencePPM(c.Successes, c.Samples, s.Successes, s.Samples)
	if err != nil {
		return domain.DifferenceEstimate{}, domain.ErrorContract{}, domain.ClassifyInsufficient("absolute difference unavailable")
	}
	relDiff, relStep, err := numeric.RelativeDifferencePPM(c.Successes, c.Samples, s.Successes, s.Samples)
	if err != nil {
		return domain.DifferenceEstimate{}, domain.ErrorContract{}, domain.ClassifyInsufficient("relative difference unavailable; stable successes must be non-zero")
	}
	ci, err := numeric.WaldHalfWidthPPM(c.Successes, c.Samples, s.Successes, s.Samples, zPPM)
	if err != nil {
		return domain.DifferenceEstimate{}, domain.ErrorContract{}, domain.ClassifyInsufficient("confidence interval unavailable")
	}
	errorBound, err := numeric.RuntimeErrorBoundPPM(c.Samples, s.Samples, zPPM)
	if err != nil {
		return domain.DifferenceEstimate{}, domain.ErrorContract{}, domain.ClassifyInsufficient("runtime error bound unavailable")
	}
	absDrop := int64(0)
	if absDiff < 0 {
		absDrop = -absDiff
	}
	relDrop := int64(0)
	if relDiff < 0 {
		relDrop = -relDiff
	}
	relativeCI, err := relativePPM(ci, s.SuccessRatePPM)
	if err != nil {
		return domain.DifferenceEstimate{}, domain.ErrorContract{}, domain.ClassifyInsufficient("relative confidence interval unavailable")
	}
	latencyDrop := int64(0)
	if c.LatencyQuantileUS > s.LatencyQuantileUS {
		latencyDrop = c.LatencyQuantileUS - s.LatencyQuantileUS
	}
	roundingAudit := numeric.NewAuditBuilder()
	roundingAudit.Add(absStep)
	roundingAudit.Add(relStep)
	accumulated := roundingAudit.Build()
	estimate := domain.DifferenceEstimate{
		Scope:                       scope,
		Stage:                       metrics.Stage,
		CanarySamples:               c.Samples,
		StableSamples:               s.Samples,
		CanarySuccesses:             c.Successes,
		StableSuccesses:             s.Successes,
		CanaryRatePPM:               c.SuccessRatePPM,
		StableRatePPM:               s.SuccessRatePPM,
		AbsoluteDifferencePPM:       absDiff,
		RelativeDifferencePPM:       relDiff,
		AbsSuccessDropPPM:           absDrop,
		RelativeSuccessDropPPM:      relDrop,
		CIHalfWidthPPM:              ci,
		AbsDropUpperPPM:             absDrop + ci,
		RelativeDropUpperPPM:        relDrop + relativeCI,
		LatencyDropUS:               latencyDrop,
		CanaryLatencyQuantileUS:     c.LatencyQuantileUS,
		StableLatencyQuantileUS:     s.LatencyQuantileUS,
		ErrorBoundPPM:               errorBound,
		RelativeTolerancePPM:        stageDef.Gate.RelativeTolerancePPM,
		RoundingSteps:               []numeric.RoundingStep{absStep, relStep},
		AccumulatedRoundingErrorPPM: accumulated.AccumulatedRoundingErrorScaled,
	}
	contract := domain.ErrorContract{
		ReleaseID:             releaseID,
		StatsVersion:          version,
		Scope:                 scope,
		Stage:                 metrics.Stage,
		QuantileMethod:        def.QuantileMethod,
		ConfidenceLevelPPM:    def.ConfidenceLevelPPM,
		ConfidenceZPPM:        zPPM,
		Scale:                 numeric.PPMScaled,
		DecimalPlaces:         numeric.DecimalPlaces,
		RoundingMode:          numeric.RoundingMode,
		CanonicalPath:         numeric.CanonicalPath,
		SampleSizes:           map[string]int64{domain.GroupCanary: c.Samples, domain.GroupStable: s.Samples},
		RuntimeErrorBoundPPM:  errorBound,
		RuntimeCIHalfWidthPPM: ci,
		Formulas: map[string]string{
			"success_rate":              "round_half_even(successes / samples * 1_000_000)",
			"absolute_difference":       "round_half_even((canary_successes/canary_n - stable_successes/stable_n) * 1_000_000)",
			"relative_difference":       "round_half_even((canary_successes/canary_n - stable_successes/stable_n) / (stable_successes/stable_n) * 1_000_000)",
			"confidence_half_width":     "round_half_even(z_ppm * sqrt(p_canary(1-p_canary)/n_canary + p_stable(1-p_stable)/n_stable))",
			"runtime_error_bound":       "round_half_even(z_ppm * sqrt(0.25/n_canary + 0.25/n_stable))",
			"linear_quantile":           "round_half_even(lower + (upper-lower)*(q_per_mille*(n-1) mod 1000)/1000)",
			"nearest_rank_quantile":     "sorted[ceil(q_per_mille*n/1000)-1]",
			"cumulative_quantile_merge": "merge raw latency samples by " + stableMergeOrder + ", independent of stage submission order",
			"threshold_comparison":      "measurement > round_half_even(threshold * tolerance_ppm / 1_000_000) + threshold",
		},
		GateThresholds: stageDef.Gate,
		Audit:          combinedAudit(c, s, absStep, relStep),
	}
	return estimate, contract, nil
}

func combinedAudit(c, s domain.GroupMetrics, extra ...numeric.RoundingStep) numeric.Audit {
	builder := numeric.NewAuditBuilder()
	builder.Add(c.SuccessRateRoundingStep)
	builder.Add(c.QuantileRoundingStep)
	builder.Add(s.SuccessRateRoundingStep)
	builder.Add(s.QuantileRoundingStep)
	for _, step := range extra {
		builder.Add(step)
	}
	return builder.Build()
}

func relativePPM(ciPPM, stableRatePPM int64) (int64, error) {
	if stableRatePPM <= 0 {
		return 0, domain.ClassifyInsufficient("stable success rate is zero")
	}
	scaled := new(big.Int).Mul(big.NewInt(ciPPM), big.NewInt(numeric.PPMScaled))
	rr, err := numeric.RoundHalfEvenRatio(scaled, big.NewInt(stableRatePPM), 1)
	if err != nil {
		return 0, err
	}
	return rr.Output, nil
}
