package risk

import (
	"fmt"
	"math/big"
	"sort"

	"local/rollout-guard/internal/domain"
)

const (
	ModeFast    = "fast"
	ModeFull    = "full"
	ModeCompare = "compare"

	ActionContinue = "CONTINUE_OBSERVING"
	ActionPass     = "PASS"
	ActionRollback = "ROLLBACK"
	ActionEscalate = "ESCALATE"

	LevelLow     = "LOW"
	LevelMedium  = "MEDIUM"
	LevelHigh    = "HIGH"
	LevelExtreme = "EXTREME"
)

type SnapshotMeta struct {
	AsOfCommitSeq       int64 `json:"as_of_commit_seq"`
	ObservationRevision int64 `json:"observation_revision"`
}

type Window struct {
	LatestObservationMS int64 `json:"latest_observation_ms"`
	StartMS             int64 `json:"start_ms"`
	EndMS               int64 `json:"end_ms"`
	LengthMS            int64 `json:"length_ms"`
}

type Sampling struct {
	Mode                    string  `json:"mode"`
	Method                  string  `json:"method"`
	ReportLimitPerMetric    int64   `json:"report_limit_per_metric,omitempty"`
	TotalReports            int     `json:"total_reports"`
	IncludedReports         int     `json:"included_reports"`
	ExcludedReports         int     `json:"excluded_reports"`
	TotalSamples            int64   `json:"total_samples"`
	IncludedSamples         int64   `json:"included_samples"`
	SampleRatePPM           int64   `json:"sample_rate_ppm"`
	Confidence              string  `json:"confidence"`
	PassAllowed             bool    `json:"pass_allowed"`
	ExcludedObservationSeqs []int64 `json:"excluded_observation_seqs,omitempty"`
}

type Metrics struct {
	ErrorRatePPM           int64 `json:"error_rate_ppm"`
	BaselineErrorRatePPM   int64 `json:"baseline_error_rate_ppm"`
	ErrorSamples           int64 `json:"error_samples"`
	ErrorCount             int64 `json:"error_count"`
	LatencyP90MS           int64 `json:"latency_p90_ms"`
	BaselineLatencyP90MS   int64 `json:"baseline_latency_p90_ms"`
	LatencySamples         int64 `json:"latency_samples"`
	P90NearestRank         int64 `json:"p90_nearest_rank"`
	LatencyDegradationPPM  int64 `json:"latency_degradation_ppm"`
	SuccessRatePPM         int64 `json:"success_rate_ppm"`
	BaselineSuccessRatePPM int64 `json:"baseline_success_rate_ppm"`
	SuccessSamples         int64 `json:"success_samples"`
	SuccessCount           int64 `json:"success_count"`
	SuccessDropPPM         int64 `json:"success_drop_ppm"`
}

type Fraction struct {
	Numerator   int64  `json:"numerator"`
	Denominator int64  `json:"denominator"`
	Decimal     string `json:"decimal"`
}

type RoundingStep struct {
	Step       string `json:"step"`
	Expression string `json:"expression"`
	Method     string `json:"method"`
	Exact      string `json:"exact"`
	Rounded    int64  `json:"rounded"`
	Unit       string `json:"unit"`
	Residual   string `json:"residual"`
}

type Component struct {
	Metric            string   `json:"metric"`
	InputPPM          int64    `json:"input_ppm"`
	ThresholdPPM      int64    `json:"threshold_ppm"`
	ExactFraction     Fraction `json:"exact_fraction"`
	RoundedMicro      int64    `json:"rounded_micro"`
	Capped            bool     `json:"capped"`
	WeightBP          int64    `json:"weight_bp"`
	WeightedNumerator int64    `json:"weighted_numerator"`
}

type BoundaryAudit struct {
	Level               string `json:"level"`
	BoundaryBP          int64  `json:"boundary_bp"`
	TolerancePPM        int64  `json:"tolerance_ppm"`
	ToleranceBP         int64  `json:"tolerance_bp"`
	EffectiveBoundaryBP int64  `json:"effective_boundary_bp"`
	Matched             bool   `json:"matched"`
}

type LevelAudit struct {
	Level         string          `json:"level"`
	TieBreakOrder []string        `json:"tie_break_order"`
	ToleranceRule string          `json:"tolerance_rule"`
	Boundaries    []BoundaryAudit `json:"boundaries"`
}

type Calculation struct {
	Formula                        string `json:"formula"`
	WeightedNumerator              int64  `json:"weighted_numerator"`
	WeightedDenominator            int64  `json:"weighted_denominator"`
	ScoreBP                        int64  `json:"score_bp"`
	ScoreExactDecimal              string `json:"score_exact_decimal"`
	CumulativeRoundingErrorMicroBP string `json:"cumulative_rounding_error_micro_bp"`
	RoundingErrorRational          string `json:"rounding_error_rational"`
	Procedure                      string `json:"procedure"`
}

type Decision struct {
	Action              string `json:"action"`
	Reason              string `json:"reason"`
	RollbackThresholdBP int64  `json:"rollback_threshold_bp"`
	PassThresholdBP     int64  `json:"pass_threshold_bp"`
	ScoreBP             int64  `json:"score_bp"`
	MarginToRollbackBP  int64  `json:"margin_to_rollback_bp"`
	AutoRollback        bool   `json:"auto_rollback"`
	SafetyGuardApplied  bool   `json:"safety_guard_applied"`
}

type Evaluation struct {
	Mode                string         `json:"mode"`
	PolicyVersion       int64          `json:"policy_version"`
	PolicyChecksum      string         `json:"policy_checksum"`
	PolicyGeneratedAtMS int64          `json:"policy_generated_at_ms"`
	AsOfCommitSeq       int64          `json:"as_of_commit_seq"`
	ObservationRevision int64          `json:"observation_revision"`
	Window              Window         `json:"window"`
	Sampling            Sampling       `json:"sampling"`
	Metrics             Metrics        `json:"metrics"`
	Components          []Component    `json:"components"`
	ScoreBP             int64          `json:"score_bp"`
	Level               string         `json:"level"`
	LevelAudit          LevelAudit     `json:"level_audit"`
	Decision            Decision       `json:"decision"`
	RoundingSteps       []RoundingStep `json:"rounding_steps"`
	Calculation         Calculation    `json:"calculation"`
	ObservationSamples  int64          `json:"observation_samples"`
	Status              string         `json:"status,omitempty"`
	Applied             bool           `json:"applied"`
}

type Difference struct {
	Field string `json:"field"`
	Fast  any    `json:"fast"`
	Full  any    `json:"full"`
	Equal bool   `json:"equal"`
}

type Comparison struct {
	PolicyVersion       int64        `json:"policy_version"`
	PolicyChecksum      string       `json:"policy_checksum"`
	AsOfCommitSeq       int64        `json:"as_of_commit_seq"`
	ObservationRevision int64        `json:"observation_revision"`
	Window              Window       `json:"window"`
	Fast                Evaluation   `json:"fast"`
	Full                Evaluation   `json:"full"`
	Differences         []Difference `json:"differences"`
}

type BucketSummary struct {
	BucketStartMS  int64   `json:"bucket_start_ms"`
	MetricType     string  `json:"metric_type"`
	SampleCount    int64   `json:"sample_count"`
	MinCommitSeq   int64   `json:"min_commit_seq"`
	MaxCommitSeq   int64   `json:"max_commit_seq"`
	PolicyVersions []int64 `json:"policy_versions"`
}

type windowedObservation struct {
	Observation   domain.Observation
	BucketStart   int64
	PolicyVersion int64
	CommitSeq     int64
}

func RoundHalfUp(numerator, denominator int64) int64 {
	if denominator <= 0 {
		return 0
	}
	if numerator <= 0 {
		return 0
	}
	return (numerator + denominator/2) / denominator
}

func roundHalfUpSigned(numerator, denominator int64) int64 {
	if denominator <= 0 {
		return 0
	}
	if numerator >= 0 {
		return (numerator + denominator/2) / denominator
	}
	return -((-numerator + denominator/2) / denominator)
}

func decimalFraction(numerator, denominator int64) string {
	if denominator == 0 {
		return "n/a"
	}
	rat := new(big.Rat).SetFrac(big.NewInt(numerator), big.NewInt(denominator))
	return rat.FloatString(6)
}

func fraction(numerator, denominator int64) Fraction {
	return Fraction{Numerator: numerator, Denominator: denominator, Decimal: decimalFraction(numerator, denominator)}
}

func Compute(policy domain.Policy, batch domain.Batch, all []storeObservation, meta SnapshotMeta, mode string) (Evaluation, error) {
	if len(all) == 0 {
		return Evaluation{}, domain.NewBizError("E_INSUFFICIENT_SAMPLES", "批次尚无已提交观测", map[string]any{
			"batch_id": batch.ID, "min_samples": policy.MinSamples,
		})
	}
	latest := int64(0)
	for _, item := range all {
		if item.Observation.TimestampMS > latest {
			latest = item.Observation.TimestampMS
		}
	}
	windowStart := latest - policy.ObservationWindowMS + 1
	windowed := make([]storeObservation, 0, len(all))
	for _, item := range all {
		if item.Observation.TimestampMS >= windowStart && item.Observation.TimestampMS <= latest {
			windowed = append(windowed, item)
		}
	}
	if len(windowed) == 0 {
		return Evaluation{}, domain.NewBizError("E_INSUFFICIENT_SAMPLES", "观察窗口内没有观测", map[string]any{
			"batch_id": batch.ID, "window_start_ms": windowStart, "window_end_ms": latest, "min_samples": policy.MinSamples,
		})
	}

	selected := windowed
	sampling := Sampling{
		Mode:         mode,
		Method:       "full_window",
		TotalReports: len(windowed),
		Confidence:   "exact_window",
		PassAllowed:  true,
	}
	if mode == ModeFast {
		selected, sampling = selectFast(windowed, policy.FastSampleLimit)
		sampling.Mode = ModeFast
		sampling.Confidence = "sampled_recent_per_metric"
		if sampling.ExcludedReports > 0 {
			sampling.Confidence = "limited_recent_slice_pass_disabled"
			sampling.PassAllowed = false
		}
	} else {
		for _, item := range selected {
			sampling.TotalSamples += item.Observation.SampleCount
			sampling.IncludedSamples += item.Observation.SampleCount
		}
		sampling.IncludedReports = len(selected)
		sampling.SampleRatePPM = 1_000_000
	}

	metrics, err := deriveMetrics(policy, batch, selected)
	if err != nil {
		return Evaluation{}, err
	}
	components, roundingSteps, weightedNumerator := scoreComponents(policy, metrics)
	scoreBP := RoundHalfUp(weightedNumerator, 1_000_000)
	if scoreBP > 10000 {
		scoreBP = 10000
	}
	if scoreBP < 0 {
		scoreBP = 0
	}
	level, levelAudit := classify(policy, scoreBP)
	decision := decide(policy, mode, sampling, scoreBP)
	calculation := buildCalculation(policy, metrics, components, weightedNumerator, scoreBP)

	return Evaluation{
		Mode:                mode,
		PolicyVersion:       policy.Version,
		PolicyChecksum:      policy.Checksum,
		PolicyGeneratedAtMS: policy.GeneratedAtMS,
		AsOfCommitSeq:       meta.AsOfCommitSeq,
		ObservationRevision: meta.ObservationRevision,
		Window: Window{
			LatestObservationMS: latest,
			StartMS:             windowStart,
			EndMS:               latest,
			LengthMS:            policy.ObservationWindowMS,
		},
		Sampling:           sampling,
		Metrics:            metrics,
		Components:         components,
		ScoreBP:            scoreBP,
		Level:              level,
		LevelAudit:         levelAudit,
		Decision:           decision,
		RoundingSteps:      roundingSteps,
		Calculation:        calculation,
		ObservationSamples: sampling.IncludedSamples,
		Applied:            false,
	}, nil
}

// StoreObservation is the minimal immutable fact consumed by the risk engine.
type StoreObservation struct {
	Observation   domain.Observation
	BucketStartMS int64
	PolicyVersion int64
	CommitSeq     int64
}

// storeObservation keeps the implementation signature stable while exposing a friendlier alias.
type storeObservation = StoreObservation

func selectFast(all []StoreObservation, limit int64) ([]StoreObservation, Sampling) {
	byMetric := map[string][]StoreObservation{}
	totalSamples := int64(0)
	for _, item := range all {
		byMetric[item.Observation.MetricType] = append(byMetric[item.Observation.MetricType], item)
		totalSamples += item.Observation.SampleCount
	}
	selected := make([]StoreObservation, 0)
	excluded := make([]int64, 0)
	for _, items := range byMetric {
		sort.Slice(items, func(i, j int) bool {
			if items[i].Observation.TimestampMS == items[j].Observation.TimestampMS {
				return items[i].Observation.Seq > items[j].Observation.Seq
			}
			return items[i].Observation.TimestampMS > items[j].Observation.TimestampMS
		})
		keep := int(limit)
		if keep > len(items) {
			keep = len(items)
		}
		selected = append(selected, items[:keep]...)
		for _, item := range items[keep:] {
			excluded = append(excluded, item.Observation.Seq)
		}
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].Observation.TimestampMS == selected[j].Observation.TimestampMS {
			return selected[i].Observation.Seq < selected[j].Observation.Seq
		}
		return selected[i].Observation.TimestampMS < selected[j].Observation.TimestampMS
	})
	sort.Slice(excluded, func(i, j int) bool { return excluded[i] < excluded[j] })
	includedSamples := int64(0)
	for _, item := range selected {
		includedSamples += item.Observation.SampleCount
	}
	rate := int64(1_000_000)
	if totalSamples > 0 {
		rate = RoundHalfUp(includedSamples*1_000_000, totalSamples)
	}
	return selected, Sampling{
		Method:                  "latest_reports_per_metric",
		ReportLimitPerMetric:    limit,
		TotalReports:            len(all),
		IncludedReports:         len(selected),
		ExcludedReports:         len(excluded),
		TotalSamples:            totalSamples,
		IncludedSamples:         includedSamples,
		SampleRatePPM:           rate,
		ExcludedObservationSeqs: excluded,
	}
}

func deriveMetrics(policy domain.Policy, batch domain.Batch, observations []StoreObservation) (Metrics, error) {
	metrics := Metrics{
		BaselineErrorRatePPM:   batch.Baseline.ErrorRatePPM,
		BaselineLatencyP90MS:   batch.Baseline.P90LatencyMS,
		BaselineSuccessRatePPM: batch.Baseline.SuccessRatePPM,
	}
	latencies := make([]int64, 0)
	var errorSamples, errorCount, successSamples, successCount int64
	for _, item := range observations {
		switch item.Observation.MetricType {
		case domain.MetricErrorRate:
			errorSamples += item.Observation.SampleCount
			errorCount += item.Observation.SampleCount - item.Observation.SuccessCount
		case domain.MetricSuccessRate:
			successSamples += item.Observation.SampleCount
			successCount += item.Observation.SuccessCount
		case domain.MetricLatencyMS:
			latencies = append(latencies, item.Observation.LatencySamplesMS...)
		}
	}
	missing := make([]string, 0)
	if errorSamples < policy.MinSamples {
		missing = append(missing, domain.MetricErrorRate)
	}
	if int64(len(latencies)) < policy.MinSamples {
		missing = append(missing, domain.MetricLatencyMS)
	}
	if successSamples < policy.MinSamples {
		missing = append(missing, domain.MetricSuccessRate)
	}
	if len(missing) > 0 {
		return Metrics{}, domain.NewBizError("E_INSUFFICIENT_SAMPLES", "样本数不足，不能以零值冒充风险分", map[string]any{
			"batch_id": batch.ID, "missing_metrics": missing, "min_samples": policy.MinSamples,
			"error_samples": errorSamples, "latency_samples": len(latencies), "success_samples": successSamples,
		})
	}
	metrics.ErrorSamples = errorSamples
	metrics.ErrorCount = errorCount
	metrics.ErrorRatePPM = RoundHalfUp(errorCount*1_000_000, errorSamples)
	metrics.SuccessSamples = successSamples
	metrics.SuccessCount = successCount
	metrics.SuccessRatePPM = RoundHalfUp(successCount*1_000_000, successSamples)
	metrics.LatencySamples = int64(len(latencies))
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	rank := (90*metrics.LatencySamples + 99) / 100
	if rank < 1 {
		rank = 1
	}
	if rank > metrics.LatencySamples {
		rank = metrics.LatencySamples
	}
	metrics.P90NearestRank = rank
	metrics.LatencyP90MS = latencies[rank-1]
	latencyDiff := metrics.LatencyP90MS - metrics.BaselineLatencyP90MS
	if latencyDiff > 0 {
		metrics.LatencyDegradationPPM = RoundHalfUp(latencyDiff*1_000_000, metrics.BaselineLatencyP90MS)
	}
	if metrics.SuccessRatePPM < metrics.BaselineSuccessRatePPM {
		metrics.SuccessDropPPM = metrics.BaselineSuccessRatePPM - metrics.SuccessRatePPM
	}
	return metrics, nil
}

func scoreComponents(policy domain.Policy, metrics Metrics) ([]Component, []RoundingStep, int64) {
	inputs := []struct {
		metric     string
		input      int64
		threshold  int64
		weight     int64
		expression string
	}{
		{"error_rate", metrics.ErrorRatePPM, policy.Thresholds.ErrorRatePPM, policy.WeightsBP.ErrorBP, "candidate_error_ppm / error_threshold_ppm"},
		{"latency_degradation", metrics.LatencyDegradationPPM, policy.Thresholds.LatencyDegradationPPM, policy.WeightsBP.LatencyBP, "max(0, candidate_p90 - baseline_p90) / baseline_p90"},
		{"success_drop", metrics.SuccessDropPPM, policy.Thresholds.SuccessDropPPM, policy.WeightsBP.SuccessBP, "max(0, baseline_success_ppm - candidate_success_ppm) / success_drop_threshold_ppm"},
	}
	components := make([]Component, 0, len(inputs))
	steps := make([]RoundingStep, 0, len(inputs))
	weightedNumerator := int64(0)
	for _, input := range inputs {
		numerator := input.input * 1_000_000
		rounded := RoundHalfUp(numerator, input.threshold)
		capped := rounded > 1_000_000
		if capped {
			rounded = 1_000_000
		}
		residualNumerator := numerator - rounded*input.threshold
		components = append(components, Component{
			Metric:            input.metric,
			InputPPM:          input.input,
			ThresholdPPM:      input.threshold,
			ExactFraction:     fraction(numerator, input.threshold),
			RoundedMicro:      rounded,
			Capped:            capped,
			WeightBP:          input.weight,
			WeightedNumerator: rounded * input.weight,
		})
		weightedNumerator += rounded * input.weight
		steps = append(steps, RoundingStep{
			Step:       "component_" + input.metric,
			Expression: input.expression,
			Method:     "integer_round_half_up_then_cap_at_1000000",
			Exact:      decimalFraction(numerator, input.threshold),
			Rounded:    rounded,
			Unit:       "micro_component",
			Residual:   decimalFraction(residualNumerator, input.threshold),
		})
	}
	scoreBP := RoundHalfUp(weightedNumerator, 1_000_000)
	if scoreBP > 10000 {
		scoreBP = 10000
	}
	steps = append(steps,
		RoundingStep{
			Step:       "metric_error_rate_aggregation",
			Expression: "error_count * 1000000 / error_samples",
			Method:     "integer_round_half_up",
			Exact:      decimalFraction(metrics.ErrorCount*1_000_000, metrics.ErrorSamples),
			Rounded:    metrics.ErrorRatePPM,
			Unit:       "ppm",
			Residual:   decimalFraction(metrics.ErrorCount*1_000_000-metrics.ErrorRatePPM*metrics.ErrorSamples, metrics.ErrorSamples),
		},
		RoundingStep{
			Step:       "metric_success_rate_aggregation",
			Expression: "success_count * 1000000 / success_samples",
			Method:     "integer_round_half_up",
			Exact:      decimalFraction(metrics.SuccessCount*1_000_000, metrics.SuccessSamples),
			Rounded:    metrics.SuccessRatePPM,
			Unit:       "ppm",
			Residual:   decimalFraction(metrics.SuccessCount*1_000_000-metrics.SuccessRatePPM*metrics.SuccessSamples, metrics.SuccessSamples),
		},
		RoundingStep{
			Step:       "metric_latency_degradation",
			Expression: "max(0, candidate_p90 - baseline_p90) * 1000000 / baseline_p90",
			Method:     "integer_round_half_up",
			Exact:      decimalFraction(max64(0, metrics.LatencyP90MS-metrics.BaselineLatencyP90MS)*1_000_000, metrics.BaselineLatencyP90MS),
			Rounded:    metrics.LatencyDegradationPPM,
			Unit:       "ppm",
			Residual:   "declared-input rounding; see exact_fraction per component",
		},
		RoundingStep{
			Step:       "weighted_risk",
			Expression: "sum(component_micro * weight_bp) / 1000000",
			Method:     "integer_round_half_up",
			Exact:      decimalFraction(weightedNumerator, 1_000_000),
			Rounded:    scoreBP,
			Unit:       "risk_bp",
			Residual:   decimalFraction(weightedNumerator-scoreBP*1_000_000, 1_000_000),
		},
	)
	return components, steps, weightedNumerator
}

func buildCalculation(policy domain.Policy, metrics Metrics, components []Component, weightedNumerator, scoreBP int64) Calculation {
	exact := new(big.Rat)
	for _, component := range components {
		ratio := new(big.Rat).SetFrac(big.NewInt(component.InputPPM), big.NewInt(component.ThresholdPPM))
		if ratio.Cmp(big.NewRat(1, 1)) > 0 {
			ratio.SetInt64(1)
		}
		contribution := new(big.Rat).Mul(ratio, new(big.Rat).SetInt64(component.WeightBP))
		exact.Add(exact, contribution)
	}
	exactDecimal := exact.FloatString(6)
	fixed := new(big.Rat).SetFrac(big.NewInt(scoreBP), big.NewInt(1))
	errorRat := new(big.Rat).Sub(exact, fixed)
	errorMicroRat := new(big.Rat).Mul(errorRat, new(big.Rat).SetInt64(1_000_000))
	roundedErrorMicro := roundHalfUpSigned(errorMicroRat.Num().Int64(), errorMicroRat.Denom().Int64())
	return Calculation{
		Formula:                        "component_micro = min(1000000, round_half_up(metric_ppm * 1000000 / threshold_ppm)); risk_bp = round_half_up(sum(component_micro * weight_bp) / 1000000)",
		WeightedNumerator:              weightedNumerator,
		WeightedDenominator:            1_000_000,
		ScoreBP:                        scoreBP,
		ScoreExactDecimal:              exactDecimal,
		CumulativeRoundingErrorMicroBP: fmt.Sprintf("%d", roundedErrorMicro),
		RoundingErrorRational:          errorMicroRat.RatString(),
		Procedure:                      "integer scoring; exact rational used only for audit residual",
	}
}

func classify(policy domain.Policy, scoreBP int64) (string, LevelAudit) {
	type boundary struct {
		level string
		value int64
	}
	boundaries := []boundary{
		{LevelLow, policy.Risk.MediumBP},
		{LevelMedium, policy.Risk.HighBP},
		{LevelHigh, policy.Risk.ExtremeBP},
	}
	audits := make([]BoundaryAudit, 0, len(boundaries))
	level := LevelExtreme
	matched := false
	for _, item := range boundaries {
		tolerance := RoundHalfUp(item.value*policy.Thresholds.LevelTolerancePPM, 1_000_000)
		effective := item.value + tolerance
		hit := false
		if !matched && scoreBP <= effective {
			level = item.level
			matched = true
			hit = true
		}
		audits = append(audits, BoundaryAudit{
			Level: item.level, BoundaryBP: item.value, TolerancePPM: policy.Thresholds.LevelTolerancePPM,
			ToleranceBP: tolerance, EffectiveBoundaryBP: effective, Matched: hit,
		})
	}
	return level, LevelAudit{
		Level: level, TieBreakOrder: []string{LevelLow, LevelMedium, LevelHigh, LevelExtreme},
		ToleranceRule: "relative_tolerance_round_half_up_then_lower_tier", Boundaries: audits,
	}
}

func decide(policy domain.Policy, mode string, sampling Sampling, scoreBP int64) Decision {
	decision := Decision{
		RollbackThresholdBP: policy.Risk.RollbackBP,
		PassThresholdBP:     policy.Risk.PassBP,
		ScoreBP:             scoreBP,
		MarginToRollbackBP:  scoreBP - policy.Risk.RollbackBP,
		AutoRollback:        policy.AutoRollback,
	}
	switch {
	case scoreBP >= policy.Risk.RollbackBP:
		if policy.AutoRollback {
			decision.Action = ActionRollback
			decision.Reason = "风险分达到自动回滚阈值且策略开关开启"
		} else {
			decision.Action = ActionEscalate
			decision.Reason = "风险分达到回滚阈值，但自动回滚关闭，升级人工处置"
		}
	case scoreBP < policy.Risk.PassBP:
		if mode == ModeFast && !sampling.PassAllowed {
			decision.Action = ActionContinue
			decision.Reason = "快速样本低于放行阈值，但仍有未采样观测；守护规则禁止放行"
			decision.SafetyGuardApplied = true
		} else {
			decision.Action = ActionPass
			decision.Reason = "风险分低于放行阈值，且当前采样允许形成完整结论"
		}
	default:
		decision.Action = ActionContinue
		decision.Reason = "风险分处于观察区间，继续观察"
	}
	return decision
}

func Compare(policy domain.Policy, batch domain.Batch, observations []StoreObservation, meta SnapshotMeta) (Comparison, error) {
	fast, err := Compute(policy, batch, observations, meta, ModeFast)
	if err != nil {
		return Comparison{}, err
	}
	full, err := Compute(policy, batch, observations, meta, ModeFull)
	if err != nil {
		return Comparison{}, err
	}
	differences := []Difference{
		{Field: "score_bp", Fast: fast.ScoreBP, Full: full.ScoreBP, Equal: fast.ScoreBP == full.ScoreBP},
		{Field: "risk_level", Fast: fast.Level, Full: full.Level, Equal: fast.Level == full.Level},
		{Field: "decision", Fast: fast.Decision.Action, Full: full.Decision.Action, Equal: fast.Decision.Action == full.Decision.Action},
		{Field: "error_rate_ppm", Fast: fast.Metrics.ErrorRatePPM, Full: full.Metrics.ErrorRatePPM, Equal: fast.Metrics.ErrorRatePPM == full.Metrics.ErrorRatePPM},
		{Field: "latency_p90_ms", Fast: fast.Metrics.LatencyP90MS, Full: full.Metrics.LatencyP90MS, Equal: fast.Metrics.LatencyP90MS == full.Metrics.LatencyP90MS},
		{Field: "success_rate_ppm", Fast: fast.Metrics.SuccessRatePPM, Full: full.Metrics.SuccessRatePPM, Equal: fast.Metrics.SuccessRatePPM == full.Metrics.SuccessRatePPM},
		{Field: "included_samples", Fast: fast.Sampling.IncludedSamples, Full: full.Sampling.IncludedSamples, Equal: fast.Sampling.IncludedSamples == full.Sampling.IncludedSamples},
	}
	return Comparison{
		PolicyVersion: policy.Version, PolicyChecksum: policy.Checksum, AsOfCommitSeq: meta.AsOfCommitSeq,
		ObservationRevision: meta.ObservationRevision, Window: full.Window, Fast: fast, Full: full, Differences: differences,
	}, nil
}

func SummarizeBuckets(observations []StoreObservation, policy domain.Policy) []BucketSummary {
	if len(observations) == 0 {
		return []BucketSummary{}
	}
	type key struct {
		bucket int64
		metric string
	}
	type aggregate struct {
		samples   int64
		minCommit int64
		maxCommit int64
		versions  map[int64]struct{}
	}
	rollup := map[key]*aggregate{}
	for _, item := range observations {
		bucket := item.Observation.TimestampMS - item.Observation.TimestampMS%policy.BucketMS
		k := key{bucket, item.Observation.MetricType}
		entry := rollup[k]
		if entry == nil {
			entry = &aggregate{minCommit: item.CommitSeq, maxCommit: item.CommitSeq, versions: map[int64]struct{}{}}
			rollup[k] = entry
		}
		entry.samples += item.Observation.SampleCount
		if item.CommitSeq < entry.minCommit {
			entry.minCommit = item.CommitSeq
		}
		if item.CommitSeq > entry.maxCommit {
			entry.maxCommit = item.CommitSeq
		}
		entry.versions[item.PolicyVersion] = struct{}{}
	}
	result := make([]BucketSummary, 0, len(rollup))
	for k, entry := range rollup {
		versions := make([]int64, 0, len(entry.versions))
		for version := range entry.versions {
			versions = append(versions, version)
		}
		sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
		result = append(result, BucketSummary{
			BucketStartMS: k.bucket, MetricType: k.metric, SampleCount: entry.samples,
			MinCommitSeq: entry.minCommit, MaxCommitSeq: entry.maxCommit, PolicyVersions: versions,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].BucketStartMS == result[j].BucketStartMS {
			return result[i].MetricType < result[j].MetricType
		}
		return result[i].BucketStartMS < result[j].BucketStartMS
	})
	return result
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
