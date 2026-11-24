package abeval

import "fmt"

const (
	ReasonInsufficientSample = "OBSERVE_INSUFFICIENT_SAMPLE"
	ReasonThresholdNotMet    = "OBSERVE_THRESHOLD_NOT_MET"
	ReasonRolloutMet         = "ROLLOUT_CRITERIA_MET"
	ReasonRollbackCTR        = "ROLLBACK_CTR_DECLINE"
	ReasonRollbackLatency    = "ROLLBACK_LATENCY_REGRESSION"
	ReasonRollbackMultiple   = "ROLLBACK_MULTIPLE_SIGNALS"
)

func EvaluateStage(metrics StageMetrics, cfg Config) Verdict {
	ctr := metrics.CTR
	latency := metrics.Latency
	verdict := Verdict{
		Decision:   DecisionObserve,
		ReasonCode: ReasonThresholdNotMet,
		Criteria:   make([]Criterion, 0, 8),
		Negated:    make([]string, 0),
	}

	sampleActual := fmt.Sprintf("%s=%d,%s=%d",
		ctr.ControlGroup, statsExposures(metrics.CumulativeGroups, ctr.ControlGroup),
		ctr.TreatmentGroup, statsExposures(metrics.CumulativeGroups, ctr.TreatmentGroup))
	sampleCriterion := Criterion{
		Code:      "min_exposures_per_group",
		Scope:     "gate",
		Passed:    ctr.MeetsMinSample,
		Actual:    sampleActual,
		Threshold: fmt.Sprintf(">=%d each", ctr.RequiredPerGroup),
	}
	if !sampleCriterion.Passed {
		sampleCriterion.Reason = "at least one group is below the stage minimum sample count"
		verdict.Criteria = append(verdict.Criteria, sampleCriterion)
		verdict.Negated = append(verdict.Negated, sampleCriterion.Code+": "+sampleCriterion.Reason)
		verdict.ReasonCode = ReasonInsufficientSample
		verdict.Summary = fmt.Sprintf("stage %d remains in observation: sample gate not met (%s; required %d/group)", metrics.StageIndex, sampleActual, ctr.RequiredPerGroup)
		return verdict
	}
	verdict.Criteria = append(verdict.Criteria, sampleCriterion)

	liftPassed := ctr.AbsoluteLift >= cfg.CTR.MinLiftAbs
	liftCriterion := Criterion{
		Code:      "ctr_absolute_lift_min",
		Scope:     "rollout",
		Passed:    liftPassed,
		Actual:    formatFloat(ctr.AbsoluteLift),
		Threshold: fmt.Sprintf(">= %.6f", cfg.CTR.MinLiftAbs),
	}
	if !liftPassed {
		liftCriterion.Reason = "CTR lift is below the rollout threshold"
		verdict.Negated = append(verdict.Negated, liftCriterion.Code+": "+liftCriterion.Reason)
	}
	verdict.Criteria = append(verdict.Criteria, liftCriterion)

	ciPassed := ctr.CILower > 0
	ciCriterion := Criterion{
		Code:      "ctr_confidence_interval_excludes_zero_below",
		Scope:     "rollout",
		Passed:    ciPassed,
		Actual:    fmt.Sprintf("[%s,%s]", formatFloat(ctr.CILower), formatFloat(ctr.CIUpper)),
		Threshold: fmt.Sprintf("lower > 0 at %.3f confidence", ctr.CILevel),
	}
	if !ciPassed {
		ciCriterion.Reason = "CTR difference interval still includes non-positive lift"
		verdict.Negated = append(verdict.Negated, ciCriterion.Code+": "+ciCriterion.Reason)
	}
	verdict.Criteria = append(verdict.Criteria, ciCriterion)

	latencyAvailable := Criterion{
		Code:   "latency_quantile_available",
		Scope:  "latency",
		Passed: latency.Available,
		Actual: fmt.Sprintf("p%.0f control=%dus treatment=%dus", latency.QuantileP*100, latency.ControlUS, latency.TreatmentUS),
	}
	if !latency.Available {
		latencyAvailable.Reason = "latency quantile is unavailable because a group has no samples"
		verdict.Criteria = append(verdict.Criteria, latencyAvailable)
		verdict.Negated = append(verdict.Negated, latencyAvailable.Code+": "+latencyAvailable.Reason)
		verdict.Summary = fmt.Sprintf("stage %d remains in observation: latency quantile unavailable", metrics.StageIndex)
		return verdict
	}
	verdict.Criteria = append(verdict.Criteria, latencyAvailable)

	absolutePassed := latency.TreatmentUS <= latency.MaxQuantileUS
	absoluteCriterion := Criterion{
		Code:      "latency_quantile_absolute_limit",
		Scope:     "rollout",
		Passed:    absolutePassed,
		Actual:    formatInt64(latency.TreatmentUS),
		Threshold: fmt.Sprintf("<=%d us", latency.MaxQuantileUS),
	}
	if !absolutePassed {
		absoluteCriterion.Reason = "treatment latency quantile exceeds absolute limit"
		verdict.Negated = append(verdict.Negated, absoluteCriterion.Code+": "+absoluteCriterion.Reason)
	}
	verdict.Criteria = append(verdict.Criteria, absoluteCriterion)

	regressionPassed := latency.DeltaUS <= latency.MaxRegressionUS
	regressionCriterion := Criterion{
		Code:      "latency_quantile_regression_limit",
		Scope:     "rollout",
		Passed:    regressionPassed,
		Actual:    formatInt64(latency.DeltaUS),
		Threshold: fmt.Sprintf("delta <=%d us", latency.MaxRegressionUS),
	}
	if !regressionPassed {
		regressionCriterion.Reason = "treatment latency regression exceeds allowed bound"
		verdict.Negated = append(verdict.Negated, regressionCriterion.Code+": "+regressionCriterion.Reason)
	}
	verdict.Criteria = append(verdict.Criteria, regressionCriterion)

	ctrDropPassed := ctr.AbsoluteLift <= -cfg.CTR.RollbackDropAbs
	ctrDropCriterion := Criterion{
		Code:      "ctr_absolute_drop_rollback",
		Scope:     "rollback",
		Passed:    ctrDropPassed,
		Actual:    formatFloat(ctr.AbsoluteLift),
		Threshold: fmt.Sprintf("<= -%.6f", cfg.CTR.RollbackDropAbs),
	}
	if ctrDropPassed {
		ctrDropCriterion.Reason = "CTR decline exceeds rollback threshold"
	}
	verdict.Criteria = append(verdict.Criteria, ctrDropCriterion)

	ctrRollbackCIPassed := ctr.CIUpper < 0
	ctrRollbackCI := Criterion{
		Code:      "ctr_confidence_interval_excludes_zero_above",
		Scope:     "rollback",
		Passed:    ctrRollbackCIPassed,
		Actual:    fmt.Sprintf("[%s,%s]", formatFloat(ctr.CILower), formatFloat(ctr.CIUpper)),
		Threshold: "upper < 0",
	}
	if ctrRollbackCIPassed {
		ctrRollbackCI.Reason = "CTR decline interval excludes zero"
	}
	verdict.Criteria = append(verdict.Criteria, ctrRollbackCI)

	ctrRollback := ctrDropPassed && ctrRollbackCIPassed
	latencyRollback := !absolutePassed || !regressionPassed

	switch {
	case ctrRollback && latencyRollback:
		verdict.Decision = DecisionRollback
		verdict.ReasonCode = ReasonRollbackMultiple
		verdict.Summary = fmt.Sprintf("stage %d recommends rollback: CTR decline and latency regression both cross thresholds", metrics.StageIndex)
	case ctrRollback:
		verdict.Decision = DecisionRollback
		verdict.ReasonCode = ReasonRollbackCTR
		verdict.Summary = fmt.Sprintf("stage %d recommends rollback: CTR lift %s with upper CI %s", metrics.StageIndex, formatFloat(ctr.AbsoluteLift), formatFloat(ctr.CIUpper))
	case latencyRollback:
		verdict.Decision = DecisionRollback
		verdict.ReasonCode = ReasonRollbackLatency
		verdict.Summary = fmt.Sprintf("stage %d recommends rollback: p%.0f latency delta %dus and treatment value %dus", metrics.StageIndex, latency.QuantileP*100, latency.DeltaUS, latency.TreatmentUS)
	case liftPassed && ciPassed && absolutePassed && regressionPassed:
		verdict.Decision = DecisionRollout
		verdict.ReasonCode = ReasonRolloutMet
		verdict.Summary = fmt.Sprintf("stage %d recommends rollout: CTR lift %s CI [%s,%s], latency within bounds", metrics.StageIndex, formatFloat(ctr.AbsoluteLift), formatFloat(ctr.CILower), formatFloat(ctr.CIUpper))
	default:
		verdict.Summary = fmt.Sprintf("stage %d continues observation: thresholds are not decisively met", metrics.StageIndex)
	}
	return verdict
}

func statsExposures(groups []GroupStats, group string) int {
	if stats, ok := FindGroupStats(groups, group); ok {
		return stats.Exposures
	}
	return 0
}

func EvaluateDataset(input Dataset) (*Report, error) {
	dataset := input
	if err := ValidateDataset(&dataset); err != nil {
		return nil, err
	}
	revision, err := HashDataset(dataset)
	if err != nil {
		return nil, err
	}
	batchID := dataset.BatchID
	if batchID == "" {
		batchID = dataset.Config.ExperimentID
	}

	report := &Report{
		SchemaVersion: SchemaVersion,
		BatchID:       batchID,
		ExperimentID:  dataset.Config.ExperimentID,
		Revision:      revision,
		Status:        StatusCompleted,
		Stages:        make([]StageReport, 0, len(dataset.Config.Stages)),
	}

	for _, stageCfg := range dataset.Config.Stages {
		metrics, err := BuildStageMetrics(dataset.Config, dataset.Events, stageCfg.Index)
		if err != nil {
			return nil, err
		}
		verdict := EvaluateStage(metrics, dataset.Config)
		report.Stages = append(report.Stages, StageReport{
			Stage:   stageCfg,
			Metrics: metrics,
			Verdict: verdict,
		})
	}

	final := report.Stages[len(report.Stages)-1].Verdict
	report.Decision = final.Decision
	report.DecisionCode = final.ReasonCode
	report.Summary = final.Summary
	finalCopy := final
	report.FinalVerdict = &finalCopy
	return report, nil
}

const SchemaVersion = "abeval.report/v1"
