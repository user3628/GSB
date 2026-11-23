package domain

import (
	"releasegate/numeric"
)

const (
	ThresholdMinObservation = "samples.min_observation"
	ThresholdMaxCIWidth     = "confidence.max_ci_half_width"
	ThresholdAbsSuccessDrop = "success.absolute_drop"
	ThresholdRelSuccessDrop = "success.relative_drop"
	ThresholdLatencyDrop    = "latency.drop"
)

type GateEvaluation struct {
	Outcome            string
	Code               string
	TriggeredThreshold string
	Measured           GateMeasured
}

// EvaluateGate applies thresholds in a fixed, stable order. The first
// insufficient-evidence condition wins over a hard failure so a wide interval
// is never reported as proof of a regression. All threshold comparisons use a
// fixed-point relative tolerance: equality inside the tolerance passes.
func EvaluateGate(stage StageDefinition, metrics StageMetrics, estimate DifferenceEstimate, contract ErrorContract) GateEvaluation {
	measured := GateMeasured{
		AbsDropUpperPPM:      estimate.AbsDropUpperPPM,
		AbsSuccessDropPPM:    estimate.AbsSuccessDropPPM,
		CanarySamples:        metrics.Canary.Samples,
		CIHalfWidthPPM:       estimate.CIHalfWidthPPM,
		ErrorBoundPPM:        estimate.ErrorBoundPPM,
		LatencyDropUS:        estimate.LatencyDropUS,
		RelativeDropPPM:      estimate.RelativeSuccessDropPPM,
		RelativeDropUpperPPM: estimate.RelativeDropUpperPPM,
		RelativeTolerancePPM: stage.Gate.RelativeTolerancePPM,
		StableSamples:        metrics.Stable.Samples,
	}

	if stage.MinObservationSamples > 0 {
		minObserved := minInt64(metrics.Canary.Samples, metrics.Stable.Samples)
		if minObserved < stage.MinObservationSamples {
			measured.MeasuredValue = minObserved
			measured.ThresholdValue = stage.MinObservationSamples
			return GateEvaluation{
				Outcome: OutcomeObserveMore, Code: CodeObserveMore,
				TriggeredThreshold: ThresholdMinObservation, Measured: measured,
			}
		}
	}

	ciLimit, ciTolerance := tolerantLimit(stage.Gate.MaxCIHalfWidthPPM, stage.Gate.RelativeTolerancePPM)
	if estimate.CIHalfWidthPPM > ciLimit {
		measured.MeasuredValue = estimate.CIHalfWidthPPM
		measured.ThresholdValue = stage.Gate.MaxCIHalfWidthPPM
		measured.ToleranceApplied = ciTolerance
		return GateEvaluation{
			Outcome: OutcomeObserveMore, Code: CodeObserveMore,
			TriggeredThreshold: ThresholdMaxCIWidth, Measured: measured,
		}
	}

	absLimit, absTolerance := tolerantLimit(stage.Gate.MaxAbsSuccessDropPPM, stage.Gate.RelativeTolerancePPM)
	if estimate.AbsDropUpperPPM > absLimit {
		measured.MeasuredValue = estimate.AbsDropUpperPPM
		measured.ThresholdValue = stage.Gate.MaxAbsSuccessDropPPM
		measured.ToleranceApplied = absTolerance
		return GateEvaluation{
			Outcome: OutcomeGateFailed, Code: CodeGateFailed,
			TriggeredThreshold: ThresholdAbsSuccessDrop, Measured: measured,
		}
	}

	relLimit, relTolerance := tolerantLimit(stage.Gate.MaxRelativeSuccessDropPPM, stage.Gate.RelativeTolerancePPM)
	if estimate.RelativeDropUpperPPM > relLimit {
		measured.MeasuredValue = estimate.RelativeDropUpperPPM
		measured.ThresholdValue = stage.Gate.MaxRelativeSuccessDropPPM
		measured.ToleranceApplied = relTolerance
		return GateEvaluation{
			Outcome: OutcomeGateFailed, Code: CodeGateFailed,
			TriggeredThreshold: ThresholdRelSuccessDrop, Measured: measured,
		}
	}

	latencyLimit, latencyTolerance := tolerantLimit(stage.Gate.MaxLatencyDeltaUS, stage.Gate.RelativeTolerancePPM)
	if estimate.LatencyDropUS > latencyLimit {
		measured.MeasuredValue = estimate.LatencyDropUS
		measured.ThresholdValue = stage.Gate.MaxLatencyDeltaUS
		measured.ToleranceApplied = latencyTolerance
		return GateEvaluation{
			Outcome: OutcomeGateFailed, Code: CodeGateFailed,
			TriggeredThreshold: ThresholdLatencyDrop, Measured: measured,
		}
	}

	return GateEvaluation{Outcome: OutcomePromote, Code: "", Measured: measured}
}

func tolerantLimit(threshold, tolerancePPM int64) (int64, int64) {
	limit, applied, err := numeric.ThresholdWithTolerance(threshold, tolerancePPM)
	if err != nil {
		return threshold, 0
	}
	return limit, applied
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
