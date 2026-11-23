package domain

import "testing"

func TestEvaluateGateStableThresholdOrder(t *testing.T) {
	stage := StageDefinition{Number: 1, MinObservationSamples: 100, Gate: GateThresholds{
		MaxAbsSuccessDropPPM: 60000, MaxRelativeSuccessDropPPM: 100000,
		MaxLatencyDeltaUS: 2000, MaxCIHalfWidthPPM: 80000, RelativeTolerancePPM: 1000,
	}}
	metrics := StageMetrics{Canary: GroupMetrics{Samples: 200}, Stable: GroupMetrics{Samples: 200}}
	estimate := DifferenceEstimate{CIHalfWidthPPM: 38357, AbsDropUpperPPM: 240281, RelativeDropUpperPPM: 252928, LatencyDropUS: 433}
	got := EvaluateGate(stage, metrics, estimate, ErrorContract{})
	if got.Code != CodeGateFailed || got.TriggeredThreshold != ThresholdAbsSuccessDrop {
		t.Fatalf("gate evaluation = %+v", got)
	}
}

func TestEvaluateGateToleranceIsInclusive(t *testing.T) {
	stage := StageDefinition{Number: 1, Gate: GateThresholds{MaxAbsSuccessDropPPM: 60000, MaxCIHalfWidthPPM: 1, RelativeTolerancePPM: 1000}}
	metrics := StageMetrics{Canary: GroupMetrics{Samples: 200}, Stable: GroupMetrics{Samples: 200}}
	estimate := DifferenceEstimate{CIHalfWidthPPM: 1, AbsDropUpperPPM: 60060}
	got := EvaluateGate(stage, metrics, estimate, ErrorContract{})
	if got.Code != "" {
		t.Fatalf("measurement at tolerance limit must pass: %+v", got)
	}
}

func TestCanTransition(t *testing.T) {
	if !CanTransition(StatePrepared, StateCanary) || !CanTransition(StatePromoting, StateObserving) {
		t.Fatal("expected legal transition rejected")
	}
	if CanTransition(StateRolledBack, StateCanary) || CanTransition(StatePrepared, StatePromoting) {
		t.Fatal("expected illegal transition accepted")
	}
}
