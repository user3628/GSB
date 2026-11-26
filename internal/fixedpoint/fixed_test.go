package fixedpoint_test

import (
	"testing"

	"netpolcompiler/internal/fixedpoint"
)

func TestThresholdToleranceBoundaryIsDistinguishable(t *testing.T) {
	threshold := int64(1_000_000)
	tolerancePpm := int64(100_000)
	inside := fixedpoint.CompareThreshold(900_000, threshold, tolerancePpm)
	outside := fixedpoint.CompareThreshold(899_999, threshold, tolerancePpm)

	if inside.State != "within_relative_tolerance" || inside.Boundary != "inside_tolerance_at_boundary" {
		t.Fatalf("expected exact tolerance boundary inside, got %+v", inside)
	}
	if outside.State != "below_threshold" || outside.Boundary != "outside_tolerance_one_q_unit" {
		t.Fatalf("expected one Q6 unit outside boundary, got %+v", outside)
	}
}

func TestTolerantTieUsesNamespaceLexicographicOrder(t *testing.T) {
	if !fixedpoint.TolerantTieLess("alpha", 900_000, "beta", 905_000, 10_000) {
		t.Fatal("scores inside 1% relative tolerance should tie-break alpha before beta")
	}
	if fixedpoint.TolerantTieLess("beta", 905_000, "alpha", 900_000, 10_000) {
		t.Fatal("lexicographic tie should not place beta before alpha")
	}
	if fixedpoint.TolerantTieLess("low", 800_000, "high", 950_000, 10_000) {
		t.Fatal("scores outside tolerance should use score order")
	}
}

func TestConsistencyOutputsCumulativeError(t *testing.T) {
	result := fixedpoint.Compute(fixedpoint.Input{
		Namespace: "alpha", RuleCount: 4, TotalPairs: 6, ConflictPairs: 1,
		RawIntervals: 5, MergedIntervals: 2, SnapshotVersion: 1,
	})
	if result.ScoreQ <= 0 || result.ScoreQ > fixedpoint.Scale {
		t.Fatalf("score out of Q6 range: %d", result.ScoreQ)
	}
	if len(result.RoundingSteps) == 0 {
		t.Fatal("expected rounding steps")
	}
	if result.CumulativeRoundingError.Numerator == "" || result.CumulativeRoundingError.Denominator == "" {
		t.Fatal("missing cumulative error fraction")
	}
}
