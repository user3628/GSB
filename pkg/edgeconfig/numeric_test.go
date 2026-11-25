package edgeconfig

import "testing"

func TestFixedPointAllMatchedScoresOne(t *testing.T) {
	nodes := []Node{
		{ID: "z", Region: "b"},
		{ID: "a", Region: "a"},
		{ID: "m", Region: "a"},
	}
	weights := map[string]int64{"a": 3, "m": 2, "z": 1}
	policy := DefaultPolicy().Numeric
	policy.NodeWeights = weights
	matched := map[string]bool{"a": true, "m": true, "z": true}

	score := EvaluateConsistency("global", 1, "", nodes, matched, policy)
	if score.ScoreUnits != score.Scale {
		t.Fatalf("all matched score=%s units=%d, want %d", score.Score, score.ScoreUnits, score.Scale)
	}
	if score.CumulativeRoundingError.Numerator != 0 {
		t.Fatalf("cumulative error numerator=%d, want zero", score.CumulativeRoundingError.Numerator)
	}
	if score.NodeOrder[0] != "a/a" || score.NodeOrder[1] != "a/m" || score.NodeOrder[2] != "b/z" {
		t.Fatalf("unexpected stable order: %v", score.NodeOrder)
	}
}

func TestCompareThresholdRelativeToleranceAndTieBreak(t *testing.T) {
	decision, applied, boundary := CompareThreshold(990_001, 990_000, 10, []string{"a", "z"}, map[string]bool{"a": true})
	if !applied || decision != "TIE_BROKEN_PASS" || boundary != "a" {
		t.Fatalf("tie = %s applied=%v boundary=%q", decision, applied, boundary)
	}

	decision, applied, _ = CompareThreshold(900_000, 990_000, 1_000, nil, nil)
	if applied || decision != "FAIL" {
		t.Fatalf("clear miss decision=%s applied=%v", decision, applied)
	}

	units, err := ParseFixedDecimal("0.990000", 1_000_000)
	if err != nil || units != 990_000 {
		t.Fatalf("ParseFixedDecimal = %d, %v", units, err)
	}
}
