package metrics

import (
	"encoding/json"
	"math/big"
	"testing"
)

func bigint(s string) *big.Int {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic(s)
	}
	return n
}

func fixture() *State {
	state := &State{
		VolumeID: "v", BlockCount: 3, BlockSize: 100, OverheadBytes: 10, LastSeq: 4,
		Live: map[int64]BlockRef{
			0: {BlockNo: 0, Version: 2, Digest: "b"},
			1: {BlockNo: 1, Version: 1, Digest: "a"},
			2: {BlockNo: 2, Version: 1, Digest: "c"},
		},
		Snapshots: map[string]Snapshot{
			"full": {ID: "full", Kind: "full", Weight: 1, Ordinal: 1},
			"inc":  {ID: "inc", Kind: "incremental", ParentID: "full", Weight: 3, Ordinal: 2},
		},
		Refs: map[string]map[int64]int64{
			"full": {0: 1, 1: 1, 2: 1},
			"inc":  {0: 2},
		},
		Versions: map[VersionKey]string{
			{BlockNo: 0, Version: 1}: "a", {BlockNo: 0, Version: 2}: "b",
			{BlockNo: 1, Version: 1}: "a", {BlockNo: 2, Version: 1}: "c",
		},
	}
	return state
}

func TestAnalyzeExactFixture(t *testing.T) {
	state := fixture()
	inc, err := Analyze(state, "inc")
	if err != nil {
		t.Fatal(err)
	}
	if inc.Metrics.CapacityUsage.Ratio.Decimal != "1.000000" {
		t.Fatalf("capacity ratio=%s", inc.Metrics.CapacityUsage.Ratio.Decimal)
	}
	if inc.Metrics.RollbackMargin.Ratio.Decimal != "1.000000" {
		t.Fatalf("inc margin=%s", inc.Metrics.RollbackMargin.Ratio.Decimal)
	}
	if inc.Metrics.WeightedConsistency.Ratio.Decimal != "0.916667" {
		t.Fatalf("weighted=%s", inc.Metrics.WeightedConsistency.Ratio.Decimal)
	}
	full, err := Analyze(state, "full")
	if err != nil {
		t.Fatal(err)
	}
	if full.Metrics.RollbackMargin.Ratio.Decimal != "0.666667" {
		t.Fatalf("full margin=%s", full.Metrics.RollbackMargin.Ratio.Decimal)
	}
	if full.Metrics.RollbackCost.WritebackBlocks != 1 || full.Metrics.RollbackCost.CostBytes != "110" {
		t.Fatalf("cost=%+v", full.Metrics.RollbackCost)
	}
}

func TestAnalyzeIsStableAcrossMapIteration(t *testing.T) {
	state := fixture()
	want, err := Analyze(state, "inc")
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, _ := json.Marshal(want.Metrics)
	for i := 0; i < 50; i++ {
		got, err := Analyze(state, "inc")
		if err != nil {
			t.Fatal(err)
		}
		gotJSON, _ := json.Marshal(got.Metrics)
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("iteration %d changed metrics\n%s\n%s", i, wantJSON, gotJSON)
		}
	}
}

func TestDecisionToleranceBoundaries(t *testing.T) {
	analysis, err := Analyze(fixture(), "full")
	if err != nil {
		t.Fatal(err)
	}
	// 0.666667 margin, floor 0.25. This case accepts comfortably.
	decision, err := Decide(analysis, DecisionRequest{
		MarginFloorUnits: bigint("250000"), CostBudgetBytes: bigint("110"),
		RelativeTolerance: bigint("4000"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Accepted || decision.CostAboveBudget {
		t.Fatalf("unexpected decision %+v", decision)
	}
	// Cost exactly at the widened upper bound is accepted.
	analysis.Metrics.RollbackCost.CostBytes = "1104"
	decision, err = Decide(analysis, DecisionRequest{
		MarginFloorUnits: bigint("250000"), CostBudgetBytes: bigint("1100"),
		RelativeTolerance: bigint("4000"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Accepted {
		t.Fatalf("boundary should be accepted: %+v", decision)
	}
	analysis.Metrics.RollbackCost.CostBytes = "1106"
	decision, _ = Decide(analysis, DecisionRequest{
		MarginFloorUnits: bigint("250000"), CostBudgetBytes: bigint("1100"),
		RelativeTolerance: bigint("4000"),
	})
	if decision.Accepted || !decision.CostAboveBudget {
		t.Fatalf("outside tolerance should reject: %+v", decision)
	}
}

func TestCandidateTieBreakLexicalAndInputOrderIndependent(t *testing.T) {
	base, err := Analyze(fixture(), "inc")
	if err != nil {
		t.Fatal(err)
	}
	a := base
	a.Snapshot.ID = "snap-z"
	b := base
	b.Snapshot.ID = "snap-a"
	req := DecisionRequest{MarginFloorUnits: bigint("0"), CostBudgetBytes: bigint("1000000"), RelativeTolerance: bigint("0")}
	one, selectedOne, err := EvaluateCandidates([]Analysis{a, b}, req)
	if err != nil {
		t.Fatal(err)
	}
	two, selectedTwo, err := EvaluateCandidates([]Analysis{b, a}, req)
	if err != nil {
		t.Fatal(err)
	}
	if selectedOne == nil || selectedTwo == nil || selectedOne.SnapshotID != "snap-a" || selectedTwo.SnapshotID != "snap-a" {
		t.Fatalf("tie-break failed: %v %v", selectedOne, selectedTwo)
	}
	if one[0].SnapshotID != "snap-a" || two[0].SnapshotID != "snap-a" {
		t.Fatalf("evaluation order not stable: %v %v", one[0].SnapshotID, two[0].SnapshotID)
	}
}
