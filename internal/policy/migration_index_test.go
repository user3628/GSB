package policy

import (
	"encoding/json"
	"os"
	"testing"

	"example.com/replicabudget/internal/apperr"
)

func loadQuotas(t *testing.T) []QuotaGroup {
	t.Helper()
	data, err := os.ReadFile("../../samples/quotas.json")
	if err != nil {
		t.Fatal(err)
	}
	var groups []QuotaGroup
	if err := json.Unmarshal(data, &groups); err != nil {
		t.Fatal(err)
	}
	return groups
}

func TestMigrationAcceptedAndDegraded(t *testing.T) {
	legacy, err := ParseLegacyJSON(readSample(t, "legacy.json"))
	if err != nil {
		t.Fatal(err)
	}
	plan := BuildMigrationPlan(legacy, loadQuotas(t), DefaultTolerance())
	if plan.Decision != "accept" {
		t.Fatalf("expected accept, got %+v", plan.Decisions)
	}
	byID := map[string]NewStrategy{}
	for _, strategy := range plan.Strategies {
		byID[strategy.ServiceID] = strategy
	}
	if byID["svc-a"].WeightPermille != 1000 || byID["svc-a"].TargetConcurrency != 256 {
		t.Fatalf("degrade fill incorrect: %+v", byID["svc-a"])
	}
	for _, decision := range plan.Decisions {
		if decision.Equivalence.ExpectedReplicas != decision.Equivalence.FixedReplicas {
			t.Fatalf("accepted migration must be self-consistent: %+v", decision.Equivalence)
		}
		if len(decision.Degradations) == 0 {
			t.Fatalf("expected weight degradation detail")
		}
	}
	if !plan.WeightedIndexEquivalence.WithinTolerance || plan.WeightedIndexEquivalence.OldIndexMicros != 4_333_333 {
		t.Fatalf("weighted replica check: %+v", plan.WeightedIndexEquivalence)
	}
}

func TestMigrationAmbiguousIncludesNumbers(t *testing.T) {
	legacy, err := ParseLegacyJSON(readSample(t, "legacy_ambiguous.json"))
	if err != nil {
		t.Fatal(err)
	}
	plan := BuildMigrationPlan(legacy, loadQuotas(t), DefaultTolerance())
	if plan.Decision != "manual_confirmation" {
		t.Fatalf("expected manual confirmation: %+v", plan.Decisions)
	}
	if len(plan.NumericMismatches) != 1 {
		t.Fatalf("expected numeric mismatch: %+v", plan.NumericMismatches)
	}
	eq := plan.NumericMismatches[0].Equivalence
	if eq.FixedReplicas != 6 || eq.ExpectedReplicas != 2 || eq.Difference != 4 || eq.WithinTolerance {
		t.Fatalf("unexpected mismatch numbers: %+v", eq)
	}
	if plan.WeightedIndexEquivalence.WithinTolerance {
		t.Fatalf("ambiguous weighted replica check should fail: %+v", plan.WeightedIndexEquivalence)
	}
	be := apperr.As(plan.AmbiguousError())
	if be.Code != apperr.MigrationAmbiguous || be.Details["numeric_mismatches"] == nil || be.Details["weighted_index_equivalence"] == nil {
		t.Fatalf("ambiguous error lost details: %+v", be)
	}
}

func TestWeightedIndexExactAndCompensated(t *testing.T) {
	services := []DerivedService{
		{Strategy: NewStrategy{ServiceID: "svc-a", WeightPermille: 1000}, DesiredReplicas: 4},
		{Strategy: NewStrategy{ServiceID: "svc-b", WeightPermille: 1500}, DesiredReplicas: 6},
		{Strategy: NewStrategy{ServiceID: "svc-c", WeightPermille: 500}, DesiredReplicas: 3},
	}
	index, err := ComputeWeightedIndex(services)
	if err != nil {
		t.Fatal(err)
	}
	if index.IndexMicros != 4_833_333 || index.IndexDecimal != "4.833333" {
		t.Fatalf("unexpected index: %+v", index)
	}
	if len(index.Steps) != 3 || index.Steps[0].ServiceID != "svc-a" {
		t.Fatalf("steps not stable/lexicographic: %+v", index.Steps)
	}
	if index.Steps[0].Rounding != "floor" || index.Steps[0].RoundedMicros != 1_333_333 {
		t.Fatalf("unexpected first rounding step: %+v", index.Steps[0])
	}
	if index.CumulativeRoundingErrorNumerator != "-1000" {
		t.Fatalf("cumulative error=%s", index.CumulativeRoundingErrorNumerator)
	}
	if index.CompensatedAdjustmentMicros != 0 {
		t.Fatalf("adjustment=%d", index.CompensatedAdjustmentMicros)
	}
}

func TestThresholdUsesToleranceAndStableOrder(t *testing.T) {
	index := WeightedIndex{IndexMicros: 4_833_333, OrderedServiceIDs: []string{"svc-a", "svc-b", "svc-c"}}
	decision := CompareThreshold(index, 4_835_000, 1)
	if decision.Exceeded || !decision.WithinTolerance || decision.Decision != "within_tolerance" {
		t.Fatalf("unexpected tolerance decision: %+v", decision)
	}
	if len(decision.TieBreakServices) != 3 || decision.TieBreakServices[0] != "svc-a" {
		t.Fatalf("tie break order: %+v", decision.TieBreakServices)
	}
	above := CompareThreshold(index, 4_700_000, 1)
	if !above.Exceeded {
		t.Fatalf("expected above threshold: %+v", above)
	}
}
