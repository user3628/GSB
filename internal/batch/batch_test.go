package batch

import (
	"testing"

	"driftengine/internal/errs"
	"driftengine/internal/spec"
)

func TestDependencyOrderAndBudgetSplit(t *testing.T) {
	objects := []spec.Object{
		{ID: "a", Desired: spec.Fields{}, Actual: spec.Fields{}, ResourceBudget: spec.Budget{Units: 3}},
		{ID: "b", Desired: spec.Fields{}, Actual: spec.Fields{}, ResourceBudget: spec.Budget{Units: 3}, Dependencies: []string{"a"}},
		{ID: "c", Desired: spec.Fields{}, Actual: spec.Fields{}, ResourceBudget: spec.Budget{Units: 1}},
	}
	policy := spec.Policy{MaxBatchSize: 2, MaxBatchBudgetUnits: 5, DependencyOrderRule: "dependencies_before_dependents"}
	plan, err := Build(1, objects, policy)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Batches) != 2 {
		t.Fatalf("batches=%d", len(plan.Batches))
	}
	byID := map[string]*Batch{}
	for _, b := range plan.Batches {
		for _, id := range b.ObjectIDs {
			byID[id] = b
		}
	}
	if byID["a"].BatchNumber >= byID["b"].BatchNumber {
		t.Fatal("dependency batch must be earlier")
	}
	if byID["a"] != byID["c"] {
		t.Fatalf("a and c should share one layer-zero batch: a=%s c=%s", byID["a"].BatchID, byID["c"].BatchID)
	}
	if byID["a"].Budget.OccupiedUnits != 4 || byID["c"].Budget.OccupiedUnits != 4 {
		t.Fatalf("unexpected shared budget occupancy: a=%+v c=%+v", byID["a"].Budget, byID["c"].Budget)
	}
	if byID["b"].Budget.OccupiedUnits != 3 {
		t.Fatalf("unexpected b budget occupancy: %+v", byID["b"].Budget)
	}
}

func TestCycleReturnsIDsAndOutsideLayers(t *testing.T) {
	objects := []spec.Object{
		{ID: "outside", Desired: spec.Fields{}, Actual: spec.Fields{}, ResourceBudget: spec.Budget{Units: 1}},
		{ID: "cyc-a", Desired: spec.Fields{}, Actual: spec.Fields{}, Dependencies: []string{"cyc-b"}, ResourceBudget: spec.Budget{Units: 1}},
		{ID: "cyc-b", Desired: spec.Fields{}, Actual: spec.Fields{}, Dependencies: []string{"cyc-a"}, ResourceBudget: spec.Budget{Units: 1}},
	}
	policy := spec.Policy{MaxBatchSize: 10, MaxBatchBudgetUnits: 10, DependencyOrderRule: "dependencies_before_dependents"}
	plan, err := Build(1, objects, policy)
	if errs.Code(err) != errs.DependencyCycle {
		t.Fatalf("err=%v", err)
	}
	if plan == nil || len(plan.CycleIDs) != 2 || len(plan.Batches) != 1 || plan.Batches[0].ObjectIDs[0] != "outside" {
		t.Fatalf("unexpected plan: %+v", plan)
	}
}
