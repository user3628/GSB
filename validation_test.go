package convergence

import (
	"math"
	"testing"
)

func TestNormalizeSnapshotRejectsAggregateBudgetOverflow(t *testing.T) {
	snapshot := SampleSnapshotV1()
	snapshot.Objects = []Object{
		{
			ClusterID: "cn-east", ID: "budget-a", Kind: "ConfigMap", Namespace: "default",
			Expected: FieldSet{}, Actual: FieldSet{},
			Budget: ResourceBudget{MemoryBytes: math.MaxInt64},
		},
		{
			ClusterID: "cn-east", ID: "budget-b", Kind: "ConfigMap", Namespace: "default",
			Expected: FieldSet{}, Actual: FieldSet{},
			Budget: ResourceBudget{MemoryBytes: math.MaxInt64},
		},
	}

	err := NormalizeSnapshot(snapshot)
	if !HasCode(err, CodeSpecInvalid) {
		t.Fatalf("NormalizeSnapshot err=%v, want E_SPEC_INVALID", err)
	}
}
