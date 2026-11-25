package convergence

import "testing"

func TestPlanDependencyOrderCapacityAndSplitMetadata(t *testing.T) {
	plan, err := BuildPlan(SampleSnapshotV1().Objects, SamplePolicy())
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	objectBatch := map[string]Batch{}
	for _, batch := range plan.Batches {
		if len(batch.ObjectIDs) > SamplePolicy().MaxBatchSize {
			t.Fatalf("batch %s exceeds max size: %d", batch.BatchID, len(batch.ObjectIDs))
		}
		if batch.SplitIndex > 0 && batch.OriginalBatchID == batch.BatchID {
			t.Fatalf("split child must preserve original id separately: %+v", batch)
		}
		for _, id := range batch.ObjectIDs {
			if _, exists := objectBatch[id]; exists {
				t.Fatalf("object %s placed twice", id)
			}
			objectBatch[id] = batch
		}
	}

	byID := map[string]Object{}
	for _, object := range SampleSnapshotV1().Objects {
		byID[object.ID] = object
	}
	for id, batch := range objectBatch {
		for _, dep := range byID[id].DependsOn {
			depBatch, ok := objectBatch[dep]
			if !ok {
				t.Fatalf("dependency %s missing from batches", dep)
			}
			if depBatch.Sequence >= batch.Sequence {
				t.Fatalf("dependency order violated: %s seq=%d, dependent %s seq=%d", dep, depBatch.Sequence, id, batch.Sequence)
			}
		}
	}

	var first *Batch
	for i := range plan.Batches {
		if plan.Batches[i].BatchID == "B001-S1" {
			first = &plan.Batches[i]
		}
	}
	if first == nil || first.OriginalBatchID != "B001" || first.SplitIndex != 1 || first.SplitCount != 2 {
		t.Fatalf("B001 split metadata wrong: %+v", first)
	}
}

func TestDependencyCycleReturnsCycleAndOutsideLayers(t *testing.T) {
	plan, err := BuildPlan(SampleCycleSnapshot().Objects, SamplePolicy())
	if !HasCode(err, CodeDependencyCycle) {
		t.Fatalf("err=%v, want E_DEPENDENCY_CYCLE", err)
	}
	if plan == nil {
		t.Fatal("partial plan must be returned with cycle error")
	}
	if len(plan.Cycles) != 1 || len(plan.Cycles[0].ObjectIDs) != 3 {
		t.Fatalf("cycles wrong: %+v", plan.Cycles)
	}
	if !containsString(plan.CyclicObjectIDs, "alpha") || !containsString(plan.CyclicObjectIDs, "bravo") || !containsString(plan.CyclicObjectIDs, "charlie") {
		t.Fatalf("cyclic ids wrong: %v", plan.CyclicObjectIDs)
	}
	outsideBatch := ""
	for _, batch := range plan.Batches {
		if containsString(batch.ObjectIDs, "outside") {
			outsideBatch = batch.BatchID
		}
		if containsString(batch.ObjectIDs, "alpha") {
			t.Fatalf("cycle node alpha should not be scheduled: %+v", batch)
		}
	}
	if outsideBatch == "" {
		t.Fatalf("outside object was not layered: %+v", plan.Layers)
	}
	if _, ok := plan.BlockedByCycle["downstream"]; !ok {
		t.Fatalf("downstream should be marked blocked by cycle: %+v", plan.BlockedByCycle)
	}
}
