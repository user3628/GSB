package batch

import (
	"reflect"
	"testing"

	"driftconverge/internal/apperr"
	"driftconverge/internal/detect"
	"driftconverge/internal/model"
)

func TestBatchDependencyConstraintsAndStableSplitting(t *testing.T) {
	samples, report := loadDrift(t, false)
	first, err := Organize(samples.Cluster, report.Results, samples.Policy)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Organize(samples.Cluster, report.Results, samples.Policy)
	if err != nil {
		t.Fatal(err)
	}
	if first.StableSignature != second.StableSignature || !reflect.DeepEqual(first.Batches, second.Batches) {
		t.Fatal("batch organization must be reproducible")
	}
	if first.Stats.CleanObjects != 3 || first.Stats.BatchedObjects != 7 {
		t.Fatalf("unexpected stats: %+v", first.Stats)
	}
	objectBatch := map[string]int{}
	for _, batch := range first.Batches {
		if len(batch.ObjectIDs) > samples.Policy.BatchSizeLimit {
			t.Fatalf("batch size %d exceeds limit %d", len(batch.ObjectIDs), samples.Policy.BatchSizeLimit)
		}
		for _, id := range batch.ObjectIDs {
			objectBatch[id] = batch.Index
		}
	}
	objects := samples.Cluster.ObjectByID()
	for _, batch := range first.Batches {
		for _, id := range batch.ObjectIDs {
			for _, dep := range objects[id].DependsOn {
				if depBatch, ok := objectBatch[dep]; ok && depBatch >= batch.Index {
					t.Fatalf("dependency %s batch %d is not before %s batch %d", dep, depBatch, id, batch.Index)
				}
			}
		}
	}
	for _, result := range report.Results {
		if !result.HasDrift && objectBatch[result.ObjectID] != 0 {
			t.Fatalf("clean object %s entered a batch", result.ObjectID)
		}
	}
}

func TestBatchSizeOneProducesStableUniquePartition(t *testing.T) {
	samples, report := loadDrift(t, false)
	samples.Policy.BatchSizeLimit = 1
	organized, err := Organize(samples.Cluster, report.Results, samples.Policy)
	if err != nil {
		t.Fatal(err)
	}
	if len(organized.Batches) != 7 {
		t.Fatalf("batches=%d want 7", len(organized.Batches))
	}
	seen := map[string]bool{}
	for _, batch := range organized.Batches {
		if len(batch.ObjectIDs) != 1 {
			t.Fatalf("unexpected split batch: %+v", batch)
		}
		if seen[batch.ObjectIDs[0]] {
			t.Fatalf("object repeated across batches: %s", batch.ObjectIDs[0])
		}
		seen[batch.ObjectIDs[0]] = true
	}
}

func TestDependencyCycleKeepsOutsideBatches(t *testing.T) {
	samples, err := model.LoadSamples("../../samples/cluster-cycle.json", "../../samples/policy-snapshot.json", "../../samples/drift-field-values.json")
	if err != nil {
		t.Fatal(err)
	}
	report, err := detect.Analyze(samples.Cluster, samples.Policy, samples.Values)
	if err != nil {
		t.Fatal(err)
	}
	organized, cycleErr := Organize(samples.Cluster, report.Results, samples.Policy)
	if apperr.CodeOf(cycleErr) != apperr.DependencyCycle {
		t.Fatalf("want E_DEPENDENCY_CYCLE, got %v", cycleErr)
	}
	if organized == nil || len(organized.Cycles) != 1 {
		t.Fatalf("cycle report missing: %+v", organized)
	}
	if got := organized.Cycles[0].ObjectIDs; !reflect.DeepEqual(got, []string{"cycle/a", "cycle/b", "cycle/c"}) {
		t.Fatalf("cycle members=%v", got)
	}
	batched := map[string]bool{}
	for _, batch := range organized.Batches {
		for _, id := range batch.ObjectIDs {
			batched[id] = true
		}
	}
	if !batched["outside/d"] || !batched["outside/e"] {
		t.Fatalf("outside DAG batches must survive: %+v", organized.Batches)
	}
	for _, id := range []string{"cycle/a", "cycle/b", "cycle/c"} {
		if batched[id] {
			t.Fatalf("cyclic object %s entered a batch", id)
		}
	}
	if organized.ErrorCode != string(apperr.DependencyCycle) {
		t.Fatalf("error code not persisted in report: %s", organized.ErrorCode)
	}
}

func loadDrift(t *testing.T, _ bool) (*model.Samples, *detect.Report) {
	t.Helper()
	samples, err := model.LoadSamples("../../samples/cluster-objects.json", "../../samples/policy-snapshot.json", "../../samples/drift-field-values.json")
	if err != nil {
		t.Fatal(err)
	}
	report, err := detect.Analyze(samples.Cluster, samples.Policy, samples.Values)
	if err != nil {
		t.Fatal(err)
	}
	return samples, report
}
