package scheduler

import (
	"reflect"
	"testing"

	"local/quantops/pkg/dataset"
	"local/quantops/pkg/quant"
)

func testShard(id string, ordinal int) dataset.Shard {
	cfg := quant.Config{Bits: 8, Mode: quant.Symmetric, Granularity: quant.PerTensor}
	return dataset.Shard{
		ID: id, Ordinal: ordinal,
		Activation:          dataset.TensorRef{Name: id + ".a", Shape: []int{2, 4}},
		Weights:             []dataset.TensorRef{{Name: id + ".w", Shape: []int{3, 4}}},
		Chain:               []dataset.Layer{{Name: "linear", Activation: cfg, Weight: cfg}},
		ExpectedOutputShape: []int{2, 3},
	}
}

func TestPlanDeterministicAndBalanced(t *testing.T) {
	shards := []dataset.Shard{
		testShard("d", 4), testShard("b", 2), testShard("a", 1), testShard("c", 3),
	}
	limits := Limits{TotalElements: 100000, PerWorkerElements: 100000, PeakMemoryBytes: 1 << 30, MaxWorkers: 2}
	p1, err := PlanBatches(shards, limits)
	if err != nil {
		t.Fatal(err)
	}
	shuffled := []dataset.Shard{shards[3], shards[1], shards[0], shards[2]}
	p2, err := PlanBatches(shuffled, limits)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p1, p2) {
		t.Fatalf("plan depends on input order:\n%+v\n%+v", p1, p2)
	}
	if p1.Concurrency != 2 {
		t.Fatalf("concurrency = %d, want 2 (queues %v)", p1.Concurrency, p1.WorkerQueues)
	}
	if len(p1.Accepted) != 4 {
		t.Fatalf("accepted = %d", len(p1.Accepted))
	}
	for i := 1; i < len(p1.Accepted); i++ {
		if p1.Accepted[i-1].ShardOrdinal > p1.Accepted[i].ShardOrdinal {
			t.Fatalf("acceptance order is not ordinal: %+v", p1.Accepted)
		}
	}
}

func TestPlanBudgetRejectionReportsEstimateAndRemaining(t *testing.T) {
	shards := []dataset.Shard{testShard("a", 1), testShard("b", 2)}
	est := EstimateShard(shards[0])
	limits := Limits{TotalElements: est.Elements, PerWorkerElements: 100000, PeakMemoryBytes: 1 << 30, MaxWorkers: 1}
	p, err := PlanBatches(shards, limits)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Accepted) != 1 || len(p.Rejected) != 1 {
		t.Fatalf("accepted=%d rejected=%d", len(p.Accepted), len(p.Rejected))
	}
	r := p.Rejected[0]
	if r.Code != "E_BUDGET_EXCEEDED" || r.EstimatedElements != est.Elements || r.BudgetRemaining != 0 {
		t.Fatalf("unexpected rejection %+v", r)
	}
}

func TestPlanHonorsMemoryAndWorkerQuota(t *testing.T) {
	shards := []dataset.Shard{testShard("a", 1), testShard("b", 2)}
	est := EstimateShard(shards[0])
	p, err := PlanBatches(shards, Limits{
		TotalElements: 100000, PerWorkerElements: est.Elements,
		PeakMemoryBytes: est.WorkingSetBytes, MaxWorkers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Accepted) != 1 || len(p.Rejected) != 1 {
		t.Fatalf("quota plan accepted=%d rejected=%d", len(p.Accepted), len(p.Rejected))
	}
	p, err = PlanBatches(shards, Limits{
		TotalElements: 100000, PerWorkerElements: est.Elements,
		PeakMemoryBytes: est.WorkingSetBytes, MaxWorkers: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Accepted) != 1 || len(p.Rejected) != 1 || p.Rejected[0].Reason != "peak memory budget would be exceeded" {
		t.Fatalf("memory plan accepted=%d rejected=%+v", len(p.Accepted), p.Rejected)
	}
}
