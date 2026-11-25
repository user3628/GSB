package scheduler

import (
	"testing"

	"gpusched/internal/domain"
)

func req(id, node, model string, prio, seq int, mem int64, samples, limit int64) domain.Request {
	return domain.Request{
		ID: id, AssignedNodeID: node, ModelID: model, Priority: prio, Seq: int64(seq),
		MemoryNeed: mem, SamplesPerBatch: samples, LatencyLimitMs: limit,
		State: domain.StateAdmitted, GroupID: "g1",
	}
}

func TestOrganizeAdjacentSameKeyAndPriorityDivider(t *testing.T) {
	requests := []domain.Request{
		req("r1", "n1", "m1", 0, 1, 100, 10, 1000),
		req("r2", "n1", "m1", 0, 2, 100, 10, 1000),
		req("r3", "n1", "m1", 1, 3, 100, 10, 1000), // different priority -> split
		req("r4", "n1", "m2", 0, 4, 100, 10, 1000), // different model -> split
	}
	free := map[string]int64{"n1": 1_000_000}
	batches, violations := Organize(requests, free, map[string]int64{"m1": 1000, "m2": 1000}, 5, 1)
	if len(violations) != 0 {
		t.Fatalf("unexpected violations: %v", violations)
	}
	if len(batches) != 3 {
		t.Fatalf("want 3 batches got %d", len(batches))
	}
	if len(batches[0].MemberIDs) != 2 || batches[0].MemberIDs[0] != "r1" || batches[0].MemberIDs[1] != "r2" {
		t.Errorf("first batch must keep adjacent order r1,r2: %v", batches[0].MemberIDs)
	}
	if batches[1].Priority != 1 || batches[1].MemberIDs[0] != "r3" {
		t.Errorf("priority divider wrong: %+v", batches[1])
	}
}

func TestOrganizeSplitsOnMemory(t *testing.T) {
	requests := []domain.Request{
		req("r1", "n1", "m1", 0, 1, 600, 10, 1000),
		req("r2", "n1", "m1", 0, 2, 600, 10, 1000),
		req("r3", "n1", "m1", 0, 3, 600, 10, 1000),
	}
	free := map[string]int64{"n1": 1000}
	batches, _ := Organize(requests, free, map[string]int64{"m1": 1000}, 5, 1)
	for _, b := range batches {
		if b.TotalMemory > 1000 {
			t.Errorf("batch exceeds node free memory: %+v", b)
		}
	}
	if len(batches) != 3 {
		t.Fatalf("each 600-byte request needs its own batch, got %d", len(batches))
	}
}

func TestOrganizeSplitsOnLatency(t *testing.T) {
	// base=10, perSample=1 -> latency of a batch with N samples = 10+N.
	requests := []domain.Request{
		req("r1", "n1", "m1", 0, 1, 10, 20, 25), // solo 30 > 25 -> violation
		req("r2", "n1", "m1", 0, 2, 10, 5, 30),  // combined 35 > r2 limit 30 -> split
	}
	free := map[string]int64{"n1": 1_000_000}
	batches, violations := Organize(requests, free, map[string]int64{"m1": 1000}, 10, 1)
	if len(batches) != 2 {
		t.Fatalf("latency split should produce 2 batches got %d", len(batches))
	}
	found := false
	for _, v := range violations {
		if v == "r1:solo_latency=30>limit=25" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected solo latency violation, got %v", violations)
	}
}

func TestEstimateLatencyDeterministic(t *testing.T) {
	got := EstimateLatencyMs(5, 1.5, 10)
	if got != 20 {
		t.Errorf("latency=%d want 20", got)
	}
}
