package feedbackstream

import "testing"

func TestGlobalPartitionConcurrencyLimit(t *testing.T) {
	policy := DefaultPolicy()
	policy.PartitionConcurrency = 1
	policy.RoundTimeBudgetMS = 1_000_000
	batches, err := BuildWindowBatches(DefaultEvents(), policy.Window)
	if err != nil {
		t.Fatal(err)
	}
	candidates := make([]Candidate, 0, len(batches))
	for _, batch := range batches {
		candidates = append(candidates, CandidateFromBatch(batch, policy))
	}
	plan := PlanRound("global-partition", candidates, policy, nil, 0)

	admitted := []string{}
	shedPartitions := map[int]bool{}
	for _, decision := range plan.Admissions {
		if decision.Admitted {
			admitted = append(admitted, decision.BatchID)
		}
	}
	for _, decision := range plan.Shed {
		if decision.ReasonCode == "PARTITION_CONCURRENCY_LIMIT" {
			shedPartitions[decision.PartitionID] = true
		}
	}
	if len(admitted) != 1 || admitted[0] != "batch-p0-w0" {
		t.Fatalf("expected only first partition admitted, got %#v", admitted)
	}
	if !shedPartitions[1] || !shedPartitions[2] {
		t.Fatalf("expected partitions 1 and 2 to be shed by global limit: %#v", plan.Shed)
	}
}
