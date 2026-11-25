package service_test

import (
	"context"
	"testing"

	"flashattention/internal/dataset"
	"flashattention/internal/service"
)

func TestCausalIncrementalReusesEarlierRowBlocks(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	spec := dataset.Spec{
		ID: "causal-inc", B: 1, H: 1, S: 24, D: 8, Causal: true,
		RowBlock: 6, ColBlock: 6, Group: 1, Bits: 24, ExpSumBits: 64,
		Strategy: "scaled", Seed: 31337,
	}
	if _, err := svc.Generate(spec, true); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Run(ctx, "causal-inc", service.RunOptions{}); err != nil {
		t.Fatal(err)
	}
	rep, err := svc.Incremental(ctx, service.UpdateRequest{ShardID: "causal-inc", Mode: "correct", Start: 18, End: 21, Seed: 515}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.ByteIdentical {
		t.Fatalf("mismatch: %+v", rep.FirstDifference)
	}
	reusedRows, recomputeRows := 0, 0
	for _, rb := range rep.RowBlocks {
		if rb.Mode == "reused_row_block_checkpoint" {
			reusedRows++
		}
		if len(rb.RecomputedColBlocks) > 0 {
			recomputeRows++
		}
	}
	if reusedRows < 3 {
		t.Fatalf("expected first three row blocks reused, got %+v", rep.RowBlocks)
	}
	if recomputeRows != 1 {
		t.Fatalf("expected only row block 3 recomputed, got %d", recomputeRows)
	}
}
