package scheduler

import (
	"testing"

	"flashattention/internal/errs"
	"flashattention/internal/tensor"
)

func TestPlanSplitsRowBlock(t *testing.T) {
	p, err := PlanWorkload(Options{
		ShardID: "x", Shape: tensor.Shape{B: 2, H: 2, S: 16, D: 8},
		RowBlock: 4, ColBlock: 4, GroupBlocks: 1, BudgetBytes: 2048,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.RowBlock >= 4 || len(p.Decisions) == 0 {
		t.Fatalf("expected split, got %+v", p)
	}
	if p.EstimatedPeakBytes > p.BudgetBytes {
		t.Fatal("plan exceeds budget")
	}
}

func TestImpossibleBudget(t *testing.T) {
	_, err := PlanWorkload(Options{RowBlock: 4, ColBlock: 4, GroupBlocks: 1, Shape: tensor.Shape{D: 8}, BudgetBytes: 1})
	if errs.Code(err) != errs.EMemoryBudget {
		t.Fatalf("code=%s err=%v", errs.Code(err), err)
	}
}
