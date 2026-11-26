package plan

import (
	"testing"

	"logagg/internal/errs"
)

func TestPlanWholeAndSegments(t *testing.T) {
	whole, err := PlanBlock(Request{BlockNo: 1, Partition: 2, RowCount: 100, DeclaredBytes: 1600, MemoryLimitBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if !whole.WholeDecode || whole.SegmentCount != 1 || whole.EstimatedPeakBytes != 1600 {
		t.Fatalf("unexpected whole plan: %+v", whole)
	}

	segmented, err := PlanBlock(Request{BlockNo: 1, RowCount: 100, DeclaredBytes: 1600, MemoryLimitBytes: 810})
	if err != nil {
		t.Fatal(err)
	}
	if segmented.WholeDecode || segmented.SegmentRows != 50 || segmented.SegmentCount != 2 {
		t.Fatalf("unexpected segment rows/count: %+v", segmented)
	}
	if segmented.EstimatedPeakBytes > segmented.MemoryLimitBytes {
		t.Fatalf("peak %d exceeds limit %d", segmented.EstimatedPeakBytes, segmented.MemoryLimitBytes)
	}
	if len(segmented.Basis) < 3 || segmented.Formula == "" {
		t.Fatalf("missing calculation basis/formula: %+v", segmented)
	}
}

func TestPlanUnboundedIncludesWidthAndBudget(t *testing.T) {
	_, err := PlanBlock(Request{BlockNo: 9, RowCount: 10, DeclaredBytes: 160, MemoryLimitBytes: 32, ReservedBytes: 24, MinRowWidthBytes: 16})
	if errs.Code(err) != errs.EBlockUnbounded {
		t.Fatalf("expected E_BLOCK_UNBOUNDED, got %v", err)
	}
	coded, ok := errs.As(err)
	if !ok {
		t.Fatal("error is not coded")
	}
	if coded.Details["min_row_width_bytes"] != int64(16) {
		t.Fatalf("missing min width: %+v", coded.Details)
	}
	if coded.Details["available_budget_bytes"] != int64(8) {
		t.Fatalf("unexpected available budget: %+v", coded.Details)
	}
}

func TestPlanUnboundedWhenBudgetBelowMinimumWidth(t *testing.T) {
	_, err := PlanBlock(Request{BlockNo: 9, RowCount: 10, DeclaredBytes: 160, MemoryLimitBytes: 8, MinRowWidthBytes: 16})
	if errs.Code(err) != errs.EBlockUnbounded {
		t.Fatalf("expected E_BLOCK_UNBOUNDED, got %v", err)
	}
	coded, ok := errs.As(err)
	if !ok {
		t.Fatal("error is not coded")
	}
	if coded.Details["available_budget_bytes"] != int64(8) {
		t.Fatalf("missing available budget: %+v", coded.Details)
	}
}

func TestPlanDeclaredBytesBelowMinimum(t *testing.T) {
	_, err := PlanBlock(Request{RowCount: 1, DeclaredBytes: 8, MemoryLimitBytes: 4096, MinRowWidthBytes: 16})
	if errs.Code(err) != errs.EBlockUnbounded {
		t.Fatalf("expected unbounded, got %v", err)
	}
}
