package spmm_test

import (
	"encoding/json"
	"math"
	"testing"

	"spmmopt/spmm"
)

func tinyOperands() (*spmm.CSRMatrix, *spmm.DenseMatrix) {
	a := &spmm.CSRMatrix{
		Name: "ordering", Rows: 4, Cols: 3,
		Indptr:  []int64{0, 4, 4, 5, 5},
		Indices: []int64{2, 2, 0, 1, 2},
		Data:    []float64{1e16, -1e16, 1, 2, 0},
	}
	b := &spmm.DenseMatrix{Rows: 3, Cols: 2, Data: []float64{1, 2, 10, 20, 1, 1}}
	return a, b
}

func tinyPlan(t *testing.T, a *spmm.CSRMatrix, b *spmm.DenseMatrix, order spmm.AccumulationOrder, skipZero bool) *spmm.Plan {
	t.Helper()
	p, err := spmm.ChoosePlan(spmm.PlanRequest{
		Matrix: a, Dense: b, RowBlockSize: 4, ColBlockSize: 3,
		SumStrategy: spmm.SumSequential, AccumulationOrder: order,
		SkipExplicitZeros: skipZero, AbsTolerance: 1e3, RelTolerance: 1e3,
	})
	if err != nil {
		t.Fatalf("ChoosePlan: %v", err)
	}
	return p
}

func TestKernelAccumulationOrderAndDegenerateCounts(t *testing.T) {
	a, b := tinyOperands()
	asc := tinyPlan(t, a, b, spmm.OrderColumnAscending, true)
	block := tinyPlan(t, a, b, spmm.OrderBlockOrder, true)
	ascUnit := asc.Memory.Rows[0]
	blockUnit := block.Memory.Rows[0]
	ar, err := spmm.ExecuteRowUnit(a, b, asc, ascUnit)
	if err != nil {
		t.Fatalf("ascending execute: %v", err)
	}
	br, err := spmm.ExecuteRowUnit(a, b, block, blockUnit)
	if err != nil {
		t.Fatalf("block execute: %v", err)
	}
	if got := ar.Values[0]; got != 20 {
		t.Fatalf("ascending row0 col0 = %v, want 20 due to large-term cancellation", got)
	}
	if got := br.Values[0]; got != 21 {
		t.Fatalf("block-order row0 col0 = %v, want 21", got)
	}
	wantBlockOrder := []int64{2, 2, 0, 1}
	gotOrder := br.Trace.Rows[0].AccumulationColumns
	if len(gotOrder) != len(wantBlockOrder) {
		t.Fatalf("block order trace len=%d want %d", len(gotOrder), len(wantBlockOrder))
	}
	for i := range wantBlockOrder {
		if gotOrder[i] != wantBlockOrder[i] {
			t.Fatalf("block order trace=%v want %v", gotOrder, wantBlockOrder)
		}
	}
	if br.Trace.DuplicateEntries != 1 || br.Trace.Rows[0].DuplicateColumns[0] != 2 {
		t.Fatalf("duplicate tracing wrong: %+v", br.Trace.Rows[0])
	}
	if br.Trace.ExplicitZerosSkipped != 1 || br.Trace.ExplicitZerosKept != 0 {
		t.Fatalf("skip-zero counts: skipped=%d kept=%d", br.Trace.ExplicitZerosSkipped, br.Trace.ExplicitZerosKept)
	}

	keepPlan := tinyPlan(t, a, b, spmm.OrderBlockOrder, false)
	kept, err := spmm.ExecuteRowUnit(a, b, keepPlan, keepPlan.Memory.Rows[0])
	if err != nil {
		t.Fatalf("keep-zero execute: %v", err)
	}
	if kept.Trace.ExplicitZerosKept != 1 || kept.Trace.ExplicitZerosSkipped != 0 {
		t.Fatalf("keep-zero counts: skipped=%d kept=%d", kept.Trace.ExplicitZerosSkipped, kept.Trace.ExplicitZerosKept)
	}
	if math.Float64bits(kept.Values[0]) != math.Float64bits(br.Values[0]) {
		t.Fatalf("explicit zero retention changed non-zero output")
	}
}

func TestKernelDeterministicBytes(t *testing.T) {
	a, b := tinyOperands()
	p := tinyPlan(t, a, b, spmm.OrderBlockOrder, true)
	r1, err := spmm.ExecuteRowUnit(a, b, p, p.Memory.Rows[0])
	if err != nil {
		t.Fatal(err)
	}
	r2, err := spmm.ExecuteRowUnit(a, b, p, p.Memory.Rows[0])
	if err != nil {
		t.Fatal(err)
	}
	j1, _ := json.Marshal(r1)
	j2, _ := json.Marshal(r2)
	if string(j1) != string(j2) {
		t.Fatal("same input and configuration produced different block bytes")
	}
	if r1.Summary.OutputDigest != r2.Summary.OutputDigest || r1.Summary.TraceDigest != r2.Summary.TraceDigest {
		t.Fatal("digest changed between deterministic executions")
	}
}

func TestMemoryPlanSplitsAndRefuses(t *testing.T) {
	a := &spmm.CSRMatrix{Rows: 8, Cols: 32, Indptr: make([]int64, 9), Indices: []int64{}, Data: []float64{}}
	for r := 0; r < 8; r++ {
		a.Indptr[r+1] = a.Indptr[r]
	}
	b := &spmm.DenseMatrix{Rows: 32, Cols: 16, Data: make([]float64, 32*16)}
	for i := range b.Data {
		b.Data[i] = 0.01 * float64(i%7)
	}
	p, err := spmm.ChoosePlan(spmm.PlanRequest{
		Matrix: a, Dense: b, RowBlockSize: 8, ColBlockSize: 32,
		PeakMemoryBytes: 4096, SumStrategy: spmm.SumCompensated,
	})
	if err != nil {
		t.Fatalf("split-capable plan: %v", err)
	}
	if !p.Memory.ColumnSplit || len(p.Memory.SplitDecisions) == 0 {
		t.Fatalf("expected an auditable output-column split, got %+v", p.Memory)
	}
	if p.Memory.MaxEstimateBytes > 4096 {
		t.Fatalf("split estimate %d exceeds 4096", p.Memory.MaxEstimateBytes)
	}
	if len(p.Memory.Steps) == 0 {
		t.Fatal("expected per-step memory estimates")
	}
	p2, err := spmm.ChoosePlan(spmm.PlanRequest{Matrix: a, Dense: b, PeakMemoryBytes: 8})
	if err == nil || !spmm.IsCode(err, spmm.CodeMemoryBudget) {
		t.Fatalf("tiny peak budget error = %v", err)
	}
	if p2 != nil {
		t.Fatal("unexpected plan for impossible budget")
	}
	e, _ := spmm.AsError(err)
	if e.Memory == nil || e.Memory.PeakLimitBytes != 8 {
		t.Fatalf("missing memory breakdown: %+v", e)
	}
}

func TestNumericMismatchCarriesRangesAndDeviations(t *testing.T) {
	a := &spmm.CSRMatrix{Rows: 1, Cols: 1, Indptr: []int64{0, 1}, Indices: []int64{0}, Data: []float64{1.0 / 3.0}}
	b := &spmm.DenseMatrix{Rows: 1, Cols: 1, Data: []float64{1.0 / 3.0}}
	p, err := spmm.ChoosePlan(spmm.PlanRequest{
		Matrix: a, Dense: b, RowBlockSize: 1, ColBlockSize: 1,
		SumStrategy: spmm.SumSequential, AbsTolerance: 1e-30, RelTolerance: 1e-30,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := spmm.ExecuteRowUnit(a, b, p, p.Memory.Rows[0])
	if err == nil || !spmm.IsCode(err, spmm.CodeNumericMismatch) {
		t.Fatalf("expected E_NUMERIC_MISMATCH, got %v", err)
	}
	e, _ := spmm.AsError(err)
	if e.RowEnd <= e.RowStart || e.ColEnd <= e.ColStart || e.MaxAbsDev <= 0 {
		t.Fatalf("mismatch fields incomplete: %+v", e)
	}
	if result == nil || len(result.Error.Samples) == 0 || result.Error.ReferenceMethod == "" {
		t.Fatal("expected an attached, explicit sampled error report")
	}
}

func TestNumericToleranceUsesHybridAbsoluteRelativeRule(t *testing.T) {
	a := &spmm.CSRMatrix{Rows: 1, Cols: 1, Indptr: []int64{0, 1}, Indices: []int64{0}, Data: []float64{1.0 / 3.0}}
	b := &spmm.DenseMatrix{Rows: 1, Cols: 1, Data: []float64{1.0 / 3.0}}
	cases := []struct {
		name   string
		absTol float64
		relTol float64
	}{
		{name: "absolute_within", absTol: 1, relTol: 1e-30},
		{name: "relative_within", absTol: 1e-30, relTol: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := spmm.ChoosePlan(spmm.PlanRequest{
				Matrix: a, Dense: b, RowBlockSize: 1, ColBlockSize: 1,
				SumStrategy: spmm.SumSequential, AbsTolerance: tc.absTol, RelTolerance: tc.relTol,
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := spmm.ExecuteRowUnit(a, b, p, p.Memory.Rows[0])
			if err != nil {
				t.Fatalf("one tolerance domain should accept the sampled error: %v", err)
			}
			if result.Error.ToleranceExceeded {
				t.Fatalf("hybrid tolerance unexpectedly exceeded: %+v", result.Error)
			}
		})
	}
}

func TestAutomaticPlanSmallInputIsNotOversized(t *testing.T) {
	const rows, cols, denseCols = 24, 24, 16
	indptr := make([]int64, rows+1)
	var indices []int64
	var data []float64
	for r := 0; r < rows; r++ {
		indptr[r] = int64(len(indices))
		for k := 0; k < 2; k++ {
			indices = append(indices, int64((r*3+k)%cols))
			data = append(data, 0.5)
		}
	}
	indptr[rows] = int64(len(indices))
	a := &spmm.CSRMatrix{Rows: rows, Cols: cols, Indptr: indptr, Indices: indices, Data: data}
	b := &spmm.DenseMatrix{Rows: cols, Cols: denseCols, Data: make([]float64, cols*denseCols)}
	for i := range b.Data {
		b.Data[i] = 0.1
	}
	p, err := spmm.ChoosePlan(spmm.PlanRequest{Matrix: a, Dense: b, CacheBudgetBytes: 64 << 10, PeakMemoryBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if p.RowBlockSize > rows/2 {
		t.Fatalf("small input selected oversized row block %d for %d rows", p.RowBlockSize, rows)
	}
	if len(p.Selection.Rejected) == 0 || p.Selection.Reason == "" {
		t.Fatal("automatic selection did not retain rejected candidates or rationale")
	}
	empty := 0
	for _, blk := range p.RowBlocks {
		if blk.Empty {
			empty++
		}
	}
	if len(p.RowBlocks) > 1 && empty == len(p.RowBlocks) {
		t.Fatal("plan degenerated to all-empty row blocks")
	}
}
