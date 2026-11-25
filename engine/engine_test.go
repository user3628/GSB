package engine_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"spmmopt/engine"
	"spmmopt/samples"
	"spmmopt/spmm"
	"spmmopt/store"
)

func generatedSamples(t *testing.T) (string, *samples.Manifest) {
	t.Helper()
	dir := t.TempDir()
	m, err := samples.GenerateAll(dir, samples.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	return dir, m
}

func planFor(t *testing.T, s *samples.Sample, strategy spmm.SumStrategy, order spmm.AccumulationOrder) *spmm.Plan {
	t.Helper()
	p, err := spmm.ChoosePlan(spmm.PlanRequest{
		Matrix: s.CSR, Dense: s.Dense,
		CacheBudgetBytes: 256 << 10, PeakMemoryBytes: 16 << 20,
		SumStrategy: strategy, AccumulationOrder: order,
		SkipExplicitZeros: true, InteractiveBudgetMS: 5000,
	})
	if err != nil {
		t.Fatalf("plan %s: %v", s.Name, err)
	}
	return p
}

func openPublished(t *testing.T, dbPath string, s *samples.Sample, p *spmm.Plan) (*store.Store, *engine.Engine, *store.PlanRecord) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := st.PublishPlan(ctx, p, store.PlanExpectation{})
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	en, err := engine.Open(ctx, st, s.CSR, s.Dense, rec.PlanKey)
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	return st, en, rec
}

func TestDualPathsAcrossAllSamples(t *testing.T) {
	dir, manifest := generatedSamples(t)
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "all.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, name := range manifest.Names() {
		t.Run(name, func(t *testing.T) {
			s, err := manifest.Load(dir, name)
			if err != nil {
				t.Fatal(err)
			}
			p := planFor(t, s, spmm.SumCompensated, spmm.OrderBlockOrder)
			if len(p.Selection.Candidates) < 2 || len(p.Selection.Rejected) == 0 {
				t.Fatalf("plan does not expose candidates and rejected alternatives")
			}
			expected := store.PlanExpectation{}
			key := spmm.PlanKey(p)
			if head, err := st.Head(ctx, key); err == nil {
				expected = store.PlanExpectation{Version: head.Version, Digest: head.Digest}
			}
			rec, err := st.PublishPlan(ctx, p, expected)
			if err != nil {
				t.Fatalf("publish plan: %v", err)
			}
			en, err := engine.Open(ctx, st, s.CSR, s.Dense, rec.PlanKey)
			if err != nil {
				t.Fatal(err)
			}
			query, err := en.Query(ctx, engine.QueryOptions{RowStart: 0, RowEnd: s.CSR.Rows, LatencyBudget: 5 * time.Second})
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			settlement, err := en.Settle(ctx, engine.RunOptions{Persist: true})
			if err != nil {
				t.Fatalf("settle: %v", err)
			}
			if err := engine.ComparePaths(query, settlement); err != nil {
				t.Fatalf("dual path mismatch: %v", err)
			}
			if query.CanonicalDigest != settlement.CanonicalDigest {
				t.Fatalf("full query and settlement canonical digests differ")
			}
			if settlement.CompletedUnits != len(p.Memory.Rows) || settlement.Stopped {
				t.Fatalf("settlement incomplete: completed=%d/%d stopped=%v", settlement.CompletedUnits, len(p.Memory.Rows), settlement.Stopped)
			}
			if len(settlement.Values) != s.CSR.Rows*s.Dense.Cols {
				t.Fatalf("settlement values=%d want %d", len(settlement.Values), s.CSR.Rows*s.Dense.Cols)
			}
			for _, b := range settlement.Blocks {
				if b.Error.ReferenceMethod == "" || b.Error.SampleCount == 0 || len(b.Error.Samples) == 0 {
					t.Fatalf("missing explicit local reference samples for block %d", b.Summary.ExecutionRowIndex)
				}
				if b.Error.ToleranceExceeded || b.Error.MaxAbsDeviation != b.Error.UpperBoundAbs {
					t.Fatalf("unexpected error report for block %d: %+v", b.Summary.ExecutionRowIndex, b.Error)
				}
			}
			again, err := en.Query(ctx, engine.QueryOptions{RowStart: 0, RowEnd: s.CSR.Rows, LatencyBudget: 5 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if again.CanonicalDigest != query.CanonicalDigest {
				t.Fatalf("repeat query changed canonical digest")
			}
			blocks, err := st.LoadBlocks(ctx, en.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if len(blocks) != len(p.Memory.Rows) {
				t.Fatalf("persisted blocks=%d want %d", len(blocks), len(p.Memory.Rows))
			}
			for i := range blocks {
				if blocks[i].ExecutionRowIndex != i {
					t.Fatalf("block sequence gap at %d", i)
				}
			}
		})
	}
}

func TestGracefulStopResumeMatchesContinuousRun(t *testing.T) {
	dir, manifest := generatedSamples(t)
	s, err := manifest.Load(dir, "square_p10")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	baseDB := filepath.Join(t.TempDir(), "baseline.db")
	stopDB := filepath.Join(t.TempDir(), "resume.db")

	baseStore, baseEngine, _ := openPublished(t, baseDB, s, planFor(t, s, spmm.SumCompensated, spmm.OrderBlockOrder))
	defer baseStore.Close()
	baseline, err := baseEngine.Settle(ctx, engine.RunOptions{Persist: true})
	if err != nil {
		t.Fatal(err)
	}

	stopStore, stopEngine, rec := openPublished(t, stopDB, s, planFor(t, s, spmm.SumCompensated, spmm.OrderBlockOrder))
	defer stopStore.Close()
	stop := make(chan struct{})
	partial, err := stopEngine.Settle(ctx, engine.RunOptions{
		Persist: true, Stop: stop,
		FaultHook: func(ev engine.ExecutionEvent) error {
			if ev.Kind == engine.EventAfterCommit && ev.UnitIndex == 0 {
				close(stop)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !partial.Stopped || partial.CompletedUnits != 1 {
		t.Fatalf("graceful stop did not stop after current unit: stopped=%v completed=%d", partial.Stopped, partial.CompletedUnits)
	}
	state, err := stopStore.State(ctx, rec.PlanKey)
	if err != nil || state.State != "stopped" {
		t.Fatalf("operator state after graceful stop: %+v err=%v", state, err)
	}
	resumed, err := stopEngine.Settle(ctx, engine.RunOptions{Persist: true})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.CanonicalDigest != baseline.CanonicalDigest {
		t.Fatalf("resumed canonical result differs: %s vs %s", resumed.CanonicalDigest, baseline.CanonicalDigest)
	}
	baseBytes, err := engine.StableJSON(baseline)
	if err != nil {
		t.Fatal(err)
	}
	resumedBytes, err := engine.StableJSON(resumed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(baseBytes, resumedBytes) {
		t.Fatal("resumed result is not byte-identical to the continuous run")
	}
	if resumed.CompletedUnits != len(resumed.Blocks) || resumed.RowEnd != s.CSR.Rows {
		t.Fatalf("resumed result incomplete: %+v", resumed)
	}
	query, err := stopEngine.Query(ctx, engine.QueryOptions{RowStart: 0, RowEnd: s.CSR.Rows, LatencyBudget: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ComparePaths(query, resumed); err != nil {
		t.Fatalf("resumed query mismatch: %v", err)
	}
}

func TestQueryLatencyBudgetAndPathMismatch(t *testing.T) {
	dir, manifest := generatedSamples(t)
	s, err := manifest.Load(dir, "wide_p10")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	st, en, _ := openPublished(t, filepath.Join(t.TempDir(), "latency.db"), s, planFor(t, s, spmm.SumSequential, spmm.OrderColumnAscending))
	defer st.Close()
	_, err = en.Query(ctx, engine.QueryOptions{RowStart: 0, RowEnd: s.CSR.Rows, LatencyBudget: time.Nanosecond})
	if !spmm.IsCode(err, spmm.CodeLatencyBudget) {
		t.Fatalf("latency budget error=%v", err)
	}
	e, _ := spmm.AsError(err)
	if e.QueryID == "" || e.ElapsedMS <= 0 || e.BudgetMS <= 0 {
		t.Fatalf("latency error lacks query id or elapsed/budget: %+v", e)
	}
	query, err := en.Query(ctx, engine.QueryOptions{RowStart: 1, RowEnd: 39, LatencyBudget: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := en.Settle(ctx, engine.RunOptions{Persist: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ComparePaths(query, settlement); err != nil {
		t.Fatal(err)
	}
	query.Blocks[0].Summary.OutputDigest = "tampered"
	err = engine.ComparePaths(query, settlement)
	if !spmm.IsCode(err, spmm.CodePathMismatch) {
		t.Fatalf("tampered path error=%v", err)
	}
	pe, _ := spmm.AsError(err)
	if pe.FirstDiff == nil || *pe.FirstDiff != query.Blocks[0].Summary.ExecutionRowIndex {
		t.Fatalf("missing first differing block: %+v", pe)
	}
}

func TestEngineNumericMismatchBusinessCode(t *testing.T) {
	a := &spmm.CSRMatrix{Rows: 1, Cols: 1, Indptr: []int64{0, 1}, Indices: []int64{0}, Data: []float64{1.0 / 3.0}}
	b := &spmm.DenseMatrix{Rows: 1, Cols: 1, Data: []float64{1.0 / 3.0}}
	s := &samples.Sample{Definition: samples.Definition{Name: "precision"}, CSR: a, Dense: b}
	p, err := spmm.ChoosePlan(spmm.PlanRequest{Matrix: a, Dense: b, AbsTolerance: 1e-30, RelTolerance: 1e-30})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	st, en, _ := openPublished(t, filepath.Join(t.TempDir(), "numeric.db"), s, p)
	defer st.Close()
	_, err = en.Query(ctx, engine.QueryOptions{RowStart: 0, RowEnd: 1, LatencyBudget: time.Second})
	if !spmm.IsCode(err, spmm.CodeNumericMismatch) {
		t.Fatalf("numeric mismatch error=%v", err)
	}
	e, _ := spmm.AsError(err)
	if e.MaxAbsDev <= 0 || e.RowEnd <= e.RowStart || e.ColEnd <= e.ColStart {
		t.Fatalf("numeric mismatch lacks reported interval/deviation: %+v", e)
	}
}
