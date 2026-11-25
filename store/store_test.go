package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"spmmopt/spmm"
)

func fixture(t *testing.T, dir string) (*Store, *spmm.Plan, *spmm.CSRMatrix, *spmm.DenseMatrix) {
	t.Helper()
	a := &spmm.CSRMatrix{
		Rows: 6, Cols: 4,
		Indptr:  []int64{0, 2, 2, 4, 6, 6, 8},
		Indices: []int64{0, 3, 0, 0, 1, 2, 0, 3},
		Data:    []float64{1, 2, 3, 0, 4, 5, 6, 7},
	}
	b := &spmm.DenseMatrix{Rows: 4, Cols: 5, Data: make([]float64, 20)}
	for i := range b.Data {
		b.Data[i] = float64(i+1) / 10
	}
	p, err := spmm.ChoosePlan(spmm.PlanRequest{
		Matrix: a, Dense: b, RowBlockSize: 3, ColBlockSize: 4,
		SumStrategy: spmm.SumCompensated, AccumulationOrder: spmm.OrderBlockOrder,
		SkipExplicitZeros: false, AbsTolerance: 1e-10, RelTolerance: 1e-10,
	})
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(context.Background(), filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, p, a, b
}

func TestConcurrentPlanPublishOnlyOneAdvances(t *testing.T) {
	dir := t.TempDir()
	st1, plan, _, _ := fixture(t, dir)
	st2, err := Open(context.Background(), st1.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	ctx := context.Background()
	start := make(chan struct{})
	type result struct {
		rec *PlanRecord
		err error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, st := range []*Store{st1, st2} {
		wg.Add(1)
		go func(st *Store) {
			defer wg.Done()
			<-start
			rec, err := st.PublishPlan(ctx, plan, PlanExpectation{})
			results <- result{rec, err}
		}(st)
	}
	close(start)
	wg.Wait()
	close(results)
	ok, stale := 0, 0
	var first *PlanRecord
	for r := range results {
		if r.err == nil {
			ok++
			first = r.rec
		} else if spmm.IsCode(r.err, spmm.CodeStalePublish) {
			stale++
		} else {
			t.Fatalf("unexpected concurrent publish error: %v", r.err)
		}
	}
	if ok != 1 || stale != 1 {
		t.Fatalf("concurrent publishes: ok=%d stale=%d, want 1/1", ok, stale)
	}
	if first.Version != 1 {
		t.Fatalf("first version=%d want 1", first.Version)
	}
	if _, err := st1.PublishPlan(ctx, plan, PlanExpectation{Version: 0}); !spmm.IsCode(err, spmm.CodeStalePublish) {
		t.Fatalf("stale zero expectation error=%v", err)
	}
	second, err := st2.PublishPlan(ctx, plan, PlanExpectation{Version: 1, Digest: first.Digest})
	if err != nil {
		t.Fatalf("version 2 publish: %v", err)
	}
	if second.Version != 2 || second.PreviousDigest != first.Digest {
		t.Fatalf("version chain not contiguous: %+v", second)
	}
	report, err := st1.ReconcilePlan(ctx, first.PlanKey, &PlanRef{PlanKey: first.PlanKey, Version: 1, Digest: first.Digest})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != IntegrityAdvance || report.DownstreamInvalidated {
		t.Fatalf("normal advance status=%s invalidated=%v", report.Status, report.DownstreamInvalidated)
	}
}

func TestPlanPublishInitializesOperatorState(t *testing.T) {
	dir := t.TempDir()
	st, plan, _, _ := fixture(t, dir)
	ctx := context.Background()
	rec, err := st.PublishPlan(ctx, plan, PlanExpectation{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := st.State(ctx, rec.PlanKey)
	if err != nil {
		t.Fatal(err)
	}
	if state.Version != rec.Version || state.PlanDigest != rec.Digest || state.LastExecutionRow != -1 || state.State != "planned" {
		t.Fatalf("unexpected state after plan publication: %+v", state)
	}
}

func TestBlockPublishOrderDuplicateAndAtomicRollback(t *testing.T) {
	dir := t.TempDir()
	st, plan, a, b := fixture(t, dir)
	ctx := context.Background()
	rec, err := st.PublishPlan(ctx, plan, PlanExpectation{})
	if err != nil {
		t.Fatal(err)
	}
	ref := PlanRef{PlanKey: rec.PlanKey, Version: rec.Version, Digest: rec.Digest}
	b0, err := spmm.ExecuteRowUnit(a, b, plan, plan.Memory.Rows[0])
	if err != nil {
		t.Fatal(err)
	}
	b1, err := spmm.ExecuteRowUnit(a, b, plan, plan.Memory.Rows[1])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PublishBlock(ctx, b1, BlockExpectation{PlanKey: ref.PlanKey, Version: ref.Version, Digest: ref.Digest}); !spmm.IsCode(err, spmm.CodeStalePublish) {
		t.Fatalf("gap publication error=%v", err)
	}
	if _, err := st.PublishBlock(ctx, b0, BlockExpectation{PlanKey: ref.PlanKey, Version: ref.Version, Digest: ref.Digest}); err != nil {
		t.Fatalf("publish block 0: %v", err)
	}
	if _, err := st.PublishBlock(ctx, b0, BlockExpectation{PlanKey: ref.PlanKey, Version: ref.Version, Digest: ref.Digest}); !spmm.IsCode(err, spmm.CodeStalePublish) {
		t.Fatalf("duplicate publication error=%v", err)
	}
	// Simulate a failure after the block row insert but during the error-report
	// insert. The surrounding transaction must leave no half-written block.
	_, err = st.db.Exec(`INSERT INTO error_reports(plan_key, version_seq, execution_row_index, report_digest, report_json, created_at)
		VALUES (?,?,?,?,?,?)`, ref.PlanKey, ref.Version, 1, "conflict", []byte("{}"), now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PublishBlock(ctx, b1, BlockExpectation{PlanKey: ref.PlanKey, Version: ref.Version, Digest: ref.Digest}); err == nil {
		t.Fatal("expected transaction failure at error-report insert")
	}
	n, err := st.BlockCount(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("block count after rolled-back write=%d want 1", n)
	}
	blocks, err := st.LoadBlocks(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || blocks[0].ExecutionRowIndex != 0 {
		t.Fatalf("unexpected persisted blocks: %+v", blocks)
	}
}

func TestReconcileDetectsRollbackReplacementAndInvalidates(t *testing.T) {
	dir := t.TempDir()
	st, plan, a, b := fixture(t, dir)
	ctx := context.Background()
	v1, err := st.PublishPlan(ctx, plan, PlanExpectation{})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := st.PublishPlan(ctx, plan, PlanExpectation{Version: v1.Version, Digest: v1.Digest})
	if err != nil {
		t.Fatal(err)
	}
	ref := PlanRef{PlanKey: v2.PlanKey, Version: v2.Version, Digest: v2.Digest}
	block, err := spmm.ExecuteRowUnit(a, b, plan, plan.Memory.Rows[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PublishBlock(ctx, block, BlockExpectation{PlanKey: ref.PlanKey, Version: ref.Version, Digest: ref.Digest}); err != nil {
		t.Fatal(err)
	}
	// Normal advance retains downstream facts.
	adv, err := st.ReconcilePlan(ctx, v2.PlanKey, &PlanRef{PlanKey: v1.PlanKey, Version: v1.Version, Digest: v1.Digest})
	if err != nil {
		t.Fatal(err)
	}
	if adv.Status != IntegrityAdvance || adv.DownstreamInvalidated {
		t.Fatalf("normal advance=%+v", adv)
	}
	if n, _ := st.BlockCount(ctx, ref); n != 1 {
		t.Fatalf("normal advance invalidated blocks: %d", n)
	}

	// Roll the head back to version 1 while immutable history still has 2.
	_, err = st.db.Exec(`UPDATE plan_heads SET version_seq=?, plan_digest=?, payload_hash=? WHERE plan_key=?`, v1.Version, v1.Digest, v1.PayloadHash, v1.PlanKey)
	if err != nil {
		t.Fatal(err)
	}
	rollback, err := st.ReconcilePlan(ctx, v2.PlanKey, &PlanRef{PlanKey: v2.PlanKey, Version: v2.Version, Digest: v2.Digest})
	if err != nil {
		t.Fatal(err)
	}
	if rollback.Status != IntegrityRollback || !rollback.DownstreamInvalidated {
		t.Fatalf("rollback report=%+v", rollback)
	}
	if n, _ := st.BlockCount(ctx, ref); n != 0 {
		t.Fatalf("rollback did not invalidate downstream blocks: %d", n)
	}
	head, err := st.Head(ctx, v2.PlanKey)
	if err != nil || head.Version != v2.Version || head.Digest != v2.Digest {
		t.Fatalf("rollback did not restore immutable head: %+v err=%v", head, err)
	}

	// Replace the head digest at the same sequence.
	_, err = st.db.Exec(`UPDATE plan_heads SET plan_digest='replaced' WHERE plan_key=?`, v2.PlanKey)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := st.ReconcilePlan(ctx, v2.PlanKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Status != IntegrityReplacement || !replacement.DownstreamInvalidated {
		t.Fatalf("replacement report=%+v", replacement)
	}
	// Restore the digest but replace a fingerprint at the same version.
	_, err = st.db.Exec(`UPDATE plan_heads SET plan_digest=?, shape_fingerprint='tampered' WHERE plan_key=?`, v2.Digest, v2.PlanKey)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := st.ReconcilePlan(ctx, v2.PlanKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fingerprint.Status != IntegrityReplacement || !fingerprint.DownstreamInvalidated {
		t.Fatalf("fingerprint replacement report=%+v", fingerprint)
	}
	invs, err := st.Invalidations(ctx, v2.PlanKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(invs) < 3 {
		t.Fatalf("expected rollback and replacement audit entries, got %v", invs)
	}
}
