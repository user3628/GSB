package arbiter

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestIdempotentCommitExecutesOnceAndDetectsConflict(t *testing.T) {
	h := newHarness(t)
	svc := NewIdempotentService(h.eng)
	ctx := context.Background()
	lease := mustAcquire(t, h, "r1", "alice", time.Minute)
	req := CommitRequest{
		RowID: "r1", Holder: "alice", LeaseVersion: lease.LeaseVersion,
		BaseRowVersion: 1, Content: "once", RequestID: "idem-1",
	}
	first, err := svc.Commit(ctx, req)
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	second, err := svc.Commit(ctx, req)
	if err != nil {
		t.Fatalf("duplicate commit: %v", err)
	}
	if first.RowVersion != second.RowVersion || first.CommitVer != second.CommitVer {
		t.Fatalf("duplicate produced a different result: %+v vs %+v", first, second)
	}
	latest, _ := h.eng.latestCommitted("r1")
	if latest.RowVersion != 2 {
		t.Fatalf("duplicate request executed twice, latest=%d", latest.RowVersion)
	}

	// Same id, different payload -> E_CONFLICTING_REQUEST.
	req.Content = "different"
	_, err = svc.Commit(ctx, req)
	if errCode(t, err) != CodeConflictingRequest {
		t.Fatalf("expected conflicting request, got %v", err)
	}
}

func TestConcurrentDuplicatesReuseOneResult(t *testing.T) {
	h := newHarness(t)
	svc := NewIdempotentService(h.eng)
	ctx := context.Background()
	lease := mustAcquire(t, h, "r1", "alice", time.Minute)
	req := CommitRequest{
		RowID: "r1", Holder: "alice", LeaseVersion: lease.LeaseVersion,
		BaseRowVersion: 1, Content: "parallel", RequestID: "idem-parallel",
	}
	errs := make(chan error, 2)
	results := make(chan *CommitResponse, 2)
	run := func() {
		resp, err := svc.Commit(ctx, req)
		errs <- err
		results <- resp
	}
	go run()
	go run()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("duplicate failed: %v", err)
		}
	}
	first := <-results
	second := <-results
	if first.RowVersion != second.RowVersion {
		t.Fatal("duplicates returned different versions")
	}
	latest, _ := h.eng.latestCommitted("r1")
	if latest.RowVersion != 2 {
		t.Fatalf("expected exactly one new version, got %d", latest.RowVersion)
	}
}

func TestRecoveryRollsBackPreparedGrantAndCommit(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	before, _ := h.st.CommitVer()

	// Prepared grant without completed intent + prepared commit with an orphan
	// row version, exactly as a SIGKILL would leave them.
	if _, err := h.eng.InjectPreparedAcquire("r1", "zombie", time.Minute.Milliseconds()); err != nil {
		t.Fatalf("inject acquire: %v", err)
	}
	if _, err := h.eng.InjectPreparedCommit("r2", "zombie", 99, 1, "orphan"); err != nil {
		t.Fatalf("inject commit: %v", err)
	}

	// Reopen the engine against the same store and run recovery.
	eng2 := New(Config{Store: h.st, Clock: h.clk, Sleeper: h.clk})
	report, err := eng2.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	kinds := map[string]string{}
	for _, a := range report.Actions {
		kinds[a.Kind] = a.Action
	}
	if kinds["acquire"] != "rollback_acquire" {
		t.Fatalf("acquire not rolled back: %+v", kinds)
	}
	if kinds["commit"] != "discard_commit" {
		t.Fatalf("commit not discarded: %+v", kinds)
	}

	// The previously zombie-held row is immediately grantable.
	grant, err := eng2.Acquire(ctx, AcquireRequest{RowID: "r1", Holder: "survivor", TTLMs: time.Minute.Milliseconds()})
	if err != nil {
		t.Fatalf("row not grantable after recovery: %v", err)
	}
	if grant.LeaseVersion != 2 {
		t.Fatalf("expected preempted lease version 2, got %d", grant.LeaseVersion)
	}

	// The orphan version is gone and the global commit version rewound.
	latest, err := eng2.latestCommitted("r2")
	if err != nil || latest.Content != "r2-content" || latest.RowVersion != 1 {
		t.Fatalf("orphan version survived: %+v err=%v", latest, err)
	}
	after, _ := h.st.CommitVer()
	if after != before {
		t.Fatalf("commit_ver not rewound: before=%d after=%d", before, after)
	}

	// Recovery is idempotent: a second run finds no prepared intents.
	report2, err := eng2.Recover(ctx)
	if err != nil || report2.PreparedIntents != 0 {
		t.Fatalf("second recovery not idempotent: %+v err=%v", report2, err)
	}
}

func TestPrecheckBudgetAndReconcileDiff(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.eng.PanelOpen(ctx, PanelOpenRequest{RowID: "r1", Holder: "alice"}); err != nil {
		t.Fatalf("panel open: %v", err)
	}
	lease := mustAcquire(t, h, "r1", "alice", time.Minute)
	pre, err := h.eng.Precheck(ctx, PrecheckRequest{
		RowID: "r1", Holder: "alice", LeaseVersion: lease.LeaseVersion,
		BudgetMs: 200, AllowRefresh: true,
	})
	if err != nil {
		t.Fatalf("precheck: %v", err)
	}
	if !pre.Completed || !pre.Editable || pre.Mode == "" {
		t.Fatalf("unexpected precheck: %+v", pre)
	}

	// Zero budget must return the incomplete marker rather than a verdict.
	inc, err := h.eng.Precheck(ctx, PrecheckRequest{RowID: "r1", Holder: "alice", BudgetMs: 0})
	if err != nil {
		t.Fatalf("incomplete precheck should not be an error return: %v", err)
	}
	if inc.Completed || inc.IncompleteCode != string(CodePrecheckIncomplete) {
		t.Fatalf("expected incomplete marker, got %+v", inc)
	}

	// Reconcile after preemption gives an itemized diff.
	if err := h.eng.Release(ctx, ReleaseRequest{RowID: "r1", Holder: "alice", LeaseVersion: lease.LeaseVersion}); err != nil {
		t.Fatal(err)
	}
	newLease := mustAcquire(t, h, "r1", "bob", time.Minute)
	verdict, err := h.eng.Reconcile(ctx, ReconcileRequest{
		RowID: "r1", Holder: "alice", LeaseVersion: lease.LeaseVersion, BaseRowVersion: 1,
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if verdict.Pass || verdict.FailureCode != string(CodeLeaseLost) {
		t.Fatalf("unexpected verdict: %+v", verdict)
	}
	if verdict.Diff.ActualHolder != "bob" || verdict.Diff.ActualLeaseVer != newLease.LeaseVersion {
		t.Fatalf("diff missing actual lease facts: %+v", verdict.Diff)
	}
	if verdict.Diff.ActualRowVersion != 1 || verdict.Diff.ExpectedRowVersion != 1 {
		t.Fatalf("row version fields wrong: %+v", verdict.Diff)
	}
}

// TestCrossProcessIdempotency opens two independent engines against the same
// SQLite file and proves durable leader election prevents double execution.
func TestCrossProcessIdempotency(t *testing.T) {
	h := newHarness(t)
	path := h.st.Path
	st2, err := storeOpen(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	eng2 := New(Config{Store: st2, Clock: h.clk, Sleeper: h.clk})
	svc1 := NewIdempotentService(h.eng)
	svc2 := NewIdempotentService(eng2)

	lease := mustAcquire(t, h, "r1", "alice", time.Minute)
	req := CommitRequest{
		RowID: "r1", Holder: "alice", LeaseVersion: lease.LeaseVersion,
		BaseRowVersion: 1, Content: "cross-process", RequestID: "xproc-1",
	}
	type outcome struct {
		resp *CommitResponse
		err  error
	}
	out := make(chan outcome, 2)
	commitWithRetry := func(svc *IdempotentService) {
		for attempt := 0; attempt < 5; attempt++ {
			r, e := svc.Commit(context.Background(), req)
			if e == nil || !isBusy(e) {
				out <- outcome{r, e}
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		out <- outcome{nil, errors.New("database stayed busy")}
	}
	go commitWithRetry(svc1)
	go commitWithRetry(svc2)
	for i := 0; i < 2; i++ {
		o := <-out
		if o.err != nil {
			t.Fatalf("cross-process commit failed: %v", o.err)
		}
		if o.resp.RowVersion != 2 {
			t.Fatalf("unexpected version %+v", o.resp)
		}
	}
	latest, _ := h.eng.latestCommitted("r1")
	if latest.RowVersion != 2 {
		t.Fatalf("double execution across engines, latest=%d", latest.RowVersion)
	}
}

func isBusy(err error) bool {
	return err != nil && strings.Contains(err.Error(), "SQLITE_BUSY")
}
