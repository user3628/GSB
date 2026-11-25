package engine

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"nonceguard/internal/model"
	"nonceguard/internal/sample"
	"nonceguard/internal/store"
)

func newTestEngine(t *testing.T) (*Engine, *store.Store) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "nonceguard-test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng := New(st)
	if _, err := eng.EnsureDefaultConfig(context.Background(), sample.DemoReferenceMs); err != nil {
		t.Fatalf("default config: %v", err)
	}
	return eng, st
}

func TestParseRejectsInvalidNonceWeightOverflowAndDoesNotNeedStore(t *testing.T) {
	batch, _, perr := model.ParseBatch(sample.InvalidBatchJSON())
	if batch != nil {
		t.Fatalf("invalid batch must not parse")
	}
	if perr == nil || perr.Code != model.CodeRequestInvalid {
		t.Fatalf("expected E_REQUEST_INVALID, got %#v", perr)
	}
	if len(perr.Issues) < 2 {
		t.Fatalf("expected nonce and overflow issues, got %#v", perr.Issues)
	}
}

func TestCanonicalBatchDecisionCodes(t *testing.T) {
	eng, _ := newTestEngine(t)
	ctx := context.Background()
	batch := sample.CanonicalBatch(sample.DemoReferenceMs)
	res, failure := eng.AdjudicateBatch(ctx, batch, AdjudicateOptions{ReferenceMs: sample.DemoReferenceMs})
	if failure != nil {
		t.Fatalf("adjudicate: %s %v", failure.Code, failure.Context)
	}
	got := make([]string, len(res.Results))
	for i, item := range res.Results {
		got[i] = item.Code
		if item.ConfigVersion != 1 {
			t.Fatalf("item %d config version = %d, want 1", i, item.ConfigVersion)
		}
	}
	want := []string{
		model.CodeOK, model.CodeOK, model.CodeSignatureMismatch,
		model.CodeTimestampEarly, model.CodeTimestampLate, model.CodeNonceReplayed,
		model.CodeOK, model.CodeOK, model.CodeConflictingRequest, model.CodeOK,
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("code %d = %s, want %s (all=%v)", i, got[i], want[i], got)
		}
	}
	if res.Results[3].OffsetMs == nil || *res.Results[3].OffsetMs != -300001 || res.Results[3].BoundaryMs == nil || *res.Results[3].BoundaryMs != -300000 {
		t.Fatalf("early context wrong: %#v", res.Results[3])
	}
	if res.Results[4].OffsetMs == nil || *res.Results[4].OffsetMs != 300001 || *res.Results[4].BoundaryMs != 300000 {
		t.Fatalf("late context wrong: %#v", res.Results[4])
	}
	if rem := res.Results[5].RemainingWindowMs; rem == nil || *rem != 600000 {
		t.Fatalf("replay remaining = %v, want 600000", rem)
	}
	if !res.Results[7].IdempotentReuse {
		t.Fatalf("identical repeated request_id must be marked idempotent reuse")
	}
}

func TestNonceWindowReplayBoundaryAndRecycleBasis(t *testing.T) {
	ctx := context.Background()
	ref := sample.DemoReferenceMs

	t.Run("active replay reports persisted first use and remaining", func(t *testing.T) {
		eng, _ := newTestEngine(t)
		first, active, _, _ := sample.RecycleBatches(ref)
		if _, f := eng.AdjudicateBatch(ctx, first, AdjudicateOptions{}); f != nil {
			t.Fatal(f)
		}
		res, f := eng.AdjudicateBatch(ctx, active, AdjudicateOptions{})
		if f != nil {
			t.Fatal(f)
		}
		item := res.Results[0]
		if item.Code != model.CodeNonceReplayed || item.FirstSeenMs == nil || *item.FirstSeenMs != ref {
			t.Fatalf("unexpected replay: %#v", item)
		}
		if item.RemainingWindowMs == nil || *item.RemainingWindowMs != 540000 {
			t.Fatalf("remaining = %v, want 540000", item.RemainingWindowMs)
		}
	})

	t.Run("right endpoint is exclusive and recyclable at expiry", func(t *testing.T) {
		eng, _ := newTestEngine(t)
		first, _, boundary, _ := sample.RecycleBatches(ref)
		if _, f := eng.AdjudicateBatch(ctx, first, AdjudicateOptions{}); f != nil {
			t.Fatal(f)
		}
		res, f := eng.AdjudicateBatch(ctx, boundary, AdjudicateOptions{})
		if f != nil {
			t.Fatal(f)
		}
		item := res.Results[0]
		if item.Code != model.CodeOK || !item.Recycled {
			t.Fatalf("exact expiry should recycle and accept: %#v", item)
		}
	})

	t.Run("after expiry deletes based on persisted first_seen", func(t *testing.T) {
		eng, _ := newTestEngine(t)
		first, _, _, expired := sample.RecycleBatches(ref)
		if _, f := eng.AdjudicateBatch(ctx, first, AdjudicateOptions{}); f != nil {
			t.Fatal(f)
		}
		res, f := eng.AdjudicateBatch(ctx, expired, AdjudicateOptions{})
		if f != nil {
			t.Fatal(f)
		}
		if got := res.Results[0]; got.Code != model.CodeOK || !got.Recycled {
			t.Fatalf("expired nonce should recycle: %#v", got)
		}
	})
}

func TestConfigVersionRulesAndAtomicUsability(t *testing.T) {
	ctx := context.Background()
	eng, _ := newTestEngine(t)
	ref := sample.DemoReferenceMs

	v2 := sample.ConfigVersionTwo(ref + 1)
	saved, f := eng.SubmitConfig(ctx, v2)
	if f != nil {
		t.Fatalf("submit v2: %#v", f)
	}
	if saved.Version != 2 {
		t.Fatalf("saved version %d", saved.Version)
	}
	if _, f := eng.SubmitConfig(ctx, sample.ConfigVersionTwo(ref+2)); f == nil || f.Code != model.CodeStaleSnapshot {
		t.Fatalf("expected stale, got %#v", f)
	}
	if _, f := eng.SubmitConfig(ctx, sample.GappedConfig(v2)); f == nil || f.Code != model.CodeConfigVersionGap {
		t.Fatalf("expected gap, got %#v", f)
	}
	bad := model.DefaultSnapshot(3, ref+3)
	bad.MinOffsetMs = -1
	if _, f := eng.SubmitConfig(ctx, bad); f == nil || f.Code != model.CodeSnapshotDigest {
		t.Fatalf("expected digest invalid, got %#v", f)
	}
	active, err := eng.ActiveConfig(ctx)
	if err != nil || active.Version != 2 || active.MinOffsetMs != v2.MinOffsetMs {
		t.Fatalf("old/new active config should remain v2: %#v err=%v", active, err)
	}

	batch := sample.CanonicalBatch(ref)
	res, f := eng.AdjudicateBatch(ctx, batch, AdjudicateOptions{})
	if f != nil {
		t.Fatal(f)
	}
	for _, item := range res.Results {
		if item.ConfigVersion != 2 {
			t.Fatalf("item used version %d, want 2", item.ConfigVersion)
		}
	}
}

func TestFastCheckDoesNotPersistAndComparisonMatchesSelected(t *testing.T) {
	ctx := context.Background()
	eng, st := newTestEngine(t)
	batch := sample.CanonicalBatch(sample.DemoReferenceMs)
	fast, f := eng.FastCheck(ctx, batch, FastOptions{PrecheckLimit: 6})
	if f != nil {
		t.Fatal(f)
	}
	if fast.Sampling.SampleRate != 0.6 || len(fast.Sampling.Selection) != 6 {
		t.Fatalf("bad sampling: %#v", fast.Sampling)
	}
	if fast.Results[6].Code != model.CodeNotPrechecked || fast.Results[6].Accepted {
		t.Fatalf("unchecked item must be explicit not-prechecked: %#v", fast.Results[6])
	}
	occ, err := eng.ListOccupancy(ctx, 0, "", 100)
	if err != nil || len(occ) != 0 {
		t.Fatalf("fast mode must not write occupancy: occ=%v err=%v", occ, err)
	}
	full, f := eng.AdjudicateBatch(ctx, batch, AdjudicateOptions{})
	if f != nil {
		t.Fatal(f)
	}
	comparison := CompareResults(fast, full)
	if comparison.Differences != 0 || comparison.Comparable != 6 || comparison.NotPrechecked != 4 {
		t.Fatalf("bad comparison: %#v", comparison)
	}
	fastAfter, f := eng.FastCheck(ctx, batch, FastOptions{PrecheckLimit: 6})
	if f != nil {
		t.Fatal(f)
	}
	afterComparison := CompareResults(fastAfter, full)
	if afterComparison.Differences != 0 {
		t.Fatalf("repeated comparison must reuse committed decisions: %#v", afterComparison.Rows)
	}
	if _, err := st.LatestRecovery(ctx); err == nil {
		t.Fatalf("no recovery report should exist yet")
	}
}

func TestConcurrentIdenticalBatchAdjudicatesOnce(t *testing.T) {
	ctx := context.Background()
	eng, _ := newTestEngine(t)
	batch := sample.ValidBatch("concurrent-fixed", 24, sample.DemoReferenceMs)
	const workers = 8
	var wg sync.WaitGroup
	failures := make(chan *Failure, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, f := eng.AdjudicateBatch(ctx, batch, AdjudicateOptions{})
			if f != nil {
				failures <- f
				return
			}
			if len(res.Results) != len(batch.Requests) {
				failures <- NewFailure("TEST", "short result", nil)
			}
		}()
	}
	wg.Wait()
	close(failures)
	for f := range failures {
		t.Fatalf("concurrent batch failure: %#v", f)
	}
	decisions, err := eng.ListDecisions(ctx, 0, "", "", 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(decisions) != len(batch.Requests) {
		t.Fatalf("decisions=%d want %d", len(decisions), len(batch.Requests))
	}
}

func TestBatchInterruptRecoveryAndReopen(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	eng := New(st)
	if _, err := eng.EnsureDefaultConfig(ctx, sample.DemoReferenceMs); err != nil {
		t.Fatal(err)
	}
	batch := sample.ValidBatch("interrupt-me", 12, sample.DemoReferenceMs)
	_, f := eng.AdjudicateBatch(ctx, batch, AdjudicateOptions{Fault: "batch_interrupt"})
	if f == nil || f.Code != model.CodeBatchInterrupted {
		t.Fatalf("expected interruption, got %#v", f)
	}
	report, err := eng.RunRecovery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.OpenIntentsFound != 12 || report.AbortedIntents != 12 || report.AbortedBatches != 1 {
		t.Fatalf("bad recovery report: %#v", report)
	}
	view, err := eng.GetBatch(ctx, batch.BatchID)
	if err != nil || view.Status != "interrupted_recovered" {
		t.Fatalf("batch status view=%#v err=%v", view, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	report2, err := st2.Recover(ctx, sample.DemoReferenceMs+999999)
	if err != nil {
		t.Fatal(err)
	}
	if report2.OpenIntentsFound != 0 {
		t.Fatalf("reopened DB should have no open intents, got %d", report2.OpenIntentsFound)
	}
}

func TestFailureTypeIsDistinguishable(t *testing.T) {
	f := NewFailure(model.CodePeerTimeout, "x", nil)
	var target *Failure
	if !errors.As(f, &target) || target.Code != model.CodePeerTimeout {
		t.Fatalf("failure should support errors.As")
	}
}
