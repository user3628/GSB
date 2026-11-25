package registry_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	. "registrylab/registry"
	"registrylab/sample"
	"registrylab/testfixture"
)

func openTestEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(filepath.Join(t.TempDir(), "lab.db"))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func fixtureSample(t *testing.T, version int64) SnapshotRequest {
	f, err := testfixture.Start()
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	t.Cleanup(f.Close)
	return sample.Generate(sample.Options{Seed: 42, Version: version, BaseURL: f.BaseURL,
		TCPAddr: f.TCPAddr, RefusedAddr: f.RefusedAddr})
}

func TestSnapshotStaleValidationAndDeltaBase(t *testing.T) {
	e := openTestEngine(t)
	ctx := context.Background()
	snap := fixtureSample(t, 1)
	res, opErr := e.SubmitSnapshot(ctx, snap)
	if opErr != nil {
		t.Fatalf("snapshot: %v", opErr)
	}
	if res.Version != 1 || res.InstanceCount != 9 || res.DerivedRevision != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}

	stale := snap
	stale.Digest = ComputeDigest(stale.Version, stale.GeneratedAt, stale.Instances)
	if _, opErr = e.SubmitSnapshot(ctx, stale); opErr == nil || opErr.Code != ErrStaleSnapshot {
		t.Fatalf("want stale snapshot, got %v", opErr)
	}
	bad := snap
	bad.Version = 2
	bad.GeneratedAt = "2026-11-26T00:00:01Z"
	bad.Digest = "deadbeef"
	if _, opErr = e.SubmitSnapshot(ctx, bad); opErr == nil || opErr.Code != ErrValidation {
		t.Fatalf("want validation, got %v", opErr)
	}
	cur, opErr := e.Current(ctx)
	if opErr != nil || cur.Version != 1 || len(cur.Instances) != 9 {
		t.Fatalf("old snapshot not intact: %+v %v", cur, opErr)
	}

	delta := sample.DeltaExample(1, 2, snap.Instances[0].Endpoint)
	dr, opErr := e.SubmitDelta(ctx, delta)
	if opErr != nil {
		t.Fatalf("delta: %v", opErr)
	}
	if dr.Version != 2 {
		t.Fatalf("delta version=%d", dr.Version)
	}
	delta.RequestID = "delta-stale"
	delta.Version = 3
	if _, opErr = e.SubmitDelta(ctx, delta); opErr == nil || opErr.Code != ErrStaleBase {
		t.Fatalf("want stale base, got %v", opErr)
	}
	cur, _ = e.Current(ctx)
	if cur.Version != 2 {
		t.Fatalf("stale delta changed version: %d", cur.Version)
	}
}

func TestDeltaCanBootstrapFromEmptyVersion(t *testing.T) {
	e := openTestEngine(t)
	ctx := context.Background()
	snap := fixtureSample(t, 1)
	req := DeltaRequest{BaseVersion: 0, Version: 1, Upserts: []Instance{snap.Instances[0]}, RequestID: "bootstrap-delta"}
	result, opErr := e.SubmitDelta(ctx, req)
	if opErr != nil {
		t.Fatalf("bootstrap delta: %v", opErr)
	}
	if result.Version != 1 || result.InstanceCount != 1 {
		t.Fatalf("bootstrap result: %+v", result)
	}
	cur, opErr := e.Current(ctx)
	if opErr != nil || cur.Version != 1 || len(cur.Instances) != 1 {
		t.Fatalf("bootstrap current: %+v %v", cur, opErr)
	}
}

func TestConcurrentDeltasOnlyOneSameBase(t *testing.T) {
	e := openTestEngine(t)
	ctx := context.Background()
	snap := fixtureSample(t, 1)
	if _, opErr := e.SubmitSnapshot(ctx, snap); opErr != nil {
		t.Fatal(opErr)
	}
	base := sample.DeltaExample(1, 2, snap.Instances[0].Endpoint)
	base.Upserts[0].Weight = 500
	var wg sync.WaitGroup
	errs := make([]*Error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := base
			req.RequestID = fmt.Sprintf("concurrent-%d", i)
			req.Version = int64(2 + i)
			req.Upserts = append([]Instance(nil), base.Upserts...)
			req.Upserts[0].Weight += i
			_, errs[i] = e.SubmitDelta(ctx, req)
		}(i)
	}
	wg.Wait()
	success, stale := 0, 0
	for _, err := range errs {
		if err == nil {
			success++
		} else if err.Code == ErrStaleBase {
			stale++
		} else {
			t.Fatalf("unexpected delta error: %v", err)
		}
	}
	if success != 1 || stale != 1 {
		t.Fatalf("success=%d stale=%d", success, stale)
	}
	cur, _ := e.Current(ctx)
	if len(cur.Instances) != 9 {
		t.Fatalf("partial delta write: %d instances", len(cur.Instances))
	}
}

func TestRealProbesDerivedAndStateMachine(t *testing.T) {
	e := openTestEngine(t)
	ctx := context.Background()
	snap := fixtureSample(t, 1)
	if _, opErr := e.SubmitSnapshot(ctx, snap); opErr != nil {
		t.Fatal(opErr)
	}
	if _, opErr := e.Heartbeat(ctx, "billing", "billing-lease-1", HeartbeatRequest{RequestID: "hb-1"}); opErr != nil {
		t.Fatalf("heartbeat: %v", opErr)
	}
	round, opErr := e.RunProbeRound(ctx, RoundOptions{RequestID: "round-1", GlobalConcurrency: 4,
		PerServiceConcurrency: 2, BudgetMS: 1500, WaitQueueCapacity: 20})
	if opErr != nil {
		t.Fatalf("round: %v", opErr)
	}
	if round.Started != 9 || len(round.Outcomes) != 9 {
		t.Fatalf("round did not start all: %+v", round)
	}
	byID := make(map[string]ProbeOutcome)
	for _, outcome := range round.Outcomes {
		byID[outcome.InstanceID] = outcome
	}
	if !byID["catalog-http-ok"].OK {
		t.Fatalf("ok HTTP probe: %+v", byID["catalog-http-ok"])
	}
	if byID["catalog-http-slow"].Code != ErrProbeTimeout {
		t.Fatalf("slow probe code: %+v", byID["catalog-http-slow"])
	}
	if byID["catalog-bad-marker"].Code != "E_PROBE_MARKER_MISMATCH" {
		t.Fatalf("marker probe: %+v", byID["catalog-bad-marker"])
	}
	if byID["orders-tcp-ok"].Code != "E_TCP_OK" || byID["orders-tcp-refused"].OK {
		t.Fatalf("tcp probes: %+v %+v", byID["orders-tcp-ok"], byID["orders-tcp-refused"])
	}
	if !byID["billing-lease-1"].OK {
		t.Fatalf("lease probe: %+v", byID["billing-lease-1"])
	}
	if _, opErr := e.RunProbeRound(ctx, RoundOptions{RequestID: "round-2", GlobalConcurrency: 4,
		PerServiceConcurrency: 2, BudgetMS: 1500, WaitQueueCapacity: 20}); opErr != nil {
		t.Fatalf("second round: %v", opErr)
	}

	cur, _ := e.Current(ctx)
	for _, in := range cur.Instances {
		if in.InstanceID == "catalog-http-ok" && in.State != StateHealthy {
			t.Fatalf("healthy state=%s", in.State)
		}
	}
	view, opErr := e.Derived(ctx)
	if opErr != nil {
		t.Fatal(opErr)
	}
	for _, w := range view.Weights {
		if w.InstanceID == "catalog-bad-marker" && (w.Available || w.SharePPM != 0) {
			t.Fatalf("unhealthy instance retained weight: %+v", w)
		}
	}
	var shareSum int64
	for _, w := range view.Weights {
		if w.ServiceName == "catalog" {
			shareSum += w.SharePPM
		}
	}
	if shareSum != 1_000_000 {
		t.Fatalf("available shares sum=%d", shareSum)
	}

	res, opErr := e.SetState(ctx, "catalog", "catalog-http-ok", SetStateRequest{State: StateDraining, Reason: "test"})
	if opErr != nil || res.To != StateDraining {
		t.Fatalf("drain: %+v %v", res, opErr)
	}
	before, _ := e.Derived(ctx)
	_, opErr = e.SetState(ctx, "catalog", "catalog-http-ok", SetStateRequest{State: StateHealthy})
	if opErr == nil || opErr.Code != ErrInvalidTransition {
		t.Fatalf("want invalid transition, got %v", opErr)
	}
	after, _ := e.Derived(ctx)
	if before.Revision != after.Revision {
		t.Fatalf("invalid transition changed derived revision")
	}
	cur, _ = e.Current(ctx)
	for _, in := range cur.Instances {
		if in.InstanceID == "catalog-http-ok" && in.State != StateDraining {
			t.Fatalf("invalid transition persisted: %s", in.State)
		}
	}
}

func TestQueueFullAndBudgetDeferOrder(t *testing.T) {
	e := openTestEngine(t)
	ctx := context.Background()
	snap := fixtureSample(t, 1)
	if _, opErr := e.SubmitSnapshot(ctx, snap); opErr != nil {
		t.Fatal(opErr)
	}
	curBefore, _ := e.Current(ctx)
	_, opErr := e.RunProbeRound(ctx, RoundOptions{GlobalConcurrency: 1, PerServiceConcurrency: 1,
		BudgetMS: 1000, WaitQueueCapacity: 1})
	if opErr == nil || opErr.Code != ErrProbeQueueFull {
		t.Fatalf("want queue full, got %v", opErr)
	}
	curAfter, _ := e.Current(ctx)
	if curBefore.DerivedRevision != curAfter.DerivedRevision {
		t.Fatalf("queue-full changed revision")
	}
	_, opErr = e.RunProbeRound(ctx, RoundOptions{GlobalConcurrency: 1, PerServiceConcurrency: 1,
		BudgetMS: 1000, WaitQueueCapacity: 0})
	if opErr == nil || opErr.Code != ErrProbeQueueFull {
		t.Fatalf("want queue full with zero wait capacity, got %v", opErr)
	}

	round, opErr := e.RunProbeRound(ctx, RoundOptions{GlobalConcurrency: 1, PerServiceConcurrency: 1,
		BudgetMS: 1, WaitQueueCapacity: 20})
	if opErr != nil {
		t.Fatalf("budget round: %v", opErr)
	}
	if len(round.Deferred) == 0 {
		t.Fatalf("expected deferred tasks: %+v", round)
	}
	for i := 1; i < len(round.Deferred); i++ {
		if round.Deferred[i-1].Priority < round.Deferred[i].Priority {
			t.Fatalf("deferred not priority ordered: %+v", round.Deferred)
		}
		if round.Deferred[i-1].Reason != "round_budget_exhausted" {
			t.Fatalf("missing defer rationale: %+v", round.Deferred[i-1])
		}
	}
}

func TestProbeRoundIdempotentConcurrent(t *testing.T) {
	e := openTestEngine(t)
	ctx := context.Background()
	snap := fixtureSample(t, 1)
	if _, opErr := e.SubmitSnapshot(ctx, snap); opErr != nil {
		t.Fatal(opErr)
	}
	opts := RoundOptions{RequestID: "same-round", GlobalConcurrency: 3, PerServiceConcurrency: 2,
		BudgetMS: 1000, WaitQueueCapacity: 20}
	var wg sync.WaitGroup
	results := make([]RoundResult, 2)
	errs := make([]*Error, 2)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = e.RunProbeRound(ctx, opts)
		}(i)
	}
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("errors: %v %v", errs[0], errs[1])
	}
	if results[0].RoundID != results[1].RoundID {
		t.Fatalf("duplicate request executed twice: %s vs %s", results[0].RoundID, results[1].RoundID)
	}
}
