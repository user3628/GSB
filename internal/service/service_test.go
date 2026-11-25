package service

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"gpusched/internal/domain"
	"gpusched/internal/store"
)

func newTestApp(t *testing.T) (*App, *domain.NormalizedSet) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	spec := domain.LoadSpec{
		Groups: []domain.GroupSpec{
			{ID: "g1", MemoryLimit: "20Gi", ComputeLimit: "1000m"},
		},
		Nodes: []domain.NodeSpec{
			{ID: "n1", GroupID: "g1", MemoryTotal: "10Gi", ComputeTotal: "500m", Health: "HEALTHY"},
			{ID: "n2", GroupID: "g1", MemoryTotal: "10Gi", ComputeTotal: "500m", Health: "HEALTHY"},
		},
		Requests: []domain.RequestSpec{
			{ID: "r1", ModelID: "m1", GroupID: "g1", MemoryNeed: "6Gi", ComputeNeed: "300m",
				SamplesPerBatch: 16, LatencyLimitMs: 200, Priority: 0},
			{ID: "r2", ModelID: "m1", GroupID: "g1", MemoryNeed: "6Gi", ComputeNeed: "300m",
				SamplesPerBatch: 16, LatencyLimitMs: 200, Priority: 1},
			{ID: "r3", ModelID: "m1", GroupID: "g1", MemoryNeed: "6Gi", ComputeNeed: "300m",
				SamplesPerBatch: 16, LatencyLimitMs: 200, Priority: 2},
			{ID: "r4", ModelID: "m1", GroupID: "g1", MemoryNeed: "6Gi", ComputeNeed: "300m",
				SamplesPerBatch: 16, LatencyLimitMs: 200, Priority: 2},
		},
		Snapshots: []domain.PolicySnapshotSpec{
			{Version: 1, Params: domain.PolicyParams{
				GlobalMaxConcurrent: 2, WaitQueueCapacity: 1, RoundBudgetMs: 100000,
				LatencyBaseMs: 5, LatencyPerSampleMs: 1,
				MaxSamplesPerModel: map[string]int64{"m1": 128},
			}},
		},
	}
	set, err := domain.Normalize(spec)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	app := New(st)
	if err := app.Load(context.Background(), set); err != nil {
		t.Fatalf("load: %v", err)
	}
	return app, set
}

func TestAdmitUpdatesReservationAndNodeAtomically(t *testing.T) {
	app, _ := newTestApp(t)
	ctx := context.Background()

	res, err := app.AdmitOne(ctx, "r1")
	if err != nil {
		t.Fatalf("admit r1: %v", err)
	}
	if res.State != domain.StateAdmitted || res.NodeID == "" {
		t.Fatalf("bad admit result: %+v", res)
	}

	report, err := app.QueryQuota(ctx)
	if err != nil {
		t.Fatalf("quota: %v", err)
	}
	g := report.Groups[0]
	want := int64(6 * 1024 * 1024 * 1024)
	if g.MemoryReserved != want {
		t.Errorf("group memory reserved=%d want %d", g.MemoryReserved, want)
	}
	if g.ComputeReservedM != 300 {
		t.Errorf("group compute reserved=%d want 300", g.ComputeReservedM)
	}
	var nodeUsed int64
	for _, n := range g.Nodes {
		nodeUsed += n.MemoryUsed
	}
	if nodeUsed != want {
		t.Errorf("aggregate node used=%d want %d", nodeUsed, want)
	}
}

func TestQuotaExceededCarriesShortfall(t *testing.T) {
	app, _ := newTestApp(t)
	ctx := context.Background()
	// Two 10Gi nodes host one 6Gi request each (12Gi reserved). The third
	// admission hits the group memory limit (20Gi would become 18Gi but no node
	// has room); either way the error code and shortfall are required.
	for _, id := range []string{"r1", "r2"} {
		if _, err := app.AdmitOne(ctx, id); err != nil {
			t.Fatalf("admit %s: %v", id, err)
		}
	}
	_, err := app.AdmitOne(ctx, "r3")
	be, ok := domain.AsBizError(err)
	if !ok || be.Code != domain.EQuotaExceeded {
		t.Fatalf("expected E_QUOTA_EXCEEDED got %v", err)
	}
	detail, _ := be.Details["shortfall"].(domain.QuotaExceedDetail)
	if detail.Shortfall <= 0 || detail.Requested == 0 || detail.Limit == 0 {
		t.Errorf("shortfall detail incomplete: %+v", detail)
	}

	// The rejected request must remain PENDING and reservation unchanged.
	req, _ := app.GetRequest(ctx, "r3")
	if req.State != domain.StatePending {
		t.Errorf("rejected request state=%s want PENDING", req.State)
	}
	report, _ := app.QueryQuota(ctx)
	two := int64(12 * 1024 * 1024 * 1024)
	if report.Groups[0].MemoryReserved != two {
		t.Errorf("reservation changed after rejection: %d", report.Groups[0].MemoryReserved)
	}
}

func TestPreemptReleasesQuotaInOneTransaction(t *testing.T) {
	app, _ := newTestApp(t)
	ctx := context.Background()
	if _, err := app.AdmitOne(ctx, "r1"); err != nil {
		t.Fatalf("admit: %v", err)
	}
	before, _ := app.QueryQuota(ctx)
	beforeMem := before.Groups[0].MemoryReserved

	res, err := app.Preempt(ctx, "r1")
	if err != nil {
		t.Fatalf("preempt: %v", err)
	}
	if res.ToState != domain.StatePreempted || res.FreedMemory == 0 {
		t.Fatalf("bad preempt result: %+v", res)
	}
	after, _ := app.QueryQuota(ctx)
	if after.Groups[0].MemoryReserved != beforeMem-res.FreedMemory {
		t.Errorf("quota not released: before=%d after=%d freed=%d",
			beforeMem, after.Groups[0].MemoryReserved, res.FreedMemory)
	}
	// Freed capacity is immediately reusable for another admission.
	if _, err := app.AdmitOne(ctx, "r2"); err != nil {
		t.Fatalf("re-admit after preempt: %v", err)
	}
	req, _ := app.GetRequest(ctx, "r1")
	if req.State != domain.StatePreempted {
		t.Errorf("victim state=%s want PREEMPTED", req.State)
	}
}

func TestInvalidTransitionLeavesStateUntouched(t *testing.T) {
	app, _ := newTestApp(t)
	ctx := context.Background()
	if _, err := app.AdmitOne(ctx, "r1"); err != nil {
		t.Fatalf("admit: %v", err)
	}
	// ADMITTED -> RUNNING is illegal (must pass through BATCHED).
	_, err := app.Transition(ctx, "r1", domain.StateRunning)
	if be, ok := domain.AsBizError(err); !ok || be.Code != domain.EInvalidTransition {
		t.Fatalf("expected E_INVALID_TRANSITION got %v", err)
	}
	req, _ := app.GetRequest(ctx, "r1")
	if req.State != domain.StateAdmitted {
		t.Errorf("state changed by illegal transition: %s", req.State)
	}
}

func TestIdempotentTransition(t *testing.T) {
	app, _ := newTestApp(t)
	ctx := context.Background()
	if _, err := app.AdmitOne(ctx, "r1"); err != nil {
		t.Fatalf("admit: %v", err)
	}
	first, err := app.Transition(ctx, "r1", domain.StateRejected)
	if err != nil || !first.Applied {
		t.Fatalf("first transition: %+v err=%v", first, err)
	}
	second, err := app.Transition(ctx, "r1", domain.StateRejected)
	if err != nil || second.Applied {
		t.Fatalf("repeat transition must be idempotent no-op: %+v err=%v", second, err)
	}
}

func TestConcurrentReadersSeeCommittedVersion(t *testing.T) {
	app, _ := newTestApp(t)
	ctx := context.Background()
	if _, err := app.AdmitOne(ctx, "r1"); err != nil {
		t.Fatalf("admit: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				report, err := app.QueryQuota(ctx)
				if err != nil {
					t.Errorf("quota read: %v", err)
					return
				}
				g := report.Groups[0]
				nodeMem := int64(0)
				nodeComp := int64(0)
				for _, n := range g.Nodes {
					nodeMem += n.MemoryUsed
					nodeComp += n.ComputeUsedM
				}
				if nodeMem != g.MemoryReserved || nodeComp != g.ComputeReservedM {
					t.Errorf("inconsistent read: node=%d/%d group=%d/%d",
						nodeMem, nodeComp, g.MemoryReserved, g.ComputeReservedM)
					return
				}
			}
		}()
	}
	wg.Wait()
}
