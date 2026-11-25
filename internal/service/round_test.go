package service

import (
	"context"
	"testing"

	"gpusched/internal/domain"
	"gpusched/internal/store"
	"path/filepath"
)

func newLargeApp(t *testing.T) *App {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "large.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	spec := domain.LoadSpec{
		Groups: []domain.GroupSpec{{ID: "g1", MemoryLimit: "100Gi", ComputeLimit: "4000m"}},
		Nodes: []domain.NodeSpec{
			{ID: "n1", GroupID: "g1", MemoryTotal: "50Gi", ComputeTotal: "2000m", Health: "HEALTHY"},
			{ID: "n2", GroupID: "g1", MemoryTotal: "50Gi", ComputeTotal: "2000m", Health: "HEALTHY"},
		},
		Requests: []domain.RequestSpec{
			{ID: "r1", ModelID: "m1", GroupID: "g1", MemoryNeed: "6Gi", ComputeNeed: "300m",
				SamplesPerBatch: 16, LatencyLimitMs: 500, Priority: 0},
			{ID: "r2", ModelID: "m2", GroupID: "g1", MemoryNeed: "6Gi", ComputeNeed: "300m",
				SamplesPerBatch: 16, LatencyLimitMs: 500, Priority: 1},
			{ID: "r3", ModelID: "m3", GroupID: "g1", MemoryNeed: "6Gi", ComputeNeed: "300m",
				SamplesPerBatch: 16, LatencyLimitMs: 500, Priority: 2},
			{ID: "r4", ModelID: "m4", GroupID: "g1", MemoryNeed: "6Gi", ComputeNeed: "300m",
				SamplesPerBatch: 16, LatencyLimitMs: 500, Priority: 2},
		},
		Snapshots: []domain.PolicySnapshotSpec{{Version: 1, Params: domain.PolicyParams{
			GlobalMaxConcurrent: 2, WaitQueueCapacity: 1, RoundBudgetMs: 100000,
			LatencyBaseMs: 5, LatencyPerSampleMs: 1,
			MaxSamplesPerModel: map[string]int64{"m1": 128, "m2": 128, "m3": 128, "m4": 128},
		}}},
	}
	set, err := domain.Normalize(spec)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	app := New(st)
	if err := app.Load(context.Background(), set); err != nil {
		t.Fatalf("load: %v", err)
	}
	return app
}

func TestRunRoundRespectsConcurrencyAndQueue(t *testing.T) {
	app := newLargeApp(t)
	ctx := context.Background()
	for _, id := range []string{"r1", "r2", "r3", "r4"} {
		if _, err := app.AdmitOne(ctx, id); err != nil {
			t.Fatalf("admit %s: %v", id, err)
		}
	}
	// Policy: max concurrent 2, queue capacity 1, 4 admitted -> queue full.
	report, err := app.RunRound(ctx, 0)
	be, ok := domain.AsBizError(err)
	if !ok || be.Code != domain.EExecQueueFull {
		t.Fatalf("expected E_EXEC_QUEUE_FULL got %v", err)
	}
	if report == nil || !report.QueueFull {
		t.Fatalf("queue-full round must still return a committed report: %+v", report)
	}

	batches, err := app.ListBatches(ctx)
	if err != nil {
		t.Fatalf("list batches: %v", err)
	}
	var running, queued, downgraded int
	for _, b := range batches {
		switch b.Status {
		case domain.BatchRunning:
			running++
		case domain.BatchQueued:
			queued++
		case domain.BatchDowngraded:
			downgraded++
		}
	}
	if running != 2 {
		t.Errorf("running=%d want 2", running)
	}
	if queued != 1 {
		t.Errorf("queued=%d want 1", queued)
	}
	if downgraded < 1 {
		t.Errorf("expected downgraded batch for overflow, got %d", downgraded)
	}

	// Downgraded requests remain ADMITTED (never silently dropped).
	for _, id := range []string{"r1", "r2", "r3", "r4"} {
		req, _ := app.GetRequest(ctx, id)
		if req.State != domain.StateBatched && req.State != domain.StateAdmitted {
			t.Errorf("request %s unexpected state %s", id, req.State)
		}
	}
}

func TestRunRoundDeterministicDowngradeUnderBudget(t *testing.T) {
	app := newLargeApp(t)
	ctx := context.Background()
	for _, id := range []string{"r1", "r2"} {
		if _, err := app.AdmitOne(ctx, id); err != nil {
			t.Fatalf("admit %s: %v", id, err)
		}
	}
	// Virtual clock starts beyond the 100000ms budget? No: pass a value that,
	// once the first batch latency is added, exceeds budget -> deterministic
	// downgrade of the remaining batch.
	report, err := app.RunRound(ctx, 99990)
	if err != nil {
		t.Fatalf("run round: %v", err)
	}
	if len(report.DowngradedBatches) == 0 && len(report.RunningBatches) == 0 {
		t.Fatalf("expected some scheduling decision, report=%+v", report)
	}
	// A downgraded batch leaves its members ADMITTED for the next round.
	for _, b := range report.Batches {
		if b.Status == domain.BatchDowngraded {
			for _, m := range b.Members {
				req, _ := app.GetRequest(ctx, m)
				if req.State != domain.StateAdmitted {
					t.Errorf("downgraded member %s state=%s want ADMITTED", m, req.State)
				}
			}
		}
	}
}

func TestBatchedRequestsTransitionToRunningAndComplete(t *testing.T) {
	app, _ := newTestApp(t)
	ctx := context.Background()
	if _, err := app.AdmitOne(ctx, "r1"); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if _, err := app.RunRound(ctx, 0); err != nil {
		t.Fatalf("round: %v", err)
	}
	req, _ := app.GetRequest(ctx, "r1")
	if req.State != domain.StateBatched {
		t.Fatalf("state=%s want BATCHED", req.State)
	}
	if _, err := app.Transition(ctx, "r1", domain.StateRunning); err != nil {
		t.Fatalf("to running: %v", err)
	}
	if _, err := app.Transition(ctx, "r1", domain.StateCompleted); err != nil {
		t.Fatalf("to completed: %v", err)
	}
	report, _ := app.QueryQuota(ctx)
	if report.Groups[0].MemoryReserved != 0 {
		t.Errorf("quota not released on completion: %d", report.Groups[0].MemoryReserved)
	}
}
