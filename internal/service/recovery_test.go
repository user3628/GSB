package service

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"gpusched/internal/domain"
	"gpusched/internal/store"
)

// TestRecoverMarksOrphanIntentAborted seeds a durable PENDING intent (as left by
// an interrupted admission whose row changes SQLite already rolled back) and
// asserts startup recovery marks it ABORTED using only on-disk facts.
func TestRecoverMarksOrphanIntentAborted(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "intent.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	app := New(st)
	if err := app.Load(context.Background(), seededSet(t)); err != nil {
		t.Fatalf("load: %v", err)
	}

	err = st.Tx(context.Background(), func(tx *sql.Tx) error {
		_, ierr := store.InsertIntent(tx, "ADMIT", 0, "r1", "", `{"orphan":true}`)
		return ierr
	})
	if err != nil {
		t.Fatalf("seed orphan intent: %v", err)
	}
	_ = st.Close()

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	app2 := New(st2)
	report, err := app2.Recover(context.Background())
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if report.AbortedIntents < 1 {
		t.Errorf("expected at least 1 aborted intent, got %d (%+v)", report.AbortedIntents, report)
	}

	// Recovery is idempotent: a second run finds nothing pending.
	report2, err := app2.Recover(context.Background())
	if err != nil {
		t.Fatalf("second recover: %v", err)
	}
	if report2.AbortedIntents != 0 || len(report2.PendingIntents) != 0 {
		t.Errorf("recovery must be idempotent: %+v", report2)
	}
}

func seededSet(t *testing.T) *domain.NormalizedSet {
	t.Helper()
	spec := domain.LoadSpec{
		Groups: []domain.GroupSpec{
			{ID: "g1", MemoryLimit: "30Gi", ComputeLimit: "1500m"},
		},
		Nodes: []domain.NodeSpec{
			{ID: "n1", GroupID: "g1", MemoryTotal: "15Gi", ComputeTotal: "750m", Health: "HEALTHY"},
			{ID: "n2", GroupID: "g1", MemoryTotal: "15Gi", ComputeTotal: "750m", Health: "HEALTHY"},
		},
		Requests: []domain.RequestSpec{
			{ID: "r1", ModelID: "m1", GroupID: "g1", MemoryNeed: "4Gi", ComputeNeed: "200m",
				SamplesPerBatch: 16, LatencyLimitMs: 500, Priority: 0},
			{ID: "r2", ModelID: "m1", GroupID: "g1", MemoryNeed: "4Gi", ComputeNeed: "200m",
				SamplesPerBatch: 16, LatencyLimitMs: 500, Priority: 1},
			{ID: "r3", ModelID: "m1", GroupID: "g1", MemoryNeed: "4Gi", ComputeNeed: "200m",
				SamplesPerBatch: 16, LatencyLimitMs: 500, Priority: 2},
		},
		Snapshots: []domain.PolicySnapshotSpec{
			{Version: 1, Params: domain.PolicyParams{
				GlobalMaxConcurrent: 1, WaitQueueCapacity: 1, RoundBudgetMs: 100000,
				LatencyBaseMs: 5, LatencyPerSampleMs: 1,
				MaxSamplesPerModel: map[string]int64{"m1": 128},
			}},
		},
	}
	set, err := domain.Normalize(spec)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	return set
}
