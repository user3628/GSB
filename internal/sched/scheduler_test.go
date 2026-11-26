package sched

import (
	"context"
	"errors"
	"testing"
	"time"

	"logagg/internal/errs"
)

func TestPlanRoundStableBudgetDeferral(t *testing.T) {
	tasks := []Task{
		{ID: "p2-b9", Partition: 2, BlockNo: 9, Bytes: 50},
		{ID: "p0-b1", Partition: 0, BlockNo: 1, Bytes: 50},
		{ID: "p0-b0", Partition: 0, BlockNo: 0, Bytes: 100},
	}
	plan, err := PlanRound(tasks, 0, 4, 10, 150)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Run) != 2 || plan.Run[0].ID != "p0-b0" || plan.Run[1].ID != "p0-b1" {
		t.Fatalf("unexpected run order: %+v", plan.Run)
	}
	if plan.RemainingBudget != 0 {
		t.Fatalf("remaining=%d want 0", plan.RemainingBudget)
	}
	if len(plan.Deferred) != 1 || plan.Deferred[0].ID != "p2-b9" {
		t.Fatalf("unexpected deferred: %+v", plan.Deferred)
	}
	if plan.Deferred[0].RemainingBudget != 0 || plan.Deferred[0].ContinueFromRow != 0 {
		t.Fatalf("bad deferred details: %+v", plan.Deferred[0])
	}
}

func TestPlanRoundQueueFull(t *testing.T) {
	tasks := []Task{{ID: "a", Partition: 0, BlockNo: 0, Bytes: 1}, {ID: "b", Partition: 0, BlockNo: 1, Bytes: 1}, {ID: "c", Partition: 0, BlockNo: 2, Bytes: 1}}
	plan, err := PlanRound(tasks, 0, 1, 1, 100)
	if errs.Code(err) != errs.EDecodeQueueFull {
		t.Fatalf("expected queue full, got %v", err)
	}
	if plan == nil || len(plan.Run) != 1 || len(plan.Queued) != 2 {
		t.Fatalf("unexpected plan with queue-full error: %+v", plan)
	}
}

func TestPoolEnforcesConcurrencyAndBoundedQueue(t *testing.T) {
	pool := NewPool(2, 0)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	job := Job{Run: func(context.Context) error {
		started <- struct{}{}
		<-release
		return nil
	}}
	if err := pool.Submit(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if err := pool.Submit(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	waitStarted(t, started, 2)
	err := pool.Submit(context.Background(), job)
	if errs.Code(err) != errs.EDecodeQueueFull {
		t.Fatalf("expected E_DECODE_QUEUE_FULL, got %v", err)
	}
	close(release)
	pool.Close()
	if pool.MaxActive() != 2 {
		t.Fatalf("max active=%d want 2", pool.MaxActive())
	}
}

func waitStarted(t *testing.T, ch <-chan struct{}, count int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for i := 0; i < count; i++ {
		select {
		case <-ch:
		case <-deadline:
			t.Fatal("timed out waiting for workers")
		}
	}
}

func TestSubmitClosedPoolReturnsCodedInternal(t *testing.T) {
	pool := NewPool(1, 1)
	pool.Close()
	err := pool.Submit(context.Background(), Job{Run: func(context.Context) error { return nil }})
	if !errors.Is(err, err) || errs.Code(err) == "" {
		t.Fatalf("expected coded error, got %v", err)
	}
}
