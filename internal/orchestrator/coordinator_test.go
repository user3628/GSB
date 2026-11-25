package orchestrator_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"leasearbiter/internal/core"
	"leasearbiter/internal/orchestrator"
)

func TestQueueFullRejectsWholeBatchBeforeAnyTaskStarts(t *testing.T) {
	coordinator, err := orchestrator.New(orchestrator.Config{Concurrency: 1, QueueCapacity: 0, Budget: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var started atomic.Int64
	tasks := []orchestrator.Task{
		{HolderID: "a", Run: func(context.Context) orchestrator.TaskResult {
			started.Add(1)
			return orchestrator.TaskResult{HolderID: "a"}
		}},
		{HolderID: "b", Run: func(context.Context) orchestrator.TaskResult {
			started.Add(1)
			return orchestrator.TaskResult{HolderID: "b"}
		}},
	}
	outcome := coordinator.Execute(context.Background(), tasks)
	if outcome.Err == nil || outcome.Err.Code != core.CodeQueueFull {
		t.Fatalf("expected %s, got %#v", core.CodeQueueFull, outcome.Err)
	}
	time.Sleep(20 * time.Millisecond)
	if started.Load() != 0 {
		t.Fatalf("queue-full batch started %d tasks", started.Load())
	}
}

func TestGlobalConcurrencyAndBoundedQueue(t *testing.T) {
	coordinator, err := orchestrator.New(orchestrator.Config{Concurrency: 2, QueueCapacity: 3, Budget: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var active atomic.Int64
	var maxActive atomic.Int64
	var mu sync.Mutex
	completed := make([]string, 0, 5)
	tasks := make([]orchestrator.Task, 0, 5)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		id := id
		tasks = append(tasks, orchestrator.Task{HolderID: id, Run: func(context.Context) orchestrator.TaskResult {
			current := active.Add(1)
			for {
				observed := maxActive.Load()
				if current <= observed || maxActive.CompareAndSwap(observed, current) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			active.Add(-1)
			mu.Lock()
			completed = append(completed, id)
			mu.Unlock()
			return orchestrator.TaskResult{HolderID: id}
		}})
	}
	outcome := coordinator.Execute(context.Background(), tasks)
	if outcome.Err != nil {
		t.Fatal(outcome.Err)
	}
	if len(outcome.Completed) != 5 || len(outcome.Deferred) != 0 {
		t.Fatalf("completed=%d deferred=%d", len(outcome.Completed), len(outcome.Deferred))
	}
	if maxActive.Load() > 2 {
		t.Fatalf("global concurrency exceeded: %d", maxActive.Load())
	}
}

func TestBudgetDeferralOrderIsDeterministic(t *testing.T) {
	coordinator, err := orchestrator.New(orchestrator.Config{Concurrency: 1, QueueCapacity: 8, Budget: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	specs := []struct {
		id        string
		magnitude int64
	}{
		{"holder-z", 50},
		{"holder-a", 50},
		{"holder-m", 30},
		{"holder-b", 10},
	}
	tasks := make([]orchestrator.Task, 0, len(specs))
	for _, spec := range specs {
		spec := spec
		tasks = append(tasks, orchestrator.Task{
			HolderID:         spec.id,
			DriftMagnitudeMS: spec.magnitude,
			Run: func(ctx context.Context) orchestrator.TaskResult {
				<-ctx.Done()
				return orchestrator.TaskResult{HolderID: spec.id, Deferred: true, DeferredReason: "ROUND_BUDGET_EXHAUSTED"}
			},
		})
	}
	outcome := coordinator.Execute(context.Background(), tasks)
	if outcome.Err != nil {
		t.Fatal(outcome.Err)
	}
	if len(outcome.Deferred) != 4 {
		t.Fatalf("deferred=%d, want 4", len(outcome.Deferred))
	}
	want := []string{"holder-a", "holder-z", "holder-m", "holder-b"}
	for i, entry := range outcome.Deferred {
		if entry.HolderID != want[i] || entry.Order != i+1 {
			t.Fatalf("deferred[%d] = %+v, want holder %s order %d", i, entry, want[i], i+1)
		}
	}
	if outcome.RemainingBudgetMS != 0 {
		t.Fatalf("remaining budget = %d, want 0", outcome.RemainingBudgetMS)
	}
}
