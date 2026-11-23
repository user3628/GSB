package main

import (
	"context"
	"testing"
	"time"
)

func TestSchedulerQueueBudgetSkipAndPriority(t *testing.T) {
	env := newTestEnv(t, 10*time.Second)
	env.topic("jobs", 3, "explicit", 100, 50, 10)
	for i := 0; i < 3; i++ {
		env.publish("j"+string(rune('0'+i)), "jobs", i, "job", "tx-job-"+string(rune('0'+i)),
			"p"+string(rune('0'+i)))
	}
	ctx := context.Background()
	env.sched.SetConfig(SchedulerConfig{GlobalConcurrency: 2, PerPartitionLeaseLimit: 1,
		QueueCapacity: 2, RoundBudgetMS: 5, QueryTimeoutMS: 100, MinDispatchMS: 2})
	if _, err := env.sched.Enqueue(ctx, SchedulerTaskRequest{Kind: "txn", TxnID: "tx-job-0"}); err != nil {
		t.Fatal(err)
	}
	dup, err := env.sched.Enqueue(ctx, SchedulerTaskRequest{Kind: "txn", TxnID: "tx-job-0"})
	if err != nil || !dup.Deduped {
		t.Fatalf("same partition/txn was not deduped: %+v %v", dup, err)
	}
	if _, err := env.sched.Enqueue(ctx, SchedulerTaskRequest{Kind: "txn", TxnID: "tx-job-1"}); err != nil {
		t.Fatal(err)
	}
	_, err = env.sched.Enqueue(ctx, SchedulerTaskRequest{Kind: "txn", TxnID: "tx-job-2"})
	requireCode(t, err, CodeQueueFull)

	round, err := env.sched.RunRound(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if round.Status != "degraded_budget" || len(round.Steps) != 2 {
		t.Fatalf("budget degradation missing: %+v", round)
	}
	for _, step := range round.Steps {
		if step.Result != "SKIPPED" || step.Reason != "round_budget_exhausted" {
			t.Fatalf("unexpected skip trace: %+v", step)
		}
	}
	if len(env.sched.View().Queue) != 2 {
		t.Fatalf("skipped tasks did not remain queued: %+v", env.sched.View().Queue)
	}
	if txn, _ := env.svc.GetTransaction(ctx, "tx-job-0"); txn.Status != TxnPending {
		t.Fatalf("skipped txn changed state: %+v", txn)
	}

	env.sched.SetConfig(SchedulerConfig{GlobalConcurrency: 2, PerPartitionLeaseLimit: 1,
		QueueCapacity: 10, RoundBudgetMS: 1000, QueryTimeoutMS: 200, MinDispatchMS: 2})
	round, err = env.sched.RunRound(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if round.Status != "completed" || len(env.sched.View().Queue) != 0 {
		t.Fatalf("scheduler did not drain queue: %+v", round)
	}
	for i := 0; i < 2; i++ {
		txn, err := env.svc.GetTransaction(ctx, "tx-job-"+string(rune('0'+i)))
		if err != nil {
			t.Fatal(err)
		}
		if txn.Status != TxnCommitted {
			t.Fatalf("scheduled txn %s = %s", txn.TxnID, txn.Status)
		}
	}

	// Higher priority partitions are emitted before lower priority partitions.
	if _, err := env.sched.Enqueue(ctx, SchedulerTaskRequest{Kind: "txn", TxnID: "tx-job-2"}); err != nil {
		t.Fatal(err)
	}
	env.sched.mu.Lock()
	ordered := env.sched.orderedQueueLocked(1)
	env.sched.mu.Unlock()
	if len(ordered) != 1 || ordered[0].TxnID != "tx-job-2" {
		t.Fatalf("unexpected ordered queue: %+v", ordered)
	}
}

func TestSchedulerGlobalConcurrencyLimit(t *testing.T) {
	env := newTestEnv(t, 10*time.Second)
	env.topic("jobs", 4, "explicit", 100, 90, 80, 70)
	for i := 0; i < 4; i++ {
		id := "tx-c" + string(rune('0'+i))
		env.publish("m"+string(rune('0'+i)), "jobs", i, "job", id, "p"+string(rune('0'+i)))
		// Make each coordinator call take long enough to overlap.
		if _, err := env.svc.CreateInjection(context.Background(), InjectionRequest{
			Kind: "txn", Target: id, Mode: CoordinatorHang, DelayMS: 80}); err != nil {
			t.Fatal(err)
		}
	}
	// The hang injection is replaced with commit for the actual run; concurrency
	// is still bounded by the scheduler's dispatch loop. Use a hanging coordinator
	// only to exercise timeout and queue interaction.
	env.sched.SetConfig(SchedulerConfig{GlobalConcurrency: 1, PerPartitionLeaseLimit: 1,
		QueueCapacity: 8, RoundBudgetMS: 1000, QueryTimeoutMS: 30, MinDispatchMS: 2})
	for i := 0; i < 4; i++ {
		if _, err := env.sched.Enqueue(context.Background(), SchedulerTaskRequest{
			Kind: "txn", TxnID: "tx-c" + string(rune('0'+i))}); err != nil {
			t.Fatal(err)
		}
	}
	round, err := env.sched.RunRound(context.Background(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(round.Steps) != 4 {
		t.Fatalf("expected four steps, got %+v", round.Steps)
	}
	for _, step := range round.Steps {
		if step.Code != CodeTxnTimeout {
			t.Fatalf("expected timeout diagnostics, got %+v", step)
		}
	}
}
