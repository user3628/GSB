package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"cdn-edge-consistency/internal/model"
)

func waitCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached")
}

func TestDeterministicBudgetSkipOrder(t *testing.T) {
	s := New(Config{GlobalLimit: 1, PerOriginLimit: 1, QueueCapacity: 10, RoundBudgetMs: 2, DefaultTaskCost: 1})
	defer s.Close()
	block := make(chan struct{})
	mk := func(id string, priority int, heat int64, blockIt bool) Task {
		return Task{ID: id, OriginID: "o1", NodeID: "n-" + id, Key: id, Priority: priority, Heat: heat, CostMs: 1, Run: func(context.Context) error {
			if blockIt {
				<-block
			}
			return nil
		}}
	}
	first, err := s.SubmitTask(context.Background(), mk("first", 100, 1, true))
	if err != nil {
		t.Fatal(err)
	}
	waitCondition(t, time.Second, func() bool { return s.Active() == 1 })
	b, _ := s.SubmitTask(context.Background(), mk("b", 60, 0, false))
	c, _ := s.SubmitTask(context.Background(), mk("c", 50, 9, false))
	a, _ := s.SubmitTask(context.Background(), mk("a", 50, 1, false))
	close(block)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	for _, ch := range []<-chan error{first, b} {
		if err := <-ch; err != nil {
			t.Fatalf("admitted task failed: %v", err)
		}
	}
	for _, ch := range []<-chan error{c, a} {
		err := <-ch
		if err == nil || model.AsBiz(err).Code != model.CodeBudgetExhausted {
			t.Fatalf("expected budget skip, got %v", err)
		}
	}
	report := s.LastReport()
	if len(report.Skipped) != 2 {
		t.Fatalf("skipped=%+v", report.Skipped)
	}
	if report.Skipped[0].TaskID != "c" || report.Skipped[1].TaskID != "a" {
		t.Fatalf("deterministic skip order changed: %+v", report.Skipped)
	}
	if report.UsedCostMs != 2 {
		t.Fatalf("used cost=%d", report.UsedCostMs)
	}
}

func TestBoundedQueueFull(t *testing.T) {
	s := New(Config{GlobalLimit: 1, PerOriginLimit: 1, QueueCapacity: 1, RoundBudgetMs: 100, DefaultTaskCost: 1})
	defer s.Close()
	block := make(chan struct{})
	first := Task{ID: "running", OriginID: "o", Key: "running", Run: func(context.Context) error { <-block; return nil }}
	ch1, err := s.SubmitTask(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	waitCondition(t, time.Second, func() bool { return s.Active() == 1 })
	ch2, err := s.SubmitTask(context.Background(), Task{ID: "queued", OriginID: "o", Key: "queued", Run: func(context.Context) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.SubmitTask(context.Background(), Task{ID: "overflow", OriginID: "o", Key: "overflow", Run: func(context.Context) error { return nil }})
	if err == nil || model.AsBiz(err).Code != model.CodeOriginQueueFull {
		t.Fatalf("expected queue full, got %v", err)
	}
	close(block)
	<-ch1
	<-ch2
}

func TestCoalescerSharesOneExecution(t *testing.T) {
	s := New(Config{GlobalLimit: 2, PerOriginLimit: 1, QueueCapacity: 10, RoundBudgetMs: 100, DefaultTaskCost: 1})
	defer s.Close()
	c := NewCoalescer(s)
	var calls atomic.Int32
	fn := func(context.Context) (any, error) {
		calls.Add(1)
		time.Sleep(60 * time.Millisecond)
		return "content-v1", nil
	}
	meta := FlightMeta{FlightKey: "k", TaskID: "k", OriginID: "o", NodeID: "n", Key: "k", Priority: 1, CostMs: 1, Kind: KindOrigin}
	type result struct {
		value     any
		coalesced bool
		err       error
	}
	ch := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			v, coalesced, err := c.Do(context.Background(), meta, time.Second, fn)
			ch <- result{value: v, coalesced: coalesced, err: err}
		}()
		time.Sleep(10 * time.Millisecond)
	}
	r1, r2 := <-ch, <-ch
	if r1.err != nil || r2.err != nil {
		t.Fatal(r1.err, r2.err)
	}
	if r1.value != "content-v1" || r2.value != "content-v1" {
		t.Fatalf("values=%v,%v", r1.value, r2.value)
	}
	if calls.Load() != 1 {
		t.Fatalf("origin function ran %d times", calls.Load())
	}
	if !(r1.coalesced || r2.coalesced) || (r1.coalesced && r2.coalesced) {
		t.Fatalf("coalesced flags: %v %v", r1.coalesced, r2.coalesced)
	}
}

func TestCoalescerWaiterTimeoutDoesNotCancelFlight(t *testing.T) {
	s := New(Config{GlobalLimit: 1, PerOriginLimit: 1, QueueCapacity: 5, RoundBudgetMs: 100, DefaultTaskCost: 1})
	defer s.Close()
	c := NewCoalescer(s)
	release := make(chan struct{})
	meta := FlightMeta{FlightKey: "slow", TaskID: "slow", OriginID: "o", NodeID: "n", Key: "slow", CostMs: 1, Kind: KindOrigin}
	firstDone := make(chan error, 1)
	go func() {
		_, _, err := c.Do(context.Background(), meta, 20*time.Millisecond, func(context.Context) (any, error) {
			<-release
			return "ok", nil
		})
		firstDone <- err
	}()
	select {
	case err := <-firstDone:
		if err == nil || model.AsBiz(err).Code != model.CodeRevalidateTimeout {
			t.Fatalf("expected E_REVALIDATE_TIMEOUT, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout did not fire")
	}
	close(release)
	waitCondition(t, time.Second, func() bool { return c.InFlight() == 0 })
}

func TestTaskErrorsRemainDistinct(t *testing.T) {
	s := New(Config{GlobalLimit: 1, PerOriginLimit: 1, QueueCapacity: 2, RoundBudgetMs: 10, DefaultTaskCost: 1})
	defer s.Close()
	ch, err := s.SubmitTask(context.Background(), Task{ID: "err", OriginID: "o", Key: "err", Run: func(context.Context) error {
		return errors.New("boom")
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-ch; err == nil {
		t.Fatal("expected task error")
	}
}
