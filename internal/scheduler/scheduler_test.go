package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/gsb/dnscache/internal/errs"
)

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func TestQueueLimitReturnsBusinessError(t *testing.T) {
	s := New(Config{GlobalLimit: 1, PerServerLimit: 1, QueueLimit: 1, RoundBudget: time.Second, DetachAfter: 500 * time.Millisecond})
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := s.Do(context.Background(), Request{Key: "a", Server: "auth-1"}, func(ctx context.Context) (any, error) {
			close(started)
			<-release
			return 1, nil
		})
		firstDone <- err
	}()
	<-started
	secondDone := make(chan error, 1)
	go func() {
		_, err := s.Do(context.Background(), Request{Key: "b", Server: "auth-1"}, func(ctx context.Context) (any, error) {
			<-release
			return 2, nil
		})
		secondDone <- err
	}()
	waitFor(t, time.Second, func() bool { return s.Stats().QueueDepth == 1 })
	start := time.Now()
	_, err := s.Do(context.Background(), Request{Key: "c", Server: "auth-1"}, func(ctx context.Context) (any, error) { return 3, nil })
	if errs.CodeOf(err) != errs.EUpstreamQueueFull {
		t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatalf("queue-full response was not immediate: %s", time.Since(start))
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}

func TestPriorityAndArrivalOrderAfterQueueing(t *testing.T) {
	s := New(Config{GlobalLimit: 1, PerServerLimit: 1, QueueLimit: 8, RoundBudget: 2 * time.Second, DetachAfter: time.Second})
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_, _ = s.Do(context.Background(), Request{Key: "blocker", Server: "auth"}, func(ctx context.Context) (any, error) {
			close(started)
			<-release
			return nil, nil
		})
	}()
	<-started
	var mu sync.Mutex
	var order []string
	run := func(key string, priority int) <-chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = s.Do(context.Background(), Request{Key: key, Server: "auth", Priority: priority}, func(ctx context.Context) (any, error) {
				mu.Lock()
				order = append(order, key)
				mu.Unlock()
				return nil, nil
			})
		}()
		return done
	}
	low1 := run("low-1", 10)
	waitFor(t, time.Second, func() bool { return s.Stats().QueueDepth == 1 })
	low2 := run("low-2", 10)
	waitFor(t, time.Second, func() bool { return s.Stats().QueueDepth == 2 })
	high := run("high", 1)
	waitFor(t, time.Second, func() bool { return s.Stats().QueueDepth == 3 })
	close(release)
	<-high
	<-low1
	<-low2
	mu.Lock()
	defer mu.Unlock()
	want := []string{"high", "low-1", "low-2"}
	if len(order) != 3 || order[0] != want[0] || order[1] != want[1] || order[2] != want[2] {
		t.Fatalf("order=%v want=%v", order, want)
	}
}

func TestPerServerLimitAllowsIndependentServer(t *testing.T) {
	s := New(Config{GlobalLimit: 3, PerServerLimit: 1, QueueLimit: 4, RoundBudget: time.Second})
	startedA := make(chan struct{})
	releaseA := make(chan struct{})
	var activeA atomic.Int32
	var maxA atomic.Int32
	go func() {
		_, _ = s.Do(context.Background(), Request{Key: "a1", Server: "A"}, func(ctx context.Context) (any, error) {
			n := activeA.Add(1)
			for {
				old := maxA.Load()
				if n <= old || maxA.CompareAndSwap(old, n) {
					break
				}
			}
			close(startedA)
			<-releaseA
			activeA.Add(-1)
			return nil, nil
		})
	}()
	<-startedA
	queued := make(chan struct{})
	go func() {
		_, _ = s.Do(context.Background(), Request{Key: "a2", Server: "A"}, func(ctx context.Context) (any, error) {
			close(queued)
			return nil, nil
		})
	}()
	waitFor(t, time.Second, func() bool { return s.Stats().QueueDepth == 1 })
	startedB := make(chan struct{})
	go func() {
		_, _ = s.Do(context.Background(), Request{Key: "b1", Server: "B"}, func(ctx context.Context) (any, error) {
			close(startedB)
			return nil, nil
		})
	}()
	select {
	case <-startedB:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("server B was blocked by server A's limit")
	}
	close(releaseA)
	select {
	case <-queued:
	case <-time.After(time.Second):
		t.Fatal("queued server A task did not run")
	}
	if maxA.Load() != 1 {
		t.Fatalf("server A max concurrency=%d", maxA.Load())
	}
}

func TestInFlightDeduplication(t *testing.T) {
	s := New(Config{GlobalLimit: 2, PerServerLimit: 2, QueueLimit: 8, RoundBudget: time.Second, DetachAfter: 800 * time.Millisecond})
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	values := make(chan any, 2)
	go func() {
		v, err := s.Do(context.Background(), Request{Key: "same", Server: "A"}, func(ctx context.Context) (any, error) {
			calls.Add(1)
			close(started)
			<-release
			return "answer", nil
		})
		if err != nil {
			t.Errorf("leader: %v", err)
		}
		values <- v
	}()
	<-started
	go func() {
		v, err := s.Do(context.Background(), Request{Key: "same", Server: "A"}, func(ctx context.Context) (any, error) {
			calls.Add(1)
			return "other", nil
		})
		if err != nil {
			t.Errorf("waiter: %v", err)
		}
		values <- v
	}()
	waitFor(t, time.Second, func() bool { return s.Stats().Joined == 1 })
	close(release)
	a, b := <-values, <-values
	if calls.Load() != 1 {
		t.Fatalf("upstream calls=%d, want 1", calls.Load())
	}
	if a != "answer" || b != "answer" {
		t.Fatalf("values=%v,%v", a, b)
	}
}

func TestRoundBudgetTimeoutReleasesSlot(t *testing.T) {
	s := New(Config{GlobalLimit: 1, PerServerLimit: 1, QueueLimit: 2, RoundBudget: 40 * time.Millisecond})
	cleaned := make(chan struct{})
	start := time.Now()
	_, err := s.Do(context.Background(), Request{Key: "timeout", Server: "A"}, func(ctx context.Context) (any, error) {
		<-ctx.Done()
		close(cleaned)
		return nil, errs.New(errs.EUpstreamTimeout, "simulated socket timeout")
	})
	if errs.CodeOf(err) != errs.EUpstreamTimeout {
		t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
	}
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond || elapsed > 400*time.Millisecond {
		t.Fatalf("unexpected elapsed=%s", elapsed)
	}
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("function did not observe cancellation")
	}
	waitFor(t, time.Second, func() bool {
		st := s.Stats()
		return st.GlobalActive == 0 && st.QueueDepth == 0
	})
}

func TestQueuedTaskRoundBudgetAndCleanup(t *testing.T) {
	s := New(Config{GlobalLimit: 1, PerServerLimit: 1, QueueLimit: 4, RoundBudget: 60 * time.Millisecond})
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		_, _ = s.Do(context.Background(), Request{Key: "long", Server: "A"}, func(ctx context.Context) (any, error) {
			close(started)
			<-release
			return nil, nil
		})
		close(firstDone)
	}()
	<-started
	start := time.Now()
	_, err := s.Do(context.Background(), Request{Key: "queued", Server: "A"}, func(ctx context.Context) (any, error) {
		t.Error("timed-out queued task must not execute")
		return nil, nil
	})
	if errs.CodeOf(err) != errs.ERoundBudgetExceeded {
		t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
	}
	if time.Since(start) > 300*time.Millisecond {
		t.Fatalf("queued timeout took too long: %s", time.Since(start))
	}
	waitFor(t, time.Second, func() bool { return s.Stats().QueueDepth == 0 })
	close(release)
	<-firstDone
}

func TestLongTailDetachesWhenCapacityExists(t *testing.T) {
	s := New(Config{GlobalLimit: 2, PerServerLimit: 2, QueueLimit: 4, RoundBudget: 600 * time.Millisecond, DetachAfter: 25 * time.Millisecond})
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	firstDone := make(chan struct{})
	go func() {
		_, _ = s.Do(context.Background(), Request{Key: "same", Server: "A"}, func(ctx context.Context) (any, error) {
			calls.Add(1)
			once.Do(func() { close(started) })
			select {
			case <-release:
				return "leader", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		close(firstDone)
	}()
	<-started
	time.Sleep(35 * time.Millisecond)
	secondDone := make(chan struct{})
	go func() {
		_, _ = s.Do(context.Background(), Request{Key: "same", Server: "A"}, func(ctx context.Context) (any, error) {
			calls.Add(1)
			select {
			case <-release:
				return "detached", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		close(secondDone)
	}()
	waitFor(t, time.Second, func() bool { return s.Stats().GlobalActive == 2 })
	close(release)
	<-firstDone
	<-secondDone
	if calls.Load() != 2 || s.Stats().Detached != 1 {
		t.Fatalf("calls=%d detached=%d", calls.Load(), s.Stats().Detached)
	}
}
