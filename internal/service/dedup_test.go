package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"gpusched/internal/domain"
)

func TestConcurrentIdenticalAdmissionRunsOnce(t *testing.T) {
	app, _ := newTestApp(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	results := make([]*AdmitResult, 12)
	errs := make([]error, 12)
	start := make(chan struct{})
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = app.AdmitOne(ctx, "r1")
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d error: %v", i, err)
		}
		if results[i] == nil || results[i].State != domain.StateAdmitted {
			t.Fatalf("goroutine %d bad result: %+v", i, results[i])
		}
	}
	// Exactly one ADMIT intent should have been committed.
	req, _ := app.GetRequest(ctx, "r1")
	if req.State != domain.StateAdmitted {
		t.Fatalf("final state=%s", req.State)
	}
}

func TestDoOnceConflictingPayload(t *testing.T) {
	app, _ := newTestApp(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)

	// First call holds the in-flight slot until we release it.
	go func() {
		_, err := app.doOnce("key-x", map[string]int{"v": 1}, func() (any, error) {
			close(entered)
			<-release
			return "first", nil
		})
		done <- err
	}()
	<-entered
	// Release the blocker asynchronously so the waiter can observe completion
	// without the test thread being stuck on its own blocking call.
	time.AfterFunc(20*time.Millisecond, func() { close(release) })

	_, err := app.doOnce("key-x", map[string]int{"v": 2}, func() (any, error) {
		return "second", nil
	})
	be, ok := domain.AsBizError(err)
	if !ok || be.Code != domain.EConflictingRequest {
		t.Fatalf("expected E_CONFLICTING_REQUEST, got %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("first call: %v", err)
	}

	// An identical payload arriving while in flight reuses the result (no error).
	entered2 := make(chan struct{})
	release2 := make(chan struct{})
	done2 := make(chan error, 1)
	go func() {
		_, e := app.doOnce("key-y", map[string]int{"v": 9}, func() (any, error) {
			close(entered2)
			<-release2
			return "ok", nil
		})
		done2 <- e
	}()
	<-entered2
	time.AfterFunc(20*time.Millisecond, func() { close(release2) })
	_, errSame := app.doOnce("key-y", map[string]int{"v": 9}, func() (any, error) {
		return "should-not-run", nil
	})
	if errSame != nil {
		t.Fatalf("identical in-flight payload should reuse result, got %v", errSame)
	}
	if err := <-done2; err != nil {
		t.Fatalf("second group first call: %v", err)
	}
}
