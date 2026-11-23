package scheduler

import (
	"context"
	"testing"
	"time"

	"vpnbench/internal/model"
)

func TestManagerQueuesAboveLimit(t *testing.T) {
	manager := New(1)
	releaseFirst, err := manager.Acquire(context.Background(), "s1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if manager.InFlight() != 1 || manager.Queued() != 0 {
		t.Fatalf("initial metrics = %+v", manager.Metrics())
	}
	acquired := make(chan func(), 1)
	go func() {
		release, acquireErr := manager.Acquire(context.Background(), "s2", 1)
		if acquireErr == nil {
			acquired <- release
		}
	}()
	deadline := time.Now().Add(time.Second)
	for manager.Queued() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if manager.Queued() != 1 || manager.InFlight() != 1 {
		t.Fatalf("queue metrics = %+v", manager.Metrics())
	}
	releaseFirst()
	select {
	case releaseSecond := <-acquired:
		if manager.InFlight() != 1 {
			t.Fatalf("second session not in flight: %+v", manager.Metrics())
		}
		releaseSecond()
	case <-time.After(time.Second):
		t.Fatal("queued session was not admitted")
	}
	metrics := manager.Metrics()
	if metrics.PeakInFlight != 1 || metrics.PeakQueued != 1 || metrics.Admitted != 2 || metrics.InFlight != 0 || metrics.Queued != 0 {
		t.Fatalf("final metrics = %+v", metrics)
	}
}

func TestManagerRecordsSessionConflict(t *testing.T) {
	manager := New(1)
	release, err := manager.Acquire(context.Background(), "same", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	_, err = manager.Acquire(context.Background(), "same", 2)
	if model.CodeOf(err) != model.CodeSessionConflict {
		t.Fatalf("code = %s", model.CodeOf(err))
	}
	metrics := manager.Metrics()
	if len(metrics.Conflicts) != 1 || metrics.Conflicts[0].SessionID != "same" || metrics.Conflicts[0].RequestSeq != 2 {
		t.Fatalf("conflicts = %+v", metrics.Conflicts)
	}
}

func TestManagerContextCancellationRemovesQueuedSession(t *testing.T) {
	manager := New(1)
	release, err := manager.Acquire(context.Background(), "held", 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = manager.Acquire(ctx, "waiting", 1)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if manager.Queued() != 0 {
		t.Fatalf("queued after cancellation = %d", manager.Queued())
	}
	release()
	nextRelease, err := manager.Acquire(context.Background(), "waiting", 1)
	if err != nil {
		t.Fatalf("cancelled session ID was not released: %v", err)
	}
	nextRelease()
}
