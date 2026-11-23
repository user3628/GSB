package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueueingAndConcurrencyLimit(t *testing.T) {
	s := New(2)
	var active atomic.Int64
	var observedMax atomic.Int64
	jobs := make([]Job, 6)
	for i := range jobs {
		jobs[i] = Job{Index: i, ID: "job", Run: func(context.Context) error {
			current := active.Add(1)
			for {
				peak := observedMax.Load()
				if current <= peak || observedMax.CompareAndSwap(peak, current) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			active.Add(-1)
			return nil
		}}
	}
	errs := s.Run(context.Background(), jobs)
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	stats := s.Stats()
	if observedMax.Load() > 2 || stats.PeakInFlight > 2 {
		t.Fatalf("concurrency exceeded: observed=%d stats=%#v", observedMax.Load(), stats)
	}
	if stats.PeakQueued < 1 {
		t.Fatalf("queue was not observable: %#v", stats)
	}
	if stats.Completed != 6 || stats.Failed != 0 {
		t.Fatalf("unexpected completion stats: %#v", stats)
	}
}
