package scheduler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"internalpki/workbench/internal/model"
)

func TestRunHonorsConcurrencyAndQueuesAllJobs(t *testing.T) {
	const jobCount = 18
	var active int32
	var peak int32
	var mu sync.Mutex
	orderSeen := make(map[int]bool)

	jobs := make([]Job, 0, jobCount)
	for i := 0; i < jobCount; i++ {
		path := fmt.Sprintf("sample-%02d", i)
		jobs = append(jobs, Job{
			Path:  path,
			Depth: 1 + i%4,
			Execute: func(ctx context.Context, queuePosition int, waitTicks int64) (model.SampleResult, error) {
				current := atomic.AddInt32(&active, 1)
				for {
					old := atomic.LoadInt32(&peak)
					if current <= old || atomic.CompareAndSwapInt32(&peak, old, current) {
						break
					}
				}
				mu.Lock()
				if orderSeen[queuePosition] {
					t.Errorf("duplicate queue position %d", queuePosition)
				}
				orderSeen[queuePosition] = true
				mu.Unlock()
				time.Sleep(4 * time.Millisecond)
				atomic.AddInt32(&active, -1)
				return model.SampleResult{Path: path, Outcome: model.OutcomePassed, State: model.StateCompleted}, nil
			},
		})
	}
	results, err := Run(context.Background(), jobs, 3)
	if err != nil {
		t.Fatalf("run scheduler: %v", err)
	}
	if len(results) != jobCount {
		t.Fatalf("got %d results, want %d", len(results), jobCount)
	}
	if peak > 3 {
		t.Fatalf("peak concurrency %d exceeded limit 3", peak)
	}
	if peak < 2 {
		t.Fatalf("expected actual parallelism, peak was %d", peak)
	}
	for i := 0; i < jobCount; i++ {
		if !orderSeen[i] {
			t.Fatalf("queue position %d was never assigned", i)
		}
	}
}
