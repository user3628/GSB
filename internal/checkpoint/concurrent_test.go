package checkpoint

import (
	"context"
	"sync"
	"testing"

	"regworkbench/internal/workerr"
)

func TestConcurrentSameSequenceOnlyOneWins(t *testing.T) {
	m, err := OpenManager(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()
	var wg sync.WaitGroup
	var mu sync.Mutex
	success, stale, other := 0, 0, 0
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.Publish(ctx, Record{Seq: 1, Stage: "preprocess", Fold: -1})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				success++
				return
			}
			if we, ok := workerr.As(err); ok && we.Code == workerr.EStaleCheckpoint {
				stale++
			} else {
				other++
				t.Errorf("unexpected publish error: %v", err)
			}
		}()
	}
	wg.Wait()
	if success != 1 || stale != 11 || other != 0 {
		t.Fatalf("success=%d stale=%d other=%d", success, stale, other)
	}
}
