package walgroup

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentReadersNeverSeeNewerThanSnapshot(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t, Options{SegmentMaxBytes: 700, MaxTxnsPerSegment: 2})
	if _, err := e.Commit(ctx, TransactionInput{ID: "seed", Records: []PageUpdate{{PageNo: 1, Value: "v1"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.AdvanceCheckpoint(ctx, CheckpointRequest{Sequence: 1, ReplayStartLSN: 1}); err != nil {
		t.Fatal(err)
	}
	const iterations = 40
	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < iterations; i++ {
				page, err := e.ReadPage(ctx, 1, 1)
				if err != nil {
					if CodeOf(err) == CodeCheckpointLag {
						continue
					}
					errCh <- fmt.Errorf("reader: %w", err)
					return
				}
				if page.Version != 1 || page.Value != "v1" || page.LSN > 1 {
					errCh <- fmt.Errorf("reader saw newer page: %+v", page)
					return
				}
			}
		}()
	}
	close(start)
	for i := 0; i < iterations; i++ {
		id := fmt.Sprintf("write-%03d", i)
		if _, err := e.Commit(ctx, TransactionInput{ID: id, Records: []PageUpdate{{PageNo: 1, Value: id}}}); err != nil {
			t.Errorf("writer %d: %v", i, err)
			break
		}
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
