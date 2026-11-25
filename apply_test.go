package convergence

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func newLoopbackService(t *testing.T) (*Service, *LocalTarget, *httptest.Server) {
	t.Helper()
	target := NewLocalTarget()
	server := httptest.NewServer(target.Handler())
	t.Cleanup(server.Close)
	service := newTestService(t)
	service.SetEndpointOverride(server.URL)
	commitSample(t, service)
	return service, target, server
}

func TestApplyBatchWritesReadsBackAndAtomicallyFlipsBatch(t *testing.T) {
	service, _, _ := newLoopbackService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := service.ApplyBatch(ctx, ApplyBatchRequest{BatchID: "B001-S1"})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if result.Status != "succeeded" || len(result.ObjectResults) != 2 {
		t.Fatalf("bad result: %+v", result)
	}
	for _, objectResult := range result.ObjectResults {
		if objectResult.Status != "verified" || objectResult.ReadbackDrift == nil || !objectResult.ReadbackDrift.Converged {
			t.Fatalf("object not readback-verified: %+v", objectResult)
		}
	}
	state := service.View().BatchStates["B001-S1"]
	if state.Status != "succeeded" || len(state.ObjectResults) != 2 {
		t.Fatalf("query observed mixed/incomplete batch state: %+v", state)
	}
}

func TestSameBatchConcurrentRequestsExecuteOnceAndConflictOnDifferentContent(t *testing.T) {
	service, target, _ := newLoopbackService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const callers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			_, errs[index] = service.ApplyBatch(ctx, ApplyBatchRequest{BatchID: "B001-S1"})
		}(i)
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("duplicate apply failed: %v", err)
		}
	}
	if target.PutCount() != 2 {
		t.Fatalf("PUT count=%d, want exactly one write per batch object (2)", target.PutCount())
	}

	_, err := service.ApplyBatch(ctx, ApplyBatchRequest{BatchID: "B001-S1", RequestHash: "sha256:different-content"})
	if !HasCode(err, CodeConflictingRequest) {
		t.Fatalf("conflict err=%v, want E_CONFLICTING_REQUEST", err)
	}
}

func TestApplyMismatchIncludesObjectExpectedAndActual(t *testing.T) {
	service, target, _ := newLoopbackService(t)
	target.SetMismatch("charlie-network", true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := service.ApplyBatch(ctx, ApplyBatchRequest{BatchID: "B001-S2"})
	if err == nil || !HasCode(err, CodeApplyMismatch) {
		t.Fatalf("err=%v result=%+v, want E_APPLY_MISMATCH", err, result)
	}
	business := AsError(err)
	details, ok := business.Details.(map[string]any)
	if !ok {
		t.Fatalf("details type %T", business.Details)
	}
	if details["object_id"] != "charlie-network" || details["expected_fields"] == nil || details["actual_fields"] == nil {
		t.Fatalf("mismatch details missing object/fields: %+v", details)
	}
	if result == nil || result.Status != "failed" {
		t.Fatalf("batch result should be failed: %+v", result)
	}
}

func TestWriteTimeoutClosesConnectionAndReleasesSchedulerSlot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(300 * time.Millisecond):
		}
	}))
	defer server.Close()

	client := NewWriteClient(20 * time.Millisecond)
	object := Object{ID: "slow", ClusterID: "c", Expected: FieldSet{"replicas": field(1, "git")}}
	_, err := client.WriteAndReadback(context.Background(), server.URL, object, "req", 20*time.Millisecond)
	if !HasCode(err, CodeWriteTimeout) {
		t.Fatalf("err=%v, want E_WRITE_TIMEOUT", err)
	}
	if !client.Tracker.WaitClosed(client.Tracker.DialCount(), time.Second) {
		t.Fatalf("timed-out connection was not closed: dial=%d close=%d", client.Tracker.DialCount(), client.Tracker.CloseCount())
	}

	scheduler := NewScheduler(1, 0)
	defer scheduler.Close()
	job, submitErr := scheduler.Submit(context.Background(), "slow-batch", func(ctx context.Context) (*BatchApplyResult, error) {
		_, callErr := client.WriteAndReadback(ctx, server.URL, object, "scheduled", 20*time.Millisecond)
		return nil, callErr
	})
	if submitErr != nil {
		t.Fatal(submitErr)
	}
	_, waitErr := scheduler.Wait(context.Background(), job)
	if !HasCode(waitErr, CodeWriteTimeout) {
		t.Fatalf("scheduled wait err=%v", waitErr)
	}
	if !waitCondition(time.Second, func() bool { return scheduler.Active() == 0 }) {
		t.Fatalf("scheduler slot not released: active=%d", scheduler.Active())
	}
}

func TestBoundedQueueReturnsQueueFull(t *testing.T) {
	scheduler := NewScheduler(1, 1)
	defer scheduler.Close()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	first, err := scheduler.Submit(context.Background(), "first", func(context.Context) (*BatchApplyResult, error) {
		started <- struct{}{}
		<-release
		return &BatchApplyResult{Status: "succeeded"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if !waitCondition(time.Second, func() bool { return scheduler.Active() == 1 }) {
		t.Fatalf("first job not active: %d", scheduler.Active())
	}
	second, err := scheduler.Submit(context.Background(), "second", func(context.Context) (*BatchApplyResult, error) {
		return &BatchApplyResult{Status: "succeeded"}, nil
	})
	if err != nil {
		t.Fatalf("second job should queue: %v", err)
	}
	if !waitCondition(time.Second, func() bool { return scheduler.Queued() == 1 }) {
		t.Fatalf("second job not queued: queued=%d", scheduler.Queued())
	}
	_, err = scheduler.Submit(context.Background(), "third", func(context.Context) (*BatchApplyResult, error) {
		return &BatchApplyResult{Status: "succeeded"}, nil
	})
	if !HasCode(err, CodeWriteQueueFull) {
		t.Fatalf("third err=%v want E_WRITE_QUEUE_FULL", err)
	}
	close(release)
	if _, err := scheduler.Wait(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Wait(context.Background(), second); err != nil {
		t.Fatal(err)
	}
}

func TestApplyRoundBudgetCancelsSlowWritesAndSettlesBatchState(t *testing.T) {
	target := NewLocalTarget()
	server := httptest.NewServer(target.Handler())
	t.Cleanup(server.Close)

	service := newTestService(t)
	service.SetEndpointOverride(server.URL)

	snapshot := SampleSnapshotV1()
	snapshot.Policy.RoundBudget = Duration(40 * time.Millisecond)
	snapshot.Policy.WriteTimeout = Duration(time.Second)
	if err := PrepareSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := service.CommitFull(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	for _, object := range snapshot.Objects {
		target.SetDelay(object.ID, 200*time.Millisecond)
	}

	start := time.Now()
	round, err := service.ApplyRound(context.Background())
	elapsed := time.Since(start)
	t.Logf("ApplyRound returned in %s", elapsed)
	if !HasCode(err, CodeRoundBudget) {
		t.Fatalf("ApplyRound err=%v round=%+v, want %s", err, round, CodeRoundBudget)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("budget cancellation took too long: %s", elapsed)
	}
	if service.View().Summary.BatchesApplying != 0 {
		t.Fatalf("round returned with applying batches: %+v", service.View().Summary)
	}
}

func TestBatchStateMutationRejectsStaleSnapshotVersion(t *testing.T) {
	service := newTestService(t)
	commitSample(t, service)
	oldView := service.View()
	oldBatch := oldView.BatchStates["B001-S1"].Batch

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := service.CommitDelta(ctx, SampleDeltaV2()); err != nil {
		t.Fatal(err)
	}

	updated := service.mutateBatchState(1, oldBatch.ContentHash, oldBatch.BatchID, func(state *BatchState) {
		state.Status = "succeeded"
	})
	if updated {
		t.Fatal("stale version mutation must not update current view")
	}
	current := service.View()
	if current.Snapshot.SnapshotVersion != 2 {
		t.Fatalf("version=%d, want 2", current.Snapshot.SnapshotVersion)
	}
	if state := current.BatchStates["B001-S1"]; state == nil || state.Status != "pending" {
		t.Fatalf("new version batch was contaminated by stale apply: %+v", state)
	}

	currentBatch := current.BatchStates["B001-S1"]
	updated = service.mutateBatchState(2, currentBatch.ContentHash, "B001-S1", func(state *BatchState) {
		state.Status = "succeeded"
	})
	if !updated || service.View().BatchStates["B001-S1"].Status != "succeeded" {
		t.Fatal("matching version/hash mutation should succeed")
	}
}
