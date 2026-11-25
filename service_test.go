package convergence

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestInvalidFullSnapshotDoesNotReplaceUsableVersion(t *testing.T) {
	service := newTestService(t)
	commitSample(t, service)
	oldView := service.View()

	bad := SampleSnapshotV1()
	bad.SnapshotVersion = 2
	bad.Objects[1].ID = bad.Objects[0].ID
	bad.Objects[2].DependsOn = append(bad.Objects[2].DependsOn, "missing-reference")
	bad.Objects[3].Budget.BatchUnits = -1

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := service.CommitFull(ctx, bad)
	if !HasCode(err, CodeSpecInvalid) {
		t.Fatalf("CommitFull bad snapshot err=%v, want E_SPEC_INVALID", err)
	}
	current := service.View()
	if current != oldView {
		t.Fatal("atomic view pointer changed after invalid commit")
	}
	if current.Snapshot.SnapshotVersion != 1 || len(current.Snapshot.Objects) != 10 {
		t.Fatalf("old snapshot not fully usable: version=%d objects=%d", current.Snapshot.SnapshotVersion, len(current.Snapshot.Objects))
	}
}

func TestConcurrentDeltasSameBaseOneWinsAndOtherStale(t *testing.T) {
	service := newTestService(t)
	commitSample(t, service)

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, errs[index] = service.CommitDelta(ctx, SampleDeltaV2())
		}(i)
	}
	close(start)
	wg.Wait()

	success := 0
	stale := 0
	for _, err := range errs {
		if err == nil {
			success++
		} else if HasCode(err, CodeStaleBase) {
			stale++
		} else {
			t.Fatalf("unexpected delta error: %v", err)
		}
	}
	if success != 1 || stale != 1 {
		t.Fatalf("success=%d stale=%d errors=%v", success, stale, errs)
	}
	if service.View().Snapshot.SnapshotVersion != 2 {
		t.Fatalf("version=%d, want 2", service.View().Snapshot.SnapshotVersion)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := service.CommitDelta(ctx, SampleDeltaV2())
	if !HasCode(err, CodeStaleBase) {
		t.Fatalf("post-commit old base err=%v, want E_STALE_BASE", err)
	}
}

func TestDeltaRebuildsCascadeAndBatchPlanAtomically(t *testing.T) {
	service := newTestService(t)
	commitSample(t, service)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := service.CommitDelta(ctx, SampleDeltaV2())
	if err != nil {
		t.Fatal(err)
	}
	if result.Cascade == nil {
		t.Fatal("missing cascade report")
	}
	if !containsString(result.Cascade.InvalidatedDriftIDs, "golf-api") || !containsString(result.Cascade.InvalidatedDriftIDs, "india-frontend") {
		t.Fatalf("transitive cascade wrong: %+v", result.Cascade)
	}
	if !containsString(result.Cascade.InvalidatedBatchIDs, "B003") {
		t.Fatalf("batch cascade should include B003: %v", result.Cascade.InvalidatedBatchIDs)
	}
	view := service.View()
	if view.Cascade == nil || view.Plan == nil || view.Summary.SnapshotVersion != 2 {
		t.Fatalf("complete new version not visible: %+v", view.Summary)
	}
	if len(view.Plan.Batches) != 7 {
		t.Fatalf("batches=%d, want rearranged 7", len(view.Plan.Batches))
	}
}

func TestStaleDeltaDoesNotChangeState(t *testing.T) {
	service := newTestService(t)
	commitSample(t, service)
	delta := SampleDeltaV2()
	delta.BaseVersion = 41
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := service.CommitDelta(ctx, delta)
	if !HasCode(err, CodeStaleBase) {
		t.Fatalf("err=%v want E_STALE_BASE", err)
	}
	if service.View().Snapshot.SnapshotVersion != 1 {
		t.Fatal("stale delta changed state")
	}
}
