package arbiter

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestInterruptibleWaitWokenByRelease proves the wait parks on a channel and is
// really woken by release; the manual clock only advances after the waiter is
// parked, and no polling loop is involved.
func TestInterruptibleWaitWokenByRelease(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	lease := mustAcquire(t, h, "r1", "alice", time.Minute)

	type result struct {
		resp *AcquireResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := h.eng.Acquire(ctx, AcquireRequest{
			RowID: "r1", Holder: "bob", TTLMs: time.Minute.Milliseconds(), WaitMs: 5000,
		})
		done <- result{resp, err}
	}()

	waitUntilWaiters(t, h, "r1", 1)
	if err := h.eng.Release(ctx, ReleaseRequest{
		RowID: "r1", Holder: "alice", LeaseVersion: lease.LeaseVersion,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("waited acquire failed: %v", r.err)
		}
		if r.resp.Mode != "waited" || r.resp.Holder != "bob" {
			t.Fatalf("unexpected grant: %+v", r.resp)
		}
		if r.resp.LeaseVersion != 2 {
			t.Fatalf("expected new lease version 2, got %d", r.resp.LeaseVersion)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not woken by release")
	}
}

func TestBoundedWaitTimesOutAndLeavesQueue(t *testing.T) {
	h := newHarness(t)
	_ = mustAcquire(t, h, "r1", "alice", time.Minute)

	done := make(chan error, 1)
	go func() {
		_, err := h.eng.Acquire(context.Background(), AcquireRequest{
			RowID: "r1", Holder: "bob", TTLMs: time.Minute.Milliseconds(), WaitMs: 1000,
		})
		done <- err
	}()
	waitUntilWaiters(t, h, "r1", 1)
	h.clk.Advance(1100 * time.Millisecond)

	select {
	case err := <-done:
		if errCode(t, err) != CodeLeaseTimeout {
			t.Fatalf("expected timeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait did not time out")
	}
	g := h.eng.gates.get("r1")
	g.mu.Lock()
	left := len(g.waiters)
	g.mu.Unlock()
	if left != 0 {
		t.Fatalf("timed-out waiter remained in queue: %d", left)
	}
}

func TestWaitCancellationIsReal(t *testing.T) {
	h := newHarness(t)
	_ = mustAcquire(t, h, "r1", "alice", time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := h.eng.Acquire(ctx, AcquireRequest{
			RowID: "r1", Holder: "bob", TTLMs: time.Minute.Milliseconds(), WaitMs: 10000,
		})
		done <- err
	}()
	waitUntilWaiters(t, h, "r1", 1)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not interrupt the wait")
	}
}

func TestRecycleRespectsActiveSnapshotsAndRetention(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	// Build row versions 1..4 on r1.
	var lastCommit int64
	for i := int64(1); i < 4; i++ {
		lease := mustAcquire(t, h, "r1", "alice", time.Minute)
		resp, err := h.eng.Commit(ctx, CommitRequest{
			RowID: "r1", Holder: "alice", LeaseVersion: lease.LeaseVersion,
			BaseRowVersion: i, Content: "content-v" + string(rune('0'+i+1)),
		})
		if err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
		lastCommit = resp.CommitVer
	}
	// A long-lived reader pins the seed snapshot, protecting version 1.
	if err := h.eng.RegisterSnapshot(1, "alice"); err != nil {
		t.Fatal(err)
	}
	res, err := h.eng.Recycle(ctx, RecycleRequest{
		RowID: "r1", MinRetainSnapshots: 1, MinRetainVersions: 1,
	})
	if err != nil {
		t.Fatalf("recycle with active snapshot: %v", err)
	}
	if len(res.RetiredRowVersions) != 0 {
		t.Fatalf("active snapshot should protect every old version, retired=%v", res.RetiredRowVersions)
	}

	// Release the reader and set a high retention floor: versions below the
	// floor become recyclable while the latest is always kept.
	if err := h.eng.ReleaseSnapshot(1, "alice"); err != nil {
		t.Fatal(err)
	}
	res, err = h.eng.Recycle(ctx, RecycleRequest{
		RowID: "r1", MinRetainSnapshots: lastCommit, MinRetainVersions: 1,
	})
	if err != nil {
		t.Fatalf("recycle: %v", err)
	}
	if len(res.RetiredRowVersions) == 0 {
		t.Fatal("expected at least one retired version")
	}
	for _, rv := range res.RetiredRowVersions {
		if rv == 4 {
			t.Fatal("latest row version must never be retired")
		}
	}

	// The retired version now deterministically returns E_VERSION_RETIRED,
	// never a newer substitute. Repeated reads give the same answer.
	for i := 0; i < 2; i++ {
		_, err := h.eng.Read(ctx, ReadRequest{RowID: "r1", SnapshotVer: 1})
		if errCode(t, err) != CodeVersionRetired {
			t.Fatalf("read %d: expected retired, got %v", i, err)
		}
	}
}

func waitUntilWaiters(t *testing.T, h *harness, rowID string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		g := h.eng.gates.get(rowID)
		g.mu.Lock()
		got := len(g.waiters)
		g.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("waiter never entered the queue")
}
