package blockpool

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.UnixMilli(1_800_000_000_000)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func testPool(t *testing.T) (*Pool, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	p, err := Open(filepath.Join(t.TempDir(), "pool.sqlite"), Options{Now: clock.Now, DefaultLeaseTTL: time.Minute})
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, clock
}

func fixture(t *testing.T, p *Pool, nodeCount int, contents ...[]byte) (map[string]string, []NodeRef) {
	t.Helper()
	ctx := context.Background()
	if _, err := p.CreateNamespace(ctx, "ns"); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	nodes := make([]NodeRef, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		ref := NodeRef{Namespace: "ns", Path: fmt.Sprintf("dir/file-%d", i)}
		if _, err := p.CreateNode(ctx, ref); err != nil {
			t.Fatalf("create node: %v", err)
		}
		nodes = append(nodes, ref)
	}
	digests := make(map[string]string, len(contents))
	for i, content := range contents {
		digest := digestOf(content)
		key := fmt.Sprintf("b%d", i)
		if _, err := p.Ingest(ctx, IngestRequest{Digest: digest, Data: content, RetentionMS: 0}); err != nil {
			t.Fatalf("ingest %s: %v", key, err)
		}
		digests[key] = digest
	}
	return digests, nodes
}

func TestDedupAndCrossNodeRefCounts(t *testing.T) {
	p, _ := testPool(t)
	ctx := context.Background()
	content := []byte("shared block")
	digests, nodes := fixture(t, p, 3, content, []byte("second block"))
	digest := digests["b0"]

	// Re-ingesting the same bytes must not create a second physical block.
	if _, err := p.Ingest(ctx, IngestRequest{Digest: digest, Data: content}); err != nil {
		t.Fatalf("re-ingest: %v", err)
	}
	var physical int
	if err := p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM blocks WHERE digest=?`, digest).Scan(&physical); err != nil {
		t.Fatal(err)
	}
	if physical != 1 {
		t.Fatalf("physical blocks = %d want 1", physical)
	}

	versions := make([]int64, 0, len(nodes))
	for i, node := range nodes {
		lease, err := p.Mount(ctx, MountRequest{Namespace: node.Namespace, Path: node.Path, Digest: digest, LeaseTTLMS: 60_000, RequestID: fmt.Sprintf("mount-%d", i)})
		if err != nil {
			t.Fatalf("mount node %d: %v", i, err)
		}
		versions = append(versions, lease.Version)
	}
	details, err := p.GetBlock(ctx, digest)
	if err != nil {
		t.Fatalf("get block: %v", err)
	}
	if details.RefCount != 3 || details.ActiveLeaseCount != 3 || len(details.Leases) != 3 {
		t.Fatalf("counts = ref:%d active:%d leases:%d", details.RefCount, details.ActiveLeaseCount, len(details.Leases))
	}

	second, err := p.Mount(ctx, MountRequest{Namespace: nodes[0].Namespace, Path: nodes[0].Path, Digest: digest, LeaseTTLMS: 60_000, RequestID: "mount-0-repeat"})
	if err != nil {
		t.Fatalf("idempotent mount: %v", err)
	}
	if second.Version != versions[0] || second.ExpiresAtMS != versionsToLease(t, p, digest, nodes[0]).ExpiresAtMS {
		t.Fatalf("idempotent mount changed lease: %+v", second)
	}

	result, err := p.Unmount(ctx, UnmountRequest{Namespace: nodes[0].Namespace, Path: nodes[0].Path, Digest: digest, RequestID: "unmount-0"})
	if err != nil || !result.Removed || result.RefCount != 2 {
		t.Fatalf("unmount = %+v, %v", result, err)
	}
	again, err := p.Unmount(ctx, UnmountRequest{Namespace: nodes[0].Namespace, Path: nodes[0].Path, Digest: digest, RequestID: "unmount-0"})
	if err != nil || !again.Removed || again.RefCount != 2 {
		t.Fatalf("idempotent unmount = %+v, %v", again, err)
	}
	if _, err := p.Unmount(ctx, UnmountRequest{Namespace: nodes[1].Namespace, Path: nodes[1].Path, Digest: digest, RequestID: "unmount-1"}); err != nil {
		t.Fatalf("unmount second: %v", err)
	}
	if _, err := p.Unmount(ctx, UnmountRequest{Namespace: nodes[2].Namespace, Path: nodes[2].Path, Digest: digest, RequestID: "unmount-2"}); err != nil {
		t.Fatalf("unmount third: %v", err)
	}
	details, err = p.GetBlock(ctx, digest)
	if err != nil || details.RefCount != 0 || details.ActiveLeaseCount != 0 {
		t.Fatalf("after unmount = %+v, %v", details, err)
	}
}

func versionsToLease(t *testing.T, p *Pool, digest string, node NodeRef) Lease {
	t.Helper()
	tx, err := p.beginRead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	lease, berr := leaseByRef(context.Background(), tx, node.Namespace, node.Path, digest)
	if berr != nil || lease == nil {
		t.Fatalf("read lease: %v %v", lease, berr)
	}
	return *lease
}

func TestLeaseExpiryAndStaleRenewal(t *testing.T) {
	p, clock := testPool(t)
	ctx := context.Background()
	digests, nodes := fixture(t, p, 1, []byte("expiring"))
	digest := digests["b0"]
	node := nodes[0]

	lease, err := p.Mount(ctx, MountRequest{Namespace: node.Namespace, Path: node.Path, Digest: digest, LeaseTTLMS: 100, RequestID: "lease-1"})
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	clock.Advance(101 * time.Millisecond)
	details, err := p.GetBlock(ctx, digest)
	if err != nil {
		t.Fatalf("get expired: %v", err)
	}
	if details.RefCount != 0 || details.ActiveLeaseCount != 0 {
		t.Fatalf("expired refs still counted: %+v", details)
	}

	lease2, err := p.Mount(ctx, MountRequest{Namespace: node.Namespace, Path: node.Path, Digest: digest, LeaseTTLMS: 100, RequestID: "lease-2"})
	if err != nil {
		t.Fatalf("remount: %v", err)
	}
	if lease2.Version <= lease.Version {
		t.Fatalf("version did not advance: %d then %d", lease.Version, lease2.Version)
	}
	if _, err := p.Renew(ctx, RenewRequest{Namespace: node.Namespace, Path: node.Path, Digest: digest, Version: lease.Version, LeaseTTLMS: 100, RequestID: "renew-stale"}); err == nil || err.Code != CodeLeaseStale {
		t.Fatalf("stale renew error = %v", err)
	} else if got := err.Context["current_version"]; got != lease2.Version {
		t.Fatalf("stale current_version = %#v want %d", got, lease2.Version)
	}
	renewed, err := p.Renew(ctx, RenewRequest{Namespace: node.Namespace, Path: node.Path, Digest: digest, Version: lease2.Version, LeaseTTLMS: 100, RequestID: "renew-current"})
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	clock.Advance(10 * time.Millisecond)
	renewed, err = p.Renew(ctx, RenewRequest{Namespace: node.Namespace, Path: node.Path, Digest: digest, Version: lease2.Version, LeaseTTLMS: 100, RequestID: "renew-current-2"})
	if err != nil {
		t.Fatalf("renew second: %v", err)
	}
	clock.Advance(50 * time.Millisecond)
	details, err = p.GetBlock(ctx, digest)
	if err != nil || details.RefCount != 1 {
		t.Fatalf("renewed lease not active: %+v %v", details, err)
	}
	clock.Advance(60 * time.Millisecond)
	details, err = p.GetBlock(ctx, digest)
	if err != nil || details.RefCount != 0 {
		t.Fatalf("renewed lease did not expire: %+v %v", details, err)
	}
	if renewed.ExpiresAtMS <= lease2.ExpiresAtMS {
		t.Fatalf("renew did not extend expiry: %d <= %d", renewed.ExpiresAtMS, lease2.ExpiresAtMS)
	}
}

func TestConcurrentMountOnlyOneIssuesLease(t *testing.T) {
	p, _ := testPool(t)
	ctx := context.Background()
	digests, nodes := fixture(t, p, 1, []byte("contended"))
	digest := digests["b0"]
	node := nodes[0]

	unlock, lockErr := p.locks.Lock(ctx, []string{digestKey(digest)})
	if lockErr != nil {
		t.Fatal(lockErr)
	}
	type outcome struct {
		lease Lease
		err   *Error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func(i int) {
			<-start
			lease, err := p.Mount(ctx, MountRequest{Namespace: node.Namespace, Path: node.Path,
				Digest: digest, LeaseTTLMS: 60_000, RequestID: fmt.Sprintf("contended-%d", i)})
			results <- outcome{lease: lease, err: err}
		}(i)
	}
	close(start)
	waitForPendingIntents(t, p, "contended-0", "contended-1")
	unlock()

	var success, held int
	for i := 0; i < 2; i++ {
		out := <-results
		if out.err == nil {
			success++
		} else if out.err.Code == CodeLeaseHeld {
			held++
			if _, ok := out.err.Context["expires_at_ms"]; !ok {
				t.Fatalf("E_LEASE_HELD missing expiry context: %+v", out.err)
			}
		} else {
			t.Fatalf("unexpected mount error: %v", out.err)
		}
	}
	if success != 1 || held != 1 {
		t.Fatalf("success=%d held=%d want 1/1", success, held)
	}
	details, err := p.GetBlock(ctx, digest)
	if err != nil || details.RefCount != 1 {
		t.Fatalf("ref count after contention = %+v %v", details, err)
	}
}

func TestBatchReplaceAtomicityAndBusy(t *testing.T) {
	p, _ := testPool(t)
	ctx := context.Background()
	digests, nodes := fixture(t, p, 1, []byte("one"), []byte("two"), []byte("three"))
	node := nodes[0]
	if _, err := p.Mount(ctx, MountRequest{Namespace: node.Namespace, Path: node.Path, Digest: digests["b0"], LeaseTTLMS: 60_000, RequestID: "initial"}); err != nil {
		t.Fatal(err)
	}

	result, err := p.ReplaceNodeRefs(ctx, ReplaceRequest{Namespace: node.Namespace, Path: node.Path,
		Blocks: []BatchBlock{{Digest: digests["b1"]}, {Digest: digests["b2"]}}, LeaseTTLMS: 60_000, RequestID: "replace-1"})
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if len(result.Added) != 2 || len(result.Removed) != 1 {
		t.Fatalf("replace result: %+v", result)
	}
	// A sequential mount after a batch-issued lease is idempotent even when the
	// injected clock has not advanced between the two operations.
	afterBatch, mountErr := p.Mount(ctx, MountRequest{Namespace: node.Namespace, Path: node.Path,
		Digest: digests["b1"], LeaseTTLMS: 60_000, RequestID: "mount-after-batch"})
	if mountErr != nil {
		t.Fatalf("mount after batch: %v", mountErr)
	}
	if afterBatch.Version != 1 {
		t.Fatalf("mount after batch version = %d want 1", afterBatch.Version)
	}
	for _, digest := range digests {
		details, err := p.GetBlock(ctx, digest)
		if err != nil {
			t.Fatal(err)
		}
		want := int64(0)
		if digest == digests["b1"] || digest == digests["b2"] {
			want = 1
		}
		if details.RefCount != want {
			t.Fatalf("digest %s ref_count=%d want %d", digest, details.RefCount, want)
		}
	}

	missing := digestOf([]byte("missing"))
	if _, err := p.ReplaceNodeRefs(ctx, ReplaceRequest{Namespace: node.Namespace, Path: node.Path,
		Blocks: []BatchBlock{{Digest: digests["b0"]}, {Digest: missing}}, LeaseTTLMS: 60_000, RequestID: "replace-missing"}); err == nil || err.Code != CodeContentCorrupt {
		t.Fatalf("missing content error = %v", err)
	}
	details, err := p.GetBlock(ctx, digests["b0"])
	if err != nil || details.RefCount != 0 {
		t.Fatalf("failed batch changed state: %+v %v", details, err)
	}
	details, err = p.GetBlock(ctx, digests["b1"])
	if err != nil || details.RefCount != 1 {
		t.Fatalf("failed batch changed previous state: %+v %v", details, err)
	}

	if _, err := p.ReplaceNodeRefs(ctx, ReplaceRequest{Namespace: node.Namespace, Path: "no-such-node",
		Blocks: []BatchBlock{}, RequestID: "replace-unknown"}); err == nil || err.Code != CodeNodeUnknown {
		t.Fatalf("unknown node error = %v", err)
	}
	if _, err := p.ReplaceNodeRefs(ctx, ReplaceRequest{Namespace: node.Namespace, Path: node.Path,
		Blocks: []BatchBlock{{Digest: digests["b0"], Data: []byte("wrong")}}, RequestID: "replace-corrupt"}); err == nil || err.Code != CodeContentCorrupt {
		t.Fatalf("corrupt content error = %v", err)
	}

	unlockGate, ok := p.batchGate.TryLock([]string{batchKey(node.Namespace, node.Path)})
	if !ok {
		t.Fatal("test could not acquire batch gate")
	}
	_, busy := p.ReplaceNodeRefs(ctx, ReplaceRequest{Namespace: node.Namespace, Path: node.Path,
		Blocks: []BatchBlock{}, RequestID: "replace-busy"})
	unlockGate()
	if busy == nil || busy.Code != CodeNodeBusy {
		t.Fatalf("busy error = %v", busy)
	}
	if _, err := p.db.ExecContext(ctx, `INSERT INTO batch_locks(node_key, request_id, acquired_at_ms)
		VALUES (?, 'external-owner', ?)`, batchKey(node.Namespace, node.Path), p.nowMS()); err != nil {
		t.Fatal(err)
	}
	_, dbBusy := p.ReplaceNodeRefs(ctx, ReplaceRequest{Namespace: node.Namespace, Path: node.Path,
		Blocks: []BatchBlock{}, RequestID: "replace-db-busy"})
	if _, err := p.db.ExecContext(ctx, `DELETE FROM batch_locks WHERE node_key=?`, batchKey(node.Namespace, node.Path)); err != nil {
		t.Fatal(err)
	}
	if dbBusy == nil || dbBusy.Code != CodeNodeBusy {
		t.Fatalf("persistent busy error = %v", dbBusy)
	}
}

func TestReclaimSafetyAndCapacityOrdering(t *testing.T) {
	p, clock := testPool(t)
	ctx := context.Background()
	large := bytes.Repeat([]byte("L"), 300)
	medium := bytes.Repeat([]byte("M"), 200)
	small := bytes.Repeat([]byte("S"), 100)
	digests, nodes := fixture(t, p, 1, large, medium, small)
	node := nodes[0]

	if _, err := p.Mount(ctx, MountRequest{Namespace: node.Namespace, Path: node.Path, Digest: digests["b0"], LeaseTTLMS: 60_000, RequestID: "reclaim-active"}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Reclaim(ctx, ReclaimRequest{Digest: digests["b0"], RequestID: "reclaim-blocked"}); err == nil || err.Code != CodeReclaimUnsafe {
		t.Fatalf("active reclaim = %v", err)
	} else if blockers, ok := err.Context["blockers"].([]string); !ok || len(blockers) == 0 {
		t.Fatalf("unsafe context missing blockers: %+v", err.Context)
	}
	if _, err := p.Unmount(ctx, UnmountRequest{Namespace: node.Namespace, Path: node.Path, Digest: digests["b0"], RequestID: "reclaim-unmount"}); err != nil {
		t.Fatal(err)
	}

	// Give b0 a long retention and verify the remaining retention blocks GC.
	if _, err := p.Ingest(ctx, IngestRequest{Digest: digests["b0"], Data: large, RetentionMS: 1_000}); err != nil {
		t.Fatal(err)
	}
	// Ingest is idempotent and keeps the original retention, so use a fresh
	// block to exercise a non-expired retention window.
	held := []byte("held by retention")
	heldDigest := digestOf(held)
	if _, err := p.Ingest(ctx, IngestRequest{Digest: heldDigest, Data: held, RetentionMS: 1_000}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Reclaim(ctx, ReclaimRequest{Digest: heldDigest, RequestID: "reclaim-retained"}); err == nil || err.Code != CodeReclaimUnsafe {
		t.Fatalf("retained reclaim = %v", err)
	}
	clock.Advance(1001 * time.Millisecond)
	if _, err := p.Reclaim(ctx, ReclaimRequest{Digest: heldDigest, RequestID: "reclaim-expired-retention"}); err != nil {
		t.Fatalf("expired retention reclaim: %v", err)
	}

	result, err := p.Reclaim(ctx, ReclaimRequest{CapacityBytes: int64ptr(350), RequestID: "capacity"})
	if err != nil {
		t.Fatalf("capacity reclaim: %v", err)
	}
	if result.ReleasedBytes != 300 || len(result.Reclaimed) != 1 || result.Reclaimed[0].Digest != digests["b0"] {
		t.Fatalf("capacity result = %+v", result)
	}
	details, getErr := p.GetBlock(ctx, digests["b0"])
	if getErr != nil || details.State != BlockReclaimed || details.RefCount != 0 {
		t.Fatalf("reclaimed state = %+v %v", details, getErr)
	}
}

func TestReclaimMountRaceIsMutuallyVisible(t *testing.T) {
	p, _ := testPool(t)
	ctx := context.Background()
	for iteration := 0; iteration < 12; iteration++ {
		content := []byte(fmt.Sprintf("race-%d", iteration))
		digest := digestOf(content)
		if _, err := p.Ingest(ctx, IngestRequest{Digest: digest, Data: content}); err != nil {
			t.Fatal(err)
		}
		if iteration == 0 {
			if _, err := p.CreateNamespace(ctx, "ns"); err != nil {
				t.Fatal(err)
			}
			if _, err := p.CreateNode(ctx, NodeRef{Namespace: "ns", Path: "race-node"}); err != nil {
				t.Fatal(err)
			}
		}
		unlock, lockErr := p.locks.Lock(ctx, []string{digestKey(digest)})
		if lockErr != nil {
			t.Fatal(lockErr)
		}
		type mountResult struct {
			lease Lease
			err   *Error
		}
		type reclaimResult struct {
			result ReclaimResult
			err    *Error
		}
		mountDone := make(chan mountResult, 1)
		reclaimDone := make(chan reclaimResult, 1)
		start := make(chan struct{})
		mountID := fmt.Sprintf("race-mount-%d", iteration)
		reclaimID := fmt.Sprintf("race-reclaim-%d", iteration)
		go func() {
			<-start
			lease, mountErr := p.Mount(ctx, MountRequest{Namespace: "ns", Path: "race-node", Digest: digest,
				LeaseTTLMS: 60_000, RequestID: mountID})
			mountDone <- mountResult{lease: lease, err: mountErr}
		}()
		go func() {
			<-start
			reclaimed, reclaimErr := p.Reclaim(ctx, ReclaimRequest{Digest: digest, RequestID: reclaimID})
			reclaimDone <- reclaimResult{result: reclaimed, err: reclaimErr}
		}()
		close(start)
		waitForPendingIntents(t, p, mountID, reclaimID)
		unlock()
		mountOut := <-mountDone
		reclaimOut := <-reclaimDone
		if mountOut.err == nil && reclaimOut.err == nil {
			t.Fatalf("iteration %d: mount and reclaim both succeeded", iteration)
		}
		if mountOut.err != nil && reclaimOut.err != nil {
			t.Fatalf("iteration %d: both failed: mount=%v reclaim=%v", iteration, mountOut.err, reclaimOut.err)
		}
		if mountOut.err == nil && (reclaimOut.err == nil || reclaimOut.err.Code != CodeReclaimUnsafe) {
			t.Fatalf("iteration %d: mount won but reclaim error = %v", iteration, reclaimOut.err)
		}
		if reclaimOut.err == nil && (mountOut.err == nil || mountOut.err.Code != CodeBlockNotFound) {
			t.Fatalf("iteration %d: reclaim won but mount error = %v", iteration, mountOut.err)
		}
		details, getErr := p.GetBlock(ctx, digest)
		if getErr != nil {
			t.Fatalf("iteration %d get: %v", iteration, getErr)
		}
		if details.State == BlockReclaimed && details.RefCount != 0 {
			t.Fatalf("iteration %d: reclaimed block has refs: %+v", iteration, details)
		}
		if details.State == BlockLive && details.RefCount == 0 {
			t.Fatalf("iteration %d: live block should retain the winning mount: %+v", iteration, details)
		}
	}
}

func waitForPendingIntents(t *testing.T, p *Pool, requestIDs ...string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		placeholders := ""
		args := make([]any, 0, len(requestIDs))
		for i, requestID := range requestIDs {
			if i > 0 {
				placeholders += ","
			}
			placeholders += "?"
			args = append(args, requestID)
		}
		var count int
		query := `SELECT COUNT(*) FROM intents WHERE state='pending' AND request_id IN (` + placeholders + `)`
		if err := p.db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
			t.Fatalf("poll pending intents: %v", err)
		}
		if count == len(requestIDs) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending intents = %d want %d", count, len(requestIDs))
		}
		time.Sleep(time.Millisecond)
	}
}

func TestIdempotentRequestAndConflict(t *testing.T) {
	p, _ := testPool(t)
	ctx := context.Background()
	digests, nodes := fixture(t, p, 1, []byte("idem"))
	node := nodes[0]
	req := MountRequest{Namespace: node.Namespace, Path: node.Path, Digest: digests["b0"], LeaseTTLMS: 60_000, RequestID: "same-request"}
	first, err := p.Mount(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Mount(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Version != second.Version || first.ExpiresAtMS != second.ExpiresAtMS {
		t.Fatalf("idempotent result changed: %+v vs %+v", first, second)
	}
	conflicting := req
	conflicting.LeaseTTLMS = 120_000
	if _, err := p.Mount(ctx, conflicting); err == nil || err.Code != CodeConflictingRequest {
		t.Fatalf("conflict error = %v", err)
	}
}

func TestConcurrentDuplicateRequestWaitsAndReusesResult(t *testing.T) {
	p, _ := testPool(t)
	ctx := context.Background()
	digests, nodes := fixture(t, p, 1, []byte("duplicate request"))
	node := nodes[0]
	req := MountRequest{Namespace: node.Namespace, Path: node.Path, Digest: digests["b0"],
		LeaseTTLMS: 60_000, RequestID: "duplicate-in-flight"}

	unlock, lockErr := p.locks.Lock(ctx, []string{digestKey(digests["b0"])})
	if lockErr != nil {
		t.Fatal(lockErr)
	}
	type outcome struct {
		lease Lease
		err   *Error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			lease, err := p.Mount(ctx, req)
			results <- outcome{lease: lease, err: err}
		}()
	}
	close(start)
	time.Sleep(30 * time.Millisecond)
	unlock()
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("duplicate request errors: %v %v", first.err, second.err)
	}
	if first.lease.Version != second.lease.Version || first.lease.ExpiresAtMS != second.lease.ExpiresAtMS {
		t.Fatalf("duplicate results differ: %+v %+v", first.lease, second.lease)
	}
	details, err := p.GetBlock(ctx, digests["b0"])
	if err != nil || details.RefCount != 1 {
		t.Fatalf("duplicate request count = %+v %v", details, err)
	}
}

func TestZeroByteBlock(t *testing.T) {
	p, _ := testPool(t)
	ctx := context.Background()
	digests, nodes := fixture(t, p, 1)
	empty := []byte{}
	digest := digestOf(empty)
	if _, err := p.Ingest(ctx, IngestRequest{Digest: digest, Data: nil}); err != nil {
		t.Fatalf("ingest empty block: %v", err)
	}
	if _, err := p.Mount(ctx, MountRequest{Namespace: nodes[0].Namespace, Path: nodes[0].Path,
		Digest: digest, LeaseTTLMS: 60_000, RequestID: "empty-mount"}); err != nil {
		t.Fatalf("mount empty block: %v", err)
	}
	details, err := p.GetBlock(ctx, digest)
	if err != nil || details.SizeBytes != 0 || details.RefCount != 1 {
		t.Fatalf("empty block details = %+v %v", details, err)
	}
	_ = digests
}

func TestRecoveryRebuildsPersistedCount(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "recovery.sqlite")
	clock := newFakeClock()
	p, err := Open(path, Options{Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	digests, nodes := fixture(t, p, 1, []byte("recover me"))
	node := nodes[0]
	if _, err := p.Mount(ctx, MountRequest{Namespace: node.Namespace, Path: node.Path, Digest: digests["b0"], LeaseTTLMS: 60_000, RequestID: "recovery-mount"}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.ExecContext(ctx, `UPDATE blocks SET ref_count=7 WHERE digest=?`, digests["b0"]); err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.ExecContext(ctx, `INSERT INTO intents
		(id, request_id, operation, aggregate_key, payload_json, state, created_at_ms)
		VALUES ('pending-test', 'crashed-request', 'mount', 'test', '{}', 'pending', ?)`, clock.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.ExecContext(ctx, `INSERT INTO requests
		(request_id, operation, request_hash, status, owner, created_at_ms)
		VALUES ('crashed-request', 'mount', 'hash', 'in_progress', 'dead-process', ?)`, clock.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	report, rerr := reopened.RecoveryReport(ctx)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if report.PendingIntents != 1 || report.AbortedIntents != 1 || report.RecoveredRequests != 1 || report.DiffCount != 1 {
		t.Fatalf("report = %+v", report)
	}
	if len(report.Diffs) != 1 || report.Diffs[0].PersistedRefCount != 7 || report.Diffs[0].RebuiltRefCount != 1 {
		t.Fatalf("diffs = %+v", report.Diffs)
	}
	details, getErr := reopened.GetBlock(ctx, digests["b0"])
	if getErr != nil || details.RefCount != 1 {
		t.Fatalf("rebuilt details = %+v %v", details, getErr)
	}
}

func TestForcedTerminationSubprocessRecovery(t *testing.T) {
	if os.Getenv("BLOCKPOOL_CRASH_CHILD") == "1" {
		runCrashChild()
		return
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "crash.sqlite")
	cmd := exec.Command(os.Args[0], "-test.run=TestForcedTerminationSubprocessRecovery", "-test.count=1")
	cmd.Env = append(os.Environ(), "BLOCKPOOL_CRASH_CHILD=1", "BLOCKPOOL_CRASH_DB="+path)
	err := cmd.Run()
	if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 17 {
		t.Fatalf("child exit = %v", err)
	}
	clock := newFakeClock()
	p, openErr := Open(path, Options{Now: clock.Now})
	if openErr != nil {
		t.Fatalf("open after crash: %v", openErr)
	}
	defer p.Close()
	report, rerr := p.RecoveryReport(context.Background())
	if rerr != nil {
		t.Fatal(rerr)
	}
	if report.PendingIntents != 1 || report.DiffCount != 1 {
		t.Fatalf("crash report = %+v", report)
	}
	if len(report.Diffs) != 1 || report.Diffs[0].RebuiltRefCount != 1 {
		t.Fatalf("crash diffs = %+v", report.Diffs)
	}
	details, getErr := p.GetBlock(context.Background(), digestOf([]byte("crash block")))
	if getErr != nil || details.RefCount != 1 {
		t.Fatalf("post-crash block = %+v %v", details, getErr)
	}
}

func runCrashChild() {
	path := os.Getenv("BLOCKPOOL_CRASH_DB")
	ctx := context.Background()
	p, err := Open(path, Options{Now: newFakeClock().Now})
	if err != nil {
		os.Exit(91)
	}
	if _, err := p.CreateNamespace(ctx, "ns"); err != nil {
		os.Exit(92)
	}
	if _, err := p.CreateNode(ctx, NodeRef{Namespace: "ns", Path: "crash-node"}); err != nil {
		os.Exit(93)
	}
	content := []byte("crash block")
	digest := digestOf(content)
	if _, err := p.Ingest(ctx, IngestRequest{Digest: digest, Data: content}); err != nil {
		os.Exit(94)
	}
	now := p.nowMS()
	tx, err := p.db.Begin()
	if err != nil {
		os.Exit(95)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO intents
		(id, request_id, operation, aggregate_key, payload_json, state, created_at_ms)
		VALUES ('crash-intent', 'crash-request', 'mount', ?, '{}', 'pending', ?)`, digest, now); err != nil {
		os.Exit(96)
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO leases
		(namespace, path, digest, version, mounted_at_ms, expires_at_ms, last_renewed_at_ms, state)
		VALUES ('ns', 'crash-node', ?, 1, ?, ?, ?, 'active')`, digest, now, now+60_000, now)
	if err != nil {
		os.Exit(97)
	}
	leaseID, _ := res.LastInsertId()
	if _, err := tx.ExecContext(ctx, `INSERT INTO refs
		(namespace, path, digest, lease_id, version, mounted_at_ms, expires_at_ms)
		VALUES ('ns', 'crash-node', ?, ?, 1, ?, ?)`, digest, leaseID, now, now+60_000); err != nil {
		os.Exit(98)
	}
	// Deliberately leave the persisted count stale to model a torn observable
	// state that recovery must diagnose from committed rows.
	if _, err := tx.ExecContext(ctx, `UPDATE blocks SET ref_count=0 WHERE digest=?`, digest); err != nil {
		os.Exit(99)
	}
	if err := tx.Commit(); err != nil {
		os.Exit(100)
	}
	os.Exit(17)
}

func int64ptr(v int64) *int64 { return &v }

func TestSampleReplayAndSharedDigest(t *testing.T) {
	sample, sampleErr := ReadSample(filepath.Join("samples", "fixed.json"))
	if sampleErr != nil {
		t.Fatalf("read sample: %v", sampleErr)
	}
	p, _ := testPool(t)
	ctx := context.Background()
	if err := SeedSample(ctx, p, sample); err != nil {
		t.Fatalf("seed sample: %v", err)
	}
	for i, action := range sample.Actions {
		if err := ApplySampleAction(ctx, p, action); err != nil {
			t.Fatalf("action %d %s: %v", i, action.Op, err)
		}
	}
	// The sample intentionally shares b0 across namespaces; at this point it
	// must still have multiple physical refs from distinct logical nodes.
	var shared string
	maxRefs := int64(0)
	for _, block := range sample.Blocks {
		details, err := p.GetBlock(ctx, block.Digest)
		if err != nil {
			t.Fatalf("get block %s: %v", block.Digest, err)
		}
		if details.RefCount > maxRefs {
			maxRefs = details.RefCount
			shared = block.Digest
		}
	}
	if maxRefs < 2 {
		t.Fatalf("sample did not leave a shared block: max refs=%d", maxRefs)
	}
	_ = shared
	reclaimed, reclaimedErr := p.GetBlock(ctx, sample.Blocks[5].Digest)
	if reclaimedErr != nil {
		t.Fatalf("reclaimed block query: %v", reclaimedErr)
	}
	if reclaimed.State != BlockReclaimed || reclaimed.RefCount != 0 {
		t.Fatalf("sample reclaim state = %+v", reclaimed)
	}
}

func TestNonNegativeIntegerAndSampleSpecErrors(t *testing.T) {
	bad := []string{
		`{"seed":-1}`,
		`{"seed":1.5}`,
		`{"seed":1e3}`,
		`{"seed":9223372036854775808}`,
		`{"seed":"1"}`,
	}
	for _, body := range bad {
		if _, err := ParseSample([]byte(body)); err == nil || err.Code != CodeSpecInvalid {
			t.Fatalf("body %s error = %v", body, err)
		}
	}
	body := []byte(`{"seed":1,"namespaces":["ns"],"nodes":[],"blocks":[],"actions":[{"op":"mount","namespace":"missing","path":"x","digest":"` + digestOf([]byte("x")) + `","request_id":"r"}]}`)
	if _, err := ParseSample(body); err == nil || err.Code != CodeSpecInvalid {
		t.Fatalf("undeclared namespace error = %v", err)
	}
	body = []byte(`{"seed":1,"namespaces":["ns"],"nodes":[],"blocks":[],"actions":[{"op":"mount","namespace":"ns","path":"x","digest":"not-a-digest","request_id":"r"}]}`)
	if _, err := ParseSample(body); err == nil || err.Code != CodeSpecInvalid {
		t.Fatalf("bad digest error = %v", err)
	}
}
