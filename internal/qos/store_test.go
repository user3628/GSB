package qos

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func newStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := OpenStore(StoreOptions{DBPath: filepath.Join(dir, "qos.db"), DataDir: filepath.Join(dir, "data"), SampleSeed: DefaultSampleSeed})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, dir
}

func snapshotPolicy(t *testing.T, store *Store, version int64, queues []Queue) *MutationResponse {
	t.Helper()
	resp, biz := store.SubmitSnapshot(context.Background(), SnapshotRequest{
		RequestID: "snapshot-" + time.Now().Format("150405.000000000"), Version: version, Queues: queues,
	})
	if biz != nil {
		t.Fatalf("snapshot: %+v", biz)
	}
	return resp
}

func TestIncrementCascadeAndUnaffectedCalculationVersion(t *testing.T) {
	store, _ := newStore(t)
	queues := []Queue{
		testQueue("root", "", 10_000, 20_000, 100, 200, 1),
		testQueue("a", "root", 8_000, 16_000, 100, 200, 2),
		testQueue("a1", "a", 7_000, 14_000, 100, 200, 3),
		testQueue("b", "root", 6_000, 12_000, 100, 200, 2),
		testQueue("b1", "b", 5_000, 10_000, 100, 200, 3),
	}
	snapshotPolicy(t, store, 1, queues)
	cir := int64(1_500)
	resp, biz := store.SubmitIncrement(context.Background(), IncrementRequest{
		RequestID: "inc-1", BaseVersion: 1,
		QueuePatches: []QueuePatch{{QueueID: "a", CIRBPS: &cir}},
	})
	if biz != nil {
		t.Fatal(biz)
	}
	if resp.Version != 2 {
		t.Fatalf("version=%d", resp.Version)
	}
	if got, want := resp.RecomputedQueueIDs, []string{"a", "a1"}; !equalStrings(got, want) {
		t.Fatalf("recomputed=%v want %v", got, want)
	}
	if !equalStrings(resp.UnchangedQueueIDs, []string{"b", "b1", "root"}) {
		t.Fatalf("unchanged=%v", resp.UnchangedQueueIDs)
	}
	a1, be := store.GetQueueView(context.Background(), "a1", 2)
	if be != nil {
		t.Fatal(be)
	}
	if a1.EffectiveRates.EffectiveCIRBPS != 1_500 || a1.CalculationVersion != 2 || a1.ValidThroughVersion != 2 {
		t.Fatalf("a1 view=%+v", a1)
	}
	if a1.EffectiveRates.CIRSRC.Kind != "parent_limit" || a1.EffectiveRates.CIRSRC.ParentQueueID != "a" {
		t.Fatalf("a1 source=%+v", a1.EffectiveRates.CIRSRC)
	}
	b1, be := store.GetQueueView(context.Background(), "b1", 2)
	if be != nil {
		t.Fatal(be)
	}
	if b1.CalculationVersion != 1 || b1.ValidThroughVersion != 2 {
		t.Fatalf("unaffected b1 was recomputed: %+v", b1)
	}
	if _, be := store.GetQueueView(context.Background(), "b1", 1); be == nil || be.Code != CodeStaleRead {
		t.Fatalf("old read should be stale: %+v", be)
	}
	if _, be := store.SubmitIncrement(context.Background(), IncrementRequest{RequestID: "inc-old", BaseVersion: 1}); be == nil || be.Code != CodeStaleBase {
		t.Fatalf("want stale base, got %+v", be)
	}
	if _, be := store.SubmitSnapshot(context.Background(), SnapshotRequest{RequestID: "snap-old", Version: 2, Queues: queues}); be == nil || be.Code != CodeStaleSnapshot {
		t.Fatalf("want stale snapshot, got %+v", be)
	}
}

func TestConcurrentIncrementsSameBaseOnlyOneWins(t *testing.T) {
	store, _ := newStore(t)
	queues := []Queue{
		testQueue("root", "", 10_000, 20_000, 100, 200, 1),
		testQueue("a", "root", 1_000, 2_000, 100, 200, 2),
	}
	snapshotPolicy(t, store, 1, queues)
	type result struct {
		code string
		err  *BusinessError
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, id := range []string{"concurrent-a", "concurrent-b"} {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			cir := int64(1_500)
			if id == "concurrent-b" {
				cir = 2_000
			}
			resp, be := store.SubmitIncrement(context.Background(), IncrementRequest{
				RequestID: id, BaseVersion: 1,
				QueuePatches: []QueuePatch{{QueueID: "a", CIRBPS: &cir}},
			})
			if be != nil {
				results <- result{err: be}
				return
			}
			results <- result{code: resp.Code}
		}()
	}
	wg.Wait()
	close(results)
	var ok, stale int
	for r := range results {
		if r.err == nil {
			ok++
			continue
		}
		if r.err.Code != CodeStaleBase {
			t.Fatalf("unexpected loser error: %+v", r.err)
		}
		stale++
	}
	if ok != 1 || stale != 1 {
		t.Fatalf("ok=%d stale=%d", ok, stale)
	}
	current, err := store.CurrentVersion(context.Background())
	if err != nil || current != 2 {
		t.Fatalf("current=%d err=%v", current, err)
	}
}

func TestRequestIdempotency(t *testing.T) {
	store, _ := newStore(t)
	queues := []Queue{testQueue("root", "", 10_000, 20_000, 100, 200, 1)}
	snapshotPolicy(t, store, 1, queues)
	cir := int64(500)
	req := IncrementRequest{RequestID: "once", BaseVersion: 1, QueuePatches: []QueuePatch{{QueueID: "root", CIRBPS: &cir}}}
	first, be := store.SubmitIncrement(context.Background(), req)
	if be != nil {
		t.Fatal(be)
	}
	second, be := store.SubmitIncrement(context.Background(), req)
	if be != nil {
		t.Fatal(be)
	}
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if string(a) != string(b) {
		t.Fatalf("idempotent responses differ:\n%s\n%s", a, b)
	}
	if v, _ := store.CurrentVersion(context.Background()); v != 2 {
		t.Fatalf("request executed more than once, version=%d", v)
	}
}

func TestAtomicReaderBoundaryDuringPausedRebuild(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "derived.ready")
	store, err := OpenStore(StoreOptions{
		DBPath: filepath.Join(dir, "qos.db"), DataDir: filepath.Join(dir, "data"), SampleSeed: DefaultSampleSeed,
		FailpointStage: "derived", FailpointFile: ready, FailpointSleep: 350 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	queues := []Queue{
		testQueue("root", "", 10_000, 20_000, 100, 200, 1),
		testQueue("child", "root", 8_000, 16_000, 100, 200, 2),
	}
	snapshotPolicy(t, store, 1, queues)
	// The snapshot also pauses; wait for it to finish and remove its marker.
	deadline := time.Now().Add(3 * time.Second)
	for {
		v, _ := store.CurrentVersion(context.Background())
		if v == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("snapshot did not commit")
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = os.Remove(ready)
	cir := int64(1_000)
	done := make(chan *BusinessError, 1)
	go func() {
		_, be := store.SubmitIncrement(context.Background(), IncrementRequest{
			RequestID: "paused", BaseVersion: 1,
			QueuePatches: []QueuePatch{{QueueID: "root", CIRBPS: &cir}},
		})
		done <- be
	}()
	deadline = time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("failpoint was not reached")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// While the writer is paused inside its transaction, a reader must see the
	// complete old version, never a half-rebuilt child.
	old, be := store.GetQueueView(context.Background(), "child", 1)
	if be != nil {
		t.Fatalf("old complete read failed: %+v", be)
	}
	if old.EffectiveRates.EffectiveCIRBPS != 8_000 || old.CalculationVersion != 1 {
		t.Fatalf("reader saw a mixed version: %+v", old)
	}
	if be := <-done; be != nil {
		t.Fatal(be)
	}
	newView, be := store.GetQueueView(context.Background(), "child", 2)
	if be != nil {
		t.Fatal(be)
	}
	if newView.EffectiveRates.EffectiveCIRBPS != 1_000 || newView.CalculationVersion != 2 {
		t.Fatalf("new view=%+v", newView)
	}
}

func TestIncrementDeleteAndParentChangeValidation(t *testing.T) {
	store, _ := newStore(t)
	queues := []Queue{
		testQueue("root", "", 10_000, 20_000, 100, 200, 1),
		testQueue("a", "root", 8_000, 16_000, 100, 200, 2),
		testQueue("a1", "a", 7_000, 14_000, 100, 200, 3),
		testQueue("b", "root", 6_000, 12_000, 100, 200, 2),
	}
	snapshotPolicy(t, store, 1, queues)
	rootID := "root"
	resp, be := store.SubmitIncrement(context.Background(), IncrementRequest{
		RequestID: "reparent", BaseVersion: 1,
		ParentChanges: []ParentChange{{QueueID: "a1", ParentID: &rootID}},
	})
	if be != nil {
		t.Fatal(be)
	}
	if !equalStrings(resp.RecomputedQueueIDs, []string{"a1"}) {
		t.Fatalf("recomputed=%v", resp.RecomputedQueueIDs)
	}
	resp, be = store.SubmitIncrement(context.Background(), IncrementRequest{
		RequestID: "delete-a", BaseVersion: 2, DeleteQueues: []string{"a"},
	})
	if be != nil {
		t.Fatal(be)
	}
	if !equalStrings(resp.RemovedQueueIDs, []string{"a"}) {
		t.Fatalf("removed=%v", resp.RemovedQueueIDs)
	}
	// Deleting a parent cascades through its remaining subtree.
	resp, be = store.SubmitIncrement(context.Background(), IncrementRequest{
		RequestID: "delete-root", BaseVersion: 3, DeleteQueues: []string{"root"},
	})
	if be != nil {
		// The resulting policy would be empty and is intentionally invalid.
		if be.Code != CodePolicyInvalid {
			t.Fatalf("unexpected error: %+v", be)
		}
		return
	}
	t.Fatalf("expected empty resulting policy to be rejected, response=%+v", resp)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

func TestInvalidMutationsLeavePolicyAndDerivedStateUnchanged(t *testing.T) {
	store, _ := newStore(t)
	queues := []Queue{
		testQueue("root", "", 10_000, 20_000, 100, 200, 1),
		testQueue("child", "root", 8_000, 16_000, 100, 200, 2),
	}
	snapshotPolicy(t, store, 1, queues)
	before, be := store.GetQueueView(context.Background(), "child", 1)
	if be != nil {
		t.Fatal(be)
	}
	// Invalid snapshot: the write intent is failed and no policy/version changes.
	badQueues := []Queue{testQueue("bad", "", 10_000, 20_000, 201, 200, 1)}
	if _, be := store.SubmitSnapshot(context.Background(), SnapshotRequest{RequestID: "bad-snap", Version: 2, Queues: badQueues}); be == nil || be.Code != CodePolicyInvalid {
		t.Fatalf("want policy invalid, got %+v", be)
	}
	// Invalid increment: missing parent must not commit a version.
	ghost := "ghost"
	if _, be := store.SubmitIncrement(context.Background(), IncrementRequest{
		RequestID: "bad-inc", BaseVersion: 1,
		ParentChanges: []ParentChange{{QueueID: "child", ParentID: &ghost}},
	}); be == nil || be.Code != CodePolicyInvalid {
		t.Fatalf("want policy invalid, got %+v", be)
	}
	if v, err := store.CurrentVersion(context.Background()); err != nil || v != 1 {
		t.Fatalf("invalid mutation changed version: %d %v", v, err)
	}
	after, be := store.GetQueueView(context.Background(), "child", 1)
	if be != nil {
		t.Fatal(be)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("invalid mutation changed derived state:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func TestLaggingViewIsStaleRead(t *testing.T) {
	store, _ := newStore(t)
	queues := []Queue{testQueue("root", "", 10_000, 20_000, 100, 200, 1)}
	snapshotPolicy(t, store, 1, queues)
	cir := int64(500)
	if _, be := store.SubmitIncrement(context.Background(), IncrementRequest{
		RequestID: "lag", BaseVersion: 1, QueuePatches: []QueuePatch{{QueueID: "root", CIRBPS: &cir}},
	}); be != nil {
		t.Fatal(be)
	}
	// Simulate an incomplete cascade to exercise the explicit stale-read branch.
	if _, err := store.DB().Exec(`UPDATE queue_views SET valid_through_version=1 WHERE queue_id='root'`); err != nil {
		t.Fatal(err)
	}
	_, be := store.GetQueueView(context.Background(), "root", 2)
	if be == nil || be.Code != CodeStaleRead {
		t.Fatalf("want stale read, got %+v", be)
	}
	if be.Context["view_version"] != int64(2) || be.Context["observed_version"] != int64(2) {
		t.Fatalf("stale context=%+v", be.Context)
	}
	if be.Context["reason"] != "derived_view_lags_observed_version" {
		t.Fatalf("stale reason=%+v", be.Context)
	}
}
