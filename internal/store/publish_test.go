package store

import (
	"path/filepath"
	"sync"
	"testing"

	"qaudit/internal/model"
)

func testArtifact(layer string) *model.LayerCalibration {
	return &model.LayerCalibration{
		LayerName: layer, BitWidth: 8, Symmetric: true,
		ActivationParam: model.TensorParams{Scale: 0.1, Channel: -1},
		OutputParam:     model.TensorParams{Scale: 0.2, Channel: -1},
	}
}

func TestConcurrentPublishOnlyOneWins(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "snap.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.UpsertVersion(1, "fp-1", "normal"); err != nil {
		t.Fatal(err)
	}
	guard, err := NewPublishGuard(st)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()

	var wg sync.WaitGroup
	results := make(chan error, 16)
	// Many workers race to publish the first sequence slot for the same layer.
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- guard.Publish(PublishRequest{
				Version: 1, ExpectedSeq: 1, LayerName: "fc1",
				InputFP: "in", PolicyFP: "pol", Digest: "d1", Artifact: testArtifact("fc1"),
			})
		}()
	}
	wg.Wait()
	close(results)
	ok, stale := 0, 0
	for err := range results {
		if err == nil {
			ok++
		} else if model.CodeOf(err) == model.CodeStaleVersion {
			stale++
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 1 || stale != 15 {
		t.Fatalf("want exactly 1 success and 15 stale, got ok=%d stale=%d", ok, stale)
	}
	rows, err := st.ListSnapshots(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("one valid record per layer/version, got %d", len(rows))
	}
}

func TestOutOfOrderAndBehindRejected(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(filepath.Join(dir, "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.UpsertVersion(1, "a", "normal"); err != nil {
		t.Fatal(err)
	}
	g, err := NewPublishGuard(st)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	pub := func(seq int64, layer string, version int64) error {
		return g.Publish(PublishRequest{
			Version: version, ExpectedSeq: seq, LayerName: layer,
			InputFP: "in", PolicyFP: "pol", Digest: layer, Artifact: testArtifact(layer),
		})
	}
	if err := pub(1, "fc1", 1); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	// Out-of-order sequence (claims seq 3 while only one row exists).
	if err := pub(3, "fc2", 1); err == nil || model.CodeOf(err) != model.CodeStaleVersion {
		t.Fatalf("expected stale out-of-order, got %v", err)
	}
	// Correct next sequence succeeds.
	if err := pub(2, "fc2", 1); err != nil {
		t.Fatalf("second publish: %v", err)
	}
	// Duplicate layer rejected.
	if err := pub(3, "fc1", 1); err == nil || model.CodeOf(err) != model.CodeStaleVersion {
		t.Fatalf("expected stale duplicate, got %v", err)
	}
	// A newer version exists; publishing to an older version is behind/stale.
	if err := st.UpsertVersion(2, "b", "replacement"); err != nil {
		t.Fatal(err)
	}
	if err := pub(1, "fc1", 1); err == nil || model.CodeOf(err) != model.CodeStaleVersion {
		t.Fatalf("expected behind-version stale, got %v", err)
	}
}

func TestFileLockMutualExclusion(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(filepath.Join(dir, "l.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	g1, err := NewPublishGuard(st)
	if err != nil {
		t.Fatal(err)
	}
	defer g1.Close()
	ok, err := g1.Lock().TryLock()
	if err != nil || !ok {
		t.Fatalf("first TryLock should succeed: ok=%v err=%v", ok, err)
	}
	st2, err := Open(filepath.Join(dir, "l2.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	g2, err := NewPublishGuard(st2)
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	// Point g2 at the same lock file explicitly.
	g2override, err := NewFileLock(st.lockPathForTest())
	if err != nil {
		t.Fatal(err)
	}
	defer g2override.Close()
	ok2, err := g2override.TryLock()
	if err != nil || ok2 {
		t.Fatalf("second TryLock must fail while held: ok=%v err=%v", ok2, err)
	}
	if err := g1.Lock().Unlock(); err != nil {
		t.Fatal(err)
	}
	ok3, err := g2override.TryLock()
	if err != nil || !ok3 {
		t.Fatalf("TryLock after unlock should succeed: ok=%v err=%v", ok3, err)
	}
}
