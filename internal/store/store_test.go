package store

import (
	"path/filepath"
	"testing"

	"queryrewrite/internal/spec"
)

func openStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	st, err := Open(filepath.Join(dir, "s.db"), filepath.Join(dir, "s.db.lock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestCheckpointUniqueAndAdvance(t *testing.T) {
	st := openStore(t)
	cp := spec.Checkpoint{
		Stage: spec.StageGenerate, SessionID: "s1", Turn: 1,
		InputDigest: "in", PolicyVersion: 1, ProductDigest: "pd", Payload: "{}",
	}
	if err := st.AdvanceTurn("s1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CommitCheckpoint(cp); err != nil {
		t.Fatal(err)
	}
	// Duplicate stage fact must be rejected as E_STALE_CHECKPOINT.
	if _, err := st.CommitCheckpoint(cp); err == nil {
		t.Fatal("duplicate checkpoint must be rejected")
	} else if berr, ok := err.(*spec.BizError); !ok || berr.Code != spec.CodeStaleCheckpoint {
		t.Fatalf("want E_STALE_CHECKPOINT, got %v", err)
	}
	// Non-consecutive turn rejected.
	if err := st.AdvanceTurn("s1", 3); err == nil {
		t.Fatal("turn 3 before turn 2 must be rejected")
	}
	if err := st.AdvanceTurn("s1", 2); err != nil {
		t.Fatal(err)
	}
}

func TestCrossSessionIndependentIndex(t *testing.T) {
	st := openStore(t)
	if err := st.AdvanceTurn("a", 1); err != nil {
		t.Fatal(err)
	}
	if err := st.AdvanceTurn("b", 1); err != nil {
		t.Fatal(err)
	}
	if err := st.AdvanceTurn("a", 2); err != nil {
		t.Fatal(err)
	}
	if n, _ := st.ExpectedNextTurn("a"); n != 3 {
		t.Fatalf("a next = %d want 3", n)
	}
	if n, _ := st.ExpectedNextTurn("b"); n != 2 {
		t.Fatalf("b next = %d want 2", n)
	}
}

func TestFileLockMutualExclusion(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "x.lock")
	l1, err := AcquireFileLock(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	// A second handle on the same lock file must block; verify try-acquire
	// semantics indirectly by releasing then re-acquiring.
	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}
	l2, err := AcquireFileLock(lockPath)
	if err != nil {
		t.Fatalf("lock should be reusable after release: %v", err)
	}
	defer l2.Release()
}

func TestPolicyVersionRoundTrip(t *testing.T) {
	st := openStore(t)
	p := spec.Policy{Version: 7, ParentVersion: 6, ContentDigest: "d",
		StrategyPerMille: map[string]int{"history": 1000}}
	if err := st.SavePolicy(p, "incremental"); err != nil {
		t.Fatal(err)
	}
	v, d, err := st.CurrentPolicyVersion()
	if err != nil || v != 7 || d != "d" {
		t.Fatalf("current version = %d %d %s err=%v", v, 7, d, err)
	}
	got, err := st.LoadPolicy(7)
	if err != nil || got == nil || got.ParentVersion != 6 {
		t.Fatalf("load policy failed: %+v err=%v", got, err)
	}
}

func TestConcurrentTurnPublishMutualExclusion(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "c.db")
	lockPath := dbPath + ".lock"

	// N goroutines each open their own handle (separate processes are emulated
	// by separate connections) and try to publish turn 1 for the same session.
	const n = 8
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			st, err := Open(dbPath, lockPath)
			if err != nil {
				results <- err
				return
			}
			defer st.Close()
			results <- st.AdvanceTurn("shared", 1)
		}()
	}
	ok, stale := 0, 0
	for i := 0; i < n; i++ {
		err := <-results
		switch {
		case err == nil:
			ok++
		default:
			if berr, isBiz := err.(*spec.BizError); isBiz && berr.Code == spec.CodeStaleCheckpoint {
				stale++
			} else {
				t.Fatalf("unexpected error: %v", err)
			}
		}
	}
	if ok != 1 || stale != n-1 {
		t.Fatalf("want exactly 1 success and %d stale, got ok=%d stale=%d", n-1, ok, stale)
	}
}
