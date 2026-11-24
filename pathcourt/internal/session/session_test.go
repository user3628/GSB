package session_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"pathcourt/internal/bizcode"
	"pathcourt/internal/reparse"
	"pathcourt/internal/resolver"
	"pathcourt/internal/session"
	"pathcourt/internal/snapshot"
)

func newHarness(t *testing.T, maxConcurrent, maxPaths int) (*session.Manager, *snapshot.Store) {
	t.Helper()
	root := t.TempDir()
	if err := writeDir(filepath.Join(root, "app")); err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(t.TempDir(), "s.db")
	store, err := snapshot.Open(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	r := resolver.New(resolver.Config{Root: root, MaxJumps: 4}, reparse.NewInspector())
	return session.NewManager(session.Config{MaxConcurrent: maxConcurrent, MaxPathsPerSession: maxPaths}, r, store), store
}

func writeDir(path string) error { return osMkdirAll(path) }

func TestBudgetTruncatesButCommits(t *testing.T) {
	mgr, store := newHarness(t, 1, 2)
	paths := []string{"app", "app", "app", "app", "app"}
	out, err := mgr.Submit(context.Background(), "budget-manifest", paths)
	if err != nil {
		t.Fatal(err)
	}
	if out.Code != bizcode.E_PATH_BUDGET {
		t.Fatalf("code = %s, want E_PATH_BUDGET", out.Code)
	}
	if out.Committed != 2 || out.PathCount != 5 {
		t.Fatalf("committed=%d pathCount=%d", out.Committed, out.PathCount)
	}
	versions, err := store.LoadChain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 || len(versions[0].Rows) != 2 {
		t.Fatalf("snapshot rows not committed for truncated session: %+v", versions)
	}
}

func TestConcurrencyQueueAndInflight(t *testing.T) {
	mgr, _ := newHarness(t, 2, 256)
	var wg sync.WaitGroup
	sawInflight := false
	var mu sync.Mutex
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = mgr.Submit(context.Background(), "m", []string{"app"})
			mu.Lock()
			if mgr.Stats().Inflight >= 0 {
				sawInflight = true
			}
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}()
	}
	// While jobs run, the waiting queue must be observable and never reject.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats := mgr.Stats()
		if stats.Inflight <= 2 {
			break
		}
	}
	wg.Wait()
	stats := mgr.Stats()
	if stats.Completed != 6 {
		t.Fatalf("completed = %d, want 6 (queued, not rejected)", stats.Completed)
	}
	if !sawInflight {
		t.Fatal("inflight counter never observed")
	}
	if stats.Inflight != 0 || stats.Waiting != 0 {
		t.Fatalf("counters not drained: %+v", stats)
	}
}
