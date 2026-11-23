package engine

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"sandboxpolicy/business"
	"sandboxpolicy/policy"
	"sandboxpolicy/snapshot"
)

func TestVersionChainAndHistoricalReplay(t *testing.T) {
	ctx := context.Background()
	store, manager, dbPath := newTestManager(t, Config{MaxInflight: 2, QueueTimeout: time.Second})
	_ = dbPath
	baseline := loadBaseline(t)
	if _, err := store.Initialize(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	changes := []string{"01-add-cache.json", "02-delete-directory.json", "03-tighten-process.json", "04-delete-registry.json"}
	for i, name := range changes {
		cs := loadChange(t, name)
		result, err := manager.Apply(ctx, cs)
		if err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
		if result.Version != i+2 || result.ParentVersion != i+1 {
			t.Fatalf("version sequence broken at %s: %+v", name, result)
		}
	}
	current, chain, err := store.Current(ctx, baseline.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if current.Version != 5 || len(chain.Entries) != 5 || chain.Warning != nil {
		t.Fatalf("current chain = %+v", chain)
	}
	for i, entry := range chain.Entries {
		if entry.Version != i+1 || entry.ParentVersion != i {
			t.Fatalf("entry %d = %+v", i, entry)
		}
	}
	third, _, err := store.SnapshotAt(ctx, baseline.Namespace, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(third.Closure.Direct); got != 2 {
		t.Fatalf("historical closure direct = %d", got)
	}
	if current.Digest == chain.Entries[0].Digest {
		t.Fatal("snapshot content did not evolve")
	}
}

func TestFailedApplyLeavesCommittedSnapshotUntouched(t *testing.T) {
	ctx := context.Background()
	store, manager, _ := newTestManager(t, Config{MaxInflight: 2, QueueTimeout: time.Second})
	baseline := loadBaseline(t)
	if _, err := store.Initialize(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	before, _, err := store.Current(ctx, baseline.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := policy.ParseChangeSetFile(filepath.Join("..", "samples", "invalid", "missing-reference.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.Apply(ctx, cs)
	if err == nil || business.From(err).Code != business.CodePolicyMalformed {
		t.Fatalf("unexpected apply error: %v", err)
	}
	after, chain, err := store.Current(ctx, baseline.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if after.Version != 1 || after.Digest != before.Digest || len(chain.Entries) != 1 {
		t.Fatalf("failed apply polluted state: %+v", chain)
	}
	events, err := store.Events(ctx, baseline.Namespace, 20)
	if err != nil {
		t.Fatal(err)
	}
	foundFailure := false
	for _, event := range events {
		if event.EventType == "failed" && event.Code == string(business.CodePolicyMalformed) {
			foundFailure = true
		}
	}
	if !foundFailure {
		t.Fatalf("failed apply was not recorded: %+v", events)
	}
}

func TestVersionGapFallsBackAndBlocksAdvance(t *testing.T) {
	ctx := context.Background()
	store, manager, _ := newTestManager(t, Config{MaxInflight: 2, QueueTimeout: time.Second})
	baseline := loadBaseline(t)
	if _, err := store.Initialize(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Apply(ctx, loadChange(t, "01-add-cache.json")); err != nil {
		t.Fatal(err)
	}
	if err := store.InjectVersionGap(ctx, baseline.Namespace, 4); err != nil {
		t.Fatal(err)
	}
	current, chain, err := store.Current(ctx, baseline.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if current.Version != 2 || chain.Warning == nil || chain.Warning.Code != business.CodeSnapshotVersion {
		t.Fatalf("gap was not rejected with fallback: current=%d warning=%+v", current.Version, chain.Warning)
	}
	if _, _, err := store.SnapshotAt(ctx, baseline.Namespace, 4); err == nil || business.From(err).Code != business.CodeSnapshotVersion {
		t.Fatalf("gap snapshot was accepted: %v", err)
	}
	_, err = manager.Apply(ctx, loadChange(t, "02-delete-directory.json"))
	if err == nil || business.From(err).Code != business.CodeSnapshotVersion {
		t.Fatalf("apply advanced past gap: %v", err)
	}
	after, chainAfter, err := store.Current(ctx, baseline.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if after.Version != 2 || after.Digest != current.Digest || len(chainAfter.Entries) != 2 {
		t.Fatalf("gap fallback prefix changed: %+v", chainAfter)
	}
}

func TestSameVersionContentReplacementIsRejected(t *testing.T) {
	ctx := context.Background()
	store, manager, _ := newTestManager(t, Config{MaxInflight: 2, QueueTimeout: time.Second})
	baseline := loadBaseline(t)
	if _, err := store.Initialize(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Apply(ctx, loadChange(t, "01-add-cache.json")); err != nil {
		t.Fatal(err)
	}
	before, _, err := store.SnapshotAt(ctx, baseline.Namespace, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InjectDigestMismatch(ctx, baseline.Namespace, 2); err != nil {
		t.Fatal(err)
	}
	current, chain, err := store.Current(ctx, baseline.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if current.Version != 1 || chain.Warning == nil || chain.Warning.InvalidVersion != 2 {
		t.Fatalf("replacement was not rejected: current=%d warning=%+v", current.Version, chain.Warning)
	}
	if _, _, err := store.SnapshotAt(ctx, baseline.Namespace, 2); err == nil {
		t.Fatal("mixed same-version content was accepted")
	}
	if before.Digest == chain.Entries[0].Digest {
		t.Fatal("test did not preserve persisted digest")
	}
}

func TestConcurrentSameNamespaceDoesNotInterleaveVersions(t *testing.T) {
	ctx := context.Background()
	store, manager, _ := newTestManager(t, Config{MaxInflight: 3, QueueTimeout: 2 * time.Second})
	baseline := loadBaseline(t)
	if _, err := store.Initialize(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	base := loadChange(t, "01-add-cache.json")
	const workers = 12
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := manager.Apply(ctx, base)
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	conflicts := 0
	for err := range results {
		if err == nil {
			success++
			continue
		}
		if business.From(err).Code == business.CodeChangeConflict {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent error: %v", err)
		}
	}
	if success != 1 || conflicts != workers-1 {
		t.Fatalf("success=%d conflicts=%d", success, conflicts)
	}
	current, chain, err := store.Current(ctx, baseline.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if current.Version != 2 || len(chain.Entries) != 2 || chain.Entries[0].Version != 1 || chain.Entries[1].Version != 2 {
		t.Fatalf("versions interleaved: %+v", chain)
	}
	events, err := store.Events(ctx, baseline.Namespace, 100)
	if err != nil {
		t.Fatal(err)
	}
	hasConflict := false
	hasQueued := false
	for _, event := range events {
		if event.EventType == "conflict" && event.Namespace == baseline.Namespace && event.Version == 2 {
			hasConflict = true
		}
		if event.EventType == "queued" && event.Namespace == baseline.Namespace && event.Operation == "apply" {
			hasQueued = true
		}
	}
	if !hasConflict {
		t.Fatalf("conflict event missing: %+v", events)
	}
	if !hasQueued {
		t.Fatalf("queue event missing: %+v", events)
	}
}

func TestQueueWaitTimeoutReturnsContention(t *testing.T) {
	ctx := context.Background()
	store, manager, dbPath := newTestManager(t, Config{MaxInflight: 1, QueueTimeout: 80 * time.Millisecond})
	baseline := loadBaseline(t)
	if _, err := store.Initialize(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	blocker, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbPath)+"?_pragma=busy_timeout(2000)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	tx, err := blocker.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO change_events(namespace, version, operation, event_type, code, detail, created_at) VALUES('blocker',0,'test','blocker','','', '')`); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = manager.Apply(ctx, loadChange(t, "01-add-cache.json"))
	elapsed := time.Since(start)
	_ = tx.Rollback()
	if err == nil || business.From(err).Code != business.CodeChangeContention {
		t.Fatalf("expected contention, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("contention waited too long: %s", elapsed)
	}
	current, _, err := store.Current(ctx, baseline.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if current.Version != 1 {
		t.Fatalf("contention advanced version to %d", current.Version)
	}
}

func TestInitializeTwiceIsConflict(t *testing.T) {
	ctx := context.Background()
	store, _, _ := newTestManager(t, Config{})
	baseline := loadBaseline(t)
	if _, err := store.Initialize(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	_, err := store.Initialize(ctx, baseline)
	if err == nil || business.From(err).Code != business.CodeChangeConflict {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func newTestManager(t *testing.T, cfg Config) (*snapshot.Store, *Manager, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	store, err := snapshot.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, NewManager(store, cfg), dbPath
}

func loadBaseline(t *testing.T) *policy.Policy {
	t.Helper()
	baseline, err := policy.ParsePolicyFile(filepath.Join("..", "samples", "policies", "baseline.json"))
	if err != nil {
		t.Fatal(err)
	}
	return baseline
}

func loadChange(t *testing.T, name string) *policy.ChangeSet {
	t.Helper()
	cs, err := policy.ParseChangeSetFile(filepath.Join("..", "samples", "changes", name))
	if err != nil {
		t.Fatal(err)
	}
	return cs
}
