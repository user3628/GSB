package cache

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func testSamples(t *testing.T) []EntryInput {
	t.Helper()
	inputs, err := LoadSampleFile(filepath.Join("..", "..", "samples", "entries.json"))
	if err != nil {
		t.Fatalf("load samples: %v", err)
	}
	return inputs
}

func openTestStore(t *testing.T, capacity int64, clock Clock) *Store {
	t.Helper()
	store, err := Open(context.Background(), Config{
		Dir:           t.TempDir(),
		CapacityBytes: capacity,
		Clock:         clock,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func sample(t *testing.T, inputs []EntryInput, stage string) EntryInput {
	t.Helper()
	input, err := SampleByName(inputs, stage)
	if err != nil {
		t.Fatal(err)
	}
	return input
}

func withContent(input EntryInput, content string) EntryInput {
	input.ArtifactContent = []byte(content)
	input.ArtifactSize = int64(len(input.ArtifactContent))
	input.ArtifactDigest = DigestBytes(input.ArtifactContent)
	return input
}

func TestWriteHitAndInvalidTransition(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	store := openTestStore(t, 1024, clock)
	input := sample(t, testSamples(t), "build")

	put, err := store.Put(ctx, input)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if put.Entry.State != StateAvailable || put.Idempotent {
		t.Fatalf("unexpected put result: %+v", put)
	}
	again, err := store.Put(ctx, input)
	if err != nil {
		t.Fatalf("idempotent put: %v", err)
	}
	if !again.Idempotent || again.Entry.EntryID != put.Entry.EntryID {
		t.Fatalf("expected idempotent same entry, got %+v", again)
	}

	hit, err := store.Hit(ctx, input.Key(), input.ArtifactDigest)
	if err != nil {
		t.Fatalf("hit: %v", err)
	}
	if hit.Entry.Hits != 1 || !bytes.Equal(hit.Content, input.ArtifactContent) {
		t.Fatalf("unexpected hit: hits=%d content=%q", hit.Entry.Hits, hit.Content)
	}

	before := hit.Entry
	_, err = store.Transition(ctx, input.Key(), StatePending)
	if !IsCode(err, CodeInvalidTransition) {
		t.Fatalf("expected E_INVALID_TRANSITION, got %v", err)
	}
	after, ok := store.EntryByKey(input.Key())
	if !ok || after.State != StateAvailable || after.Version != before.Version {
		t.Fatalf("illegal transition mutated persisted state: before=%+v after=%+v", before, after)
	}
}

func TestDigestMismatchAndExpiry(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	store := openTestStore(t, 1024, clock)
	inputs := testSamples(t)
	build := sample(t, inputs, "build")
	put, err := store.Put(ctx, build)
	if err != nil {
		t.Fatal(err)
	}
	path, err := store.artifactPath(put.Entry.ArtifactDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("tampered-alpha\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = store.Hit(ctx, build.Key(), "")
	if !IsCode(err, CodeDigestMismatch) {
		t.Fatalf("expected E_DIGEST_MISMATCH, got %v", err)
	}
	if count := store.HitCount(put.Entry.CacheKey); count != 0 {
		t.Fatalf("digest mismatch updated hit count: %d", count)
	}
	if err := os.WriteFile(path, build.ArtifactContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Hit(ctx, build.Key(), ""); err != nil {
		t.Fatalf("hit after repair: %v", err)
	}

	lint := sample(t, inputs, "lint")
	expiredPut, err := store.Put(ctx, lint)
	if err != nil {
		t.Fatal(err)
	}
	if expiredPut.Entry.State != StateAvailable {
		t.Fatalf("unexpected expired sample state: %s", expiredPut.Entry.State)
	}
	_, err = store.Hit(ctx, lint.Key(), "")
	if !IsCode(err, CodeEntryExpired) {
		t.Fatalf("expected E_ENTRY_EXPIRED, got %v", err)
	}
	expiredEntry, _ := store.EntryByKey(lint.Key())
	if expiredEntry.State != StateStale {
		t.Fatalf("expired entry was not moved to STALE: %s", expiredEntry.State)
	}
}

func TestConflictingWriteAndConcurrentIdempotency(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	store := openTestStore(t, 1024, clock)
	inputs := testSamples(t)
	build := sample(t, inputs, "build")
	other := withContent(build, "artifact-omega\n")
	if _, err := store.Put(ctx, build); err != nil {
		t.Fatal(err)
	}
	_, err := store.Put(ctx, other)
	if !IsCode(err, CodeConflictingWrite) {
		t.Fatalf("expected E_CONFLICTING_WRITE, got %v", err)
	}

	concurrentStore := openTestStore(t, 1024, clock)
	results := make(chan bool, 16)
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := concurrentStore.Put(ctx, build)
			if err != nil {
				errs <- err
				return
			}
			results <- result.Idempotent
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent put: %v", err)
	}
	nonIdempotent := 0
	for idempotent := range results {
		if !idempotent {
			nonIdempotent++
		}
	}
	if nonIdempotent != 1 {
		t.Fatalf("expected exactly one effective concurrent write, got %d", nonIdempotent)
	}
	entry, ok := concurrentStore.EntryByKey(build.Key())
	if !ok || entry.State != StateAvailable || entry.Hits != 0 {
		t.Fatalf("unexpected concurrent entry: %+v", entry)
	}
}

func TestConcurrentHitsCommitOneCompleteVersion(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	store := openTestStore(t, 1024, clock)
	input := sample(t, testSamples(t), "build")
	if _, err := store.Put(ctx, input); err != nil {
		t.Fatal(err)
	}

	const readers = 24
	counts := make(chan int64, readers)
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hit, err := store.Hit(ctx, input.Key(), "")
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(hit.Content, input.ArtifactContent) {
				errs <- errors.New("reader observed incomplete artifact")
				return
			}
			counts <- hit.Entry.Hits
		}()
	}
	wg.Wait()
	close(counts)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent hit: %v", err)
	}
	seen := make([]int64, 0, readers)
	for count := range counts {
		seen = append(seen, count)
	}
	sort.Slice(seen, func(i, j int) bool { return seen[i] < seen[j] })
	for index, count := range seen {
		want := int64(index + 1)
		if count != want {
			t.Fatalf("hit statistics skipped or repeated: got %v", seen)
		}
	}
}

func TestDeterministicCapacityAndRetentionEviction(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	store := openTestStore(t, 15, clock)
	inputs := testSamples(t)
	build := sample(t, inputs, "build")
	test := sample(t, inputs, "test")
	pkg := sample(t, inputs, "package")
	expired := sample(t, inputs, "lint")

	if _, err := store.Put(ctx, build); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if _, err := store.Hit(ctx, build.Key(), ""); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if _, err := store.Put(ctx, test); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if _, err := store.Put(ctx, pkg); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if _, err := store.Put(ctx, expired); err != nil {
		t.Fatal(err)
	}

	result, err := store.Evict(ctx)
	if err != nil {
		t.Fatalf("evict: %v", err)
	}
	gotOrder := make([]string, 0, len(result.Evicted))
	for _, item := range result.Evicted {
		gotOrder = append(gotOrder, item.StageID)
	}
	wantOrder := []string{"build", "test", "lint"}
	if len(gotOrder) != len(wantOrder) {
		t.Fatalf("unexpected eviction order: got=%v want=%v result=%+v", gotOrder, wantOrder, result)
	}
	for i := range wantOrder {
		if gotOrder[i] != wantOrder[i] {
			t.Fatalf("unexpected eviction order: got=%v want=%v", gotOrder, wantOrder)
		}
	}
	if result.UsedBytesAfter > store.CapacityBytes() {
		t.Fatalf("capacity not restored: used=%d cap=%d", result.UsedBytesAfter, store.CapacityBytes())
	}
	if result.ReleasedBytes != 46 {
		t.Fatalf("unexpected released bytes: %d", result.ReleasedBytes)
	}
	for _, stage := range []string{"build", "test", "lint"} {
		input := sample(t, inputs, stage)
		entry, ok := store.EntryByKey(input.Key())
		if !ok || entry.State != StateEvicted {
			t.Fatalf("%s not evicted: %+v", stage, entry)
		}
		path, _ := store.artifactPath(input.ArtifactDigest)
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s artifact was not physically released: %v", stage, err)
		}
	}
	remaining, ok := store.EntryByKey(pkg.Key())
	if !ok || remaining.State != StateAvailable {
		t.Fatalf("package entry should remain AVAILABLE: %+v", remaining)
	}
	if _, err := store.Hit(ctx, build.Key(), ""); !IsCode(err, CodeCacheMiss) {
		t.Fatalf("evicted hit should be E_CACHE_MISS, got %v", err)
	}
	_, buildCacheKey, err := NormalizeKey(build.Key())
	if err != nil {
		t.Fatal(err)
	}
	var historyHits int64
	if err := store.db.QueryRow(`SELECT hits FROM evictions WHERE cache_key=?`, buildCacheKey).Scan(&historyHits); err != nil {
		t.Fatalf("query eviction history: %v", err)
	}
	if historyHits != 1 {
		t.Fatalf("eviction history lost hits: %d", historyHits)
	}

	rebuilt, err := store.Put(ctx, build)
	if err != nil {
		t.Fatalf("rewrite evicted key: %v", err)
	}
	if rebuilt.Entry.EntryID == result.Evicted[0].EntryID || rebuilt.Entry.State != StateAvailable {
		t.Fatalf("evicted key was not rebuilt as a new generation: %+v", rebuilt.Entry)
	}
	if hit, err := store.Hit(ctx, build.Key(), ""); err != nil || hit.Entry.Hits != 1 {
		t.Fatalf("rebuilt entry is not reusable: hit=%+v err=%v", hit.Entry, err)
	}
}

func TestExplicitStaleToEvictedReleasesArtifact(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	store := openTestStore(t, 1024, clock)
	input := sample(t, testSamples(t), "build")
	put, err := store.Put(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transition(ctx, input.Key(), StateStale); err != nil {
		t.Fatalf("available->stale: %v", err)
	}
	evicted, err := store.Transition(ctx, input.Key(), StateEvicted)
	if err != nil {
		t.Fatalf("stale->evicted: %v", err)
	}
	if evicted.State != StateEvicted {
		t.Fatalf("unexpected state: %s", evicted.State)
	}
	path, _ := store.artifactPath(put.Entry.ArtifactDigest)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("artifact not released by explicit eviction: %v", err)
	}
}

func TestCacheDirectoryWithSpaces(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "cache dir with spaces")
	store, err := Open(ctx, Config{Dir: dir, CapacityBytes: 1024})
	if err != nil {
		t.Fatalf("open cache with spaces in path: %v", err)
	}
	defer store.Close()
	input := sample(t, testSamples(t), "build")
	if _, err := store.Put(ctx, input); err != nil {
		t.Fatalf("put with spaces in path: %v", err)
	}
	if _, err := store.Hit(ctx, input.Key(), ""); err != nil {
		t.Fatalf("hit with spaces in path: %v", err)
	}
}

func TestSharedArtifactReferenceIsHeldUntilLastEntryEvicted(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	store := openTestStore(t, 1024, clock)
	inputs := testSamples(t)
	first := sample(t, inputs, "build")
	second := withContent(sample(t, inputs, "test"), string(first.ArtifactContent))
	if _, err := store.Put(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transition(ctx, first.Key(), StateStale); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Evict(ctx); err != nil {
		t.Fatalf("evict first shared entry: %v", err)
	}
	path, _ := store.artifactPath(first.ArtifactDigest)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("shared artifact removed too early: %v", err)
	}
	if _, err := store.Hit(ctx, second.Key(), ""); err != nil {
		t.Fatalf("remaining shared entry cannot hit: %v", err)
	}
}
