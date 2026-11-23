package cache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotSwitchValidationAndResourceRelease(t *testing.T) {
	ctx := context.Background()
	clock := NewManualClock(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	store := openTestStore(t, 1024, clock)
	inputs := testSamples(t)
	build := sample(t, inputs, "build")
	testEntry := sample(t, inputs, "test")
	if _, err := store.Put(ctx, build); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, testEntry); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Hit(ctx, build.Key(), ""); err != nil {
		t.Fatal(err)
	}

	exported, err := store.ExportSnapshot(ctx)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if exported.Sequence != 1 || len(exported.Entries) != 2 {
		t.Fatalf("unexpected export: %+v", exported)
	}
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := WriteSnapshotFile(path, exported); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	loaded, err := LoadSnapshotFile(path)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	// Equal sequence is not monotonic.
	if _, err := store.SwitchSnapshot(ctx, loaded); !IsCode(err, CodeSnapshotSequence) {
		t.Fatalf("expected E_SNAPSHOT_SEQUENCE, got %v", err)
	}
	if _, err := store.Hit(ctx, build.Key(), ""); err != nil {
		t.Fatalf("old catalog unavailable after sequence rejection: %v", err)
	}

	corrupt := cloneSnapshotForTest(loaded)
	corrupt.Entries[0].Hits++
	if _, err := store.SwitchSnapshot(ctx, corrupt); !IsCode(err, CodeSnapshotInvalid) {
		t.Fatalf("expected E_SNAPSHOT_INVALID, got %v", err)
	}
	if _, err := store.Hit(ctx, build.Key(), ""); err != nil {
		t.Fatalf("old catalog unavailable after checksum rejection: %v", err)
	}

	digestBroken := cloneSnapshotForTest(loaded)
	digestBroken.Sequence = loaded.Sequence + 10
	for index := range digestBroken.Entries {
		if digestBroken.Entries[index].StageID == "build" {
			digestBroken.Entries[index].ArtifactContentBase64 = "dGFtcGVyZWQtYWxwaGEK"
		}
	}
	if err := SealSnapshot(&digestBroken); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SwitchSnapshot(ctx, digestBroken); !IsCode(err, CodeDigestMismatch) {
		t.Fatalf("expected E_DIGEST_MISMATCH, got %v", err)
	}
	if _, err := store.Hit(ctx, build.Key(), ""); err != nil {
		t.Fatalf("old catalog unavailable after digest rejection: %v", err)
	}

	replacement := cloneSnapshotForTest(loaded)
	replacement.Sequence = loaded.Sequence + 20
	filtered := make([]SnapshotEntry, 0, 1)
	for _, entry := range replacement.Entries {
		if entry.StageID == "build" {
			filtered = append(filtered, entry)
		}
	}
	replacement.Entries = filtered
	if err := SealSnapshot(&replacement); err != nil {
		t.Fatal(err)
	}
	switchResult, err := store.SwitchSnapshot(ctx, replacement)
	if err != nil {
		t.Fatalf("switch: %v", err)
	}
	if switchResult.Sequence != replacement.Sequence || switchResult.ReleasedArtifacts != 1 {
		t.Fatalf("unexpected switch result: %+v", switchResult)
	}
	testPath, _ := store.artifactPath(testEntry.ArtifactDigest)
	if _, err := os.Stat(testPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old artifact was not released: %v", err)
	}
	hit, err := store.Hit(ctx, build.Key(), "")
	if err != nil {
		t.Fatalf("hit after snapshot switch: %v", err)
	}
	if hit.Entry.Hits != 2 || hit.Entry.EntryID != exported.Entries[0].EntryID && hit.Entry.Hits != 2 {
		t.Fatalf("snapshot hit statistics were not adopted: %+v", hit.Entry)
	}
	if _, ok := store.EntryByKey(testEntry.Key()); ok {
		t.Fatal("removed snapshot entry is still active")
	}
}

func cloneSnapshotForTest(snapshot Snapshot) Snapshot {
	clone := snapshot
	clone.Entries = append([]SnapshotEntry(nil), snapshot.Entries...)
	return clone
}
