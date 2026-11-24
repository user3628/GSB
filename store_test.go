package main

import (
	"testing"
)

func inspectAndCommit(t *testing.T, samples, state, repo string, budget int) (*Store, int) {
	t.Helper()
	res, err := InspectRepo(samples, repo, budget)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(state)
	if err != nil {
		t.Fatal(err)
	}
	v, err := store.CommitSnapshot(repo, res, false)
	if err != nil {
		t.Fatal(err)
	}
	return store, v
}

func TestSnapshotVersionsIncrement(t *testing.T) {
	samples := genSamples(t, &Plan{Repos: []PlanRepo{{Name: "r", Generations: 3}}})
	state := t.TempDir()
	store, v1 := inspectAndCommit(t, samples, state, "r", 0)
	defer store.Close()
	res, _ := InspectRepo(samples, "r", 0)
	v2, err := store.CommitSnapshot("r", res, false)
	if err != nil {
		t.Fatal(err)
	}
	if v1 != 1 || v2 != 2 {
		t.Fatalf("versions should increment 1,2; got %d,%d", v1, v2)
	}
	snaps, code, err := store.LoadSnapshots("r")
	if err != nil {
		t.Fatal(err)
	}
	if code != "" || len(snaps) != 2 {
		t.Fatalf("expected 2 contiguous snapshots, code=%q n=%d", code, len(snaps))
	}
	// Old snapshots are retained for replay. Identical payloads digest
	// identically (determinism); a content change must change the digest.
	if snaps[0].Digest != snaps[1].Digest {
		t.Fatal("identical payloads must digest identically")
	}
	res2, _ := InspectRepo(samples, "r", 2)
	if _, err := store.CommitSnapshot("r", res2, false); err != nil {
		t.Fatal(err)
	}
	snaps3, _, err := store.LoadSnapshots("r")
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps3) != 3 || snaps3[2].Digest == snaps3[1].Digest {
		t.Fatal("snapshot comparison must follow content, not version numbers")
	}
}

func TestSnapshotGapFallback(t *testing.T) {
	samples := genSamples(t, &Plan{Repos: []PlanRepo{{Name: "r", Generations: 3}}})
	state := t.TempDir()
	store, _ := inspectAndCommit(t, samples, state, "r", 0)
	defer store.Close()
	res, _ := InspectRepo(samples, "r", 0)
	if _, err := store.CommitSnapshot("r", res, false); err != nil {
		t.Fatal(err)
	}
	gapVersion, err := store.InjectSnapshotGap("r")
	if err != nil {
		t.Fatal(err)
	}
	if gapVersion != 4 {
		t.Fatalf("expected injected gap version 4, got %d", gapVersion)
	}
	snaps, code, err := store.LoadSnapshots("r")
	if err != nil {
		t.Fatal(err)
	}
	if code != ESnapshotVersion {
		t.Fatalf("expected %s, got %q", ESnapshotVersion, code)
	}
	if len(snaps) != 2 {
		t.Fatalf("fallback must keep only contiguous prefix, got %d", len(snaps))
	}
	// Fallback must not advance the version chain.
	snapsAgain, _, _ := store.LoadSnapshots("r")
	if len(snapsAgain) != 2 {
		t.Fatal("fallback advanced state")
	}
	// Trusted restore end-state stays readable from the contiguous prefix.
	latest, version, _, err := store.LatestResult("r")
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 || latest.RestoreGen != 3 {
		t.Fatalf("unexpected fallback state: version=%d restore=%d", version, latest.RestoreGen)
	}
}

func TestSnapshotDigestTamper(t *testing.T) {
	samples := genSamples(t, &Plan{Repos: []PlanRepo{{Name: "r", Generations: 2}}})
	state := t.TempDir()
	store, _ := inspectAndCommit(t, samples, state, "r", 0)
	defer store.Close()
	// Wholesale replacement of the snapshot payload outside the engine.
	if _, err := store.db.Exec(`UPDATE snapshots SET payload = '{"repo":"r","forged":true}' WHERE repo='r' AND version=1`); err != nil {
		t.Fatal(err)
	}
	_, code, err := store.LoadSnapshots("r")
	if err != nil {
		t.Fatal(err)
	}
	if code != ESnapshotDigest {
		t.Fatalf("expected %s, got %q", ESnapshotDigest, code)
	}
}

func TestCrashBeforeCommitRollsBack(t *testing.T) {
	samples := genSamples(t, &Plan{Repos: []PlanRepo{{Name: "r", Generations: 2}}})
	state := t.TempDir()
	res, err := InspectRepo(samples, "r", 0)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(state)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// Intercept the simulated hard kill.
	old := crashExit
	crashExit = func() { panic("simulated-kill") }
	defer func() { crashExit = old }()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected simulated kill")
			}
		}()
		store.CommitSnapshot("r", res, true)
	}()
	// Uncommitted transaction must be discarded wholesale.
	snaps, _, err := store.LoadSnapshots("r")
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 0 {
		t.Fatalf("uncommitted snapshot leaked: %d", len(snaps))
	}
	// The repo is inspectable again and the version starts at 1.
	v, err := store.CommitSnapshot("r", res, false)
	if err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("version must restart at 1 after rollback, got %d", v)
	}
}
