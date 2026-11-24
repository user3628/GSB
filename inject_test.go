package main

import (
	"testing"
)

// TestFiveInjectionClasses co-locates all five anomaly classes in one batch
// and asserts codes, unpolluted restore end-state and version integrity.
func TestFiveInjectionClasses(t *testing.T) {
	plan := &Plan{
		Repos: []PlanRepo{
			{Name: "ok", Generations: 3},
			{Name: "inj-digest", Generations: 3},
			{Name: "inj-gap", Generations: 3},
			{Name: "inj-replace", Generations: 3},
			{Name: "inj-missing", Generations: 3},
			{Name: "inj-snap", Generations: 3},
		},
		Tampers: []PlanTamper{
			{Repo: "inj-digest", Generation: 2, Kind: "object-digest"},
			{Repo: "inj-gap", Generation: 2, Kind: "drop-generation"},
			{Repo: "inj-replace", Generation: 2, Kind: "replace-manifest"},
			{Repo: "inj-missing", Generation: 2, Kind: "delete-object"},
		},
	}
	samples := genSamples(t, plan)
	state := t.TempDir()
	br, store, err := RunBatch(samples, state, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	byRepo := map[string]RepoOutcome{}
	for _, o := range br.Outcomes {
		byRepo[o.Repo] = o
	}
	assertGenCode := func(repo string, gen int, code string) {
		t.Helper()
		if !hasCode(codesOf(byRepo[repo].Result, gen), code) {
			t.Fatalf("%s gen %d: expected %s, got %+v", repo, gen, code, codesOf(byRepo[repo].Result, gen))
		}
	}
	assertGenCode("inj-digest", 2, EObjectDigest)
	assertGenCode("inj-gap", 3, EChainBroken)
	assertGenCode("inj-replace", 2, EManifestReplaced)
	assertGenCode("inj-missing", 2, EObjectMissing)

	// Fifth class: snapshot version gap, injected via store-level input.
	if _, err := store.InjectSnapshotGap("inj-snap"); err != nil {
		t.Fatal(err)
	}
	snaps, code, err := store.LoadSnapshots("inj-snap")
	if err != nil {
		t.Fatal(err)
	}
	if code != ESnapshotVersion || len(snaps) != 1 {
		t.Fatalf("expected %s with contiguous prefix 1, got code=%q n=%d", ESnapshotVersion, code, len(snaps))
	}

	// Trusted restore end-state is unpolluted for the clean repo, and the
	// injected repos keep honest fallback restore points.
	if r := byRepo["ok"].Result; r.RestoreGen != 3 || len(r.Codes) != 0 {
		t.Fatalf("clean repo polluted: %+v", r)
	}
	if r := byRepo["inj-replace"].Result; r.RestoreGen != 1 {
		t.Fatalf("replaced generation must not be restorable: %+v", r)
	}
	// Committed snapshot versions were not advanced by gap fallback.
	for _, o := range br.Outcomes {
		if o.SnapshotVersion != 1 {
			t.Fatalf("repo %s version advanced unexpectedly: %d", o.Repo, o.SnapshotVersion)
		}
	}
}
