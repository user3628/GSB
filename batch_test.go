package main

import (
	"bytes"
	"testing"
)

func TestBatchConcurrencyAndIsolation(t *testing.T) {
	samples := genSamples(t, nil) // default plan: 5 repos incl. tampered
	state := t.TempDir()
	br, store, err := RunBatch(samples, state, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if len(br.Outcomes) != 5 {
		t.Fatalf("expected 5 repos, got %d", len(br.Outcomes))
	}
	if br.MaxInFlight < 1 || br.MaxInFlight > 2 {
		t.Fatalf("in-flight sessions must respect concurrency=2, got %d", br.MaxInFlight)
	}
	for _, o := range br.Outcomes {
		if o.SnapshotVersion != 1 {
			t.Fatalf("repo %s: isolated session must start at version 1, got %d", o.Repo, o.SnapshotVersion)
		}
	}
	// Tamper localization: failures trace to repo and generation.
	found := map[string]bool{}
	for _, o := range br.Outcomes {
		for _, g := range o.Result.Generations {
			for _, c := range g.Codes {
				found[o.Repo+":"+c] = true
			}
		}
	}
	for _, want := range []string{
		"beta:" + EObjectDigest,
		"gamma:" + EChainBroken,
		"delta:" + EManifestReplaced,
		"epsilon:" + EObjectMissing,
	} {
		if !found[want] {
			t.Fatalf("missing localized finding %s", want)
		}
	}
	st, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.ReposTotal != 5 || st.MaxInFlight > 2 || st.InFlight != 0 {
		t.Fatalf("bad stats: %+v", st)
	}
}

func TestBatchBudgetIsolation(t *testing.T) {
	samples := genSamples(t, &Plan{Repos: []PlanRepo{
		{Name: "big", Generations: 6},
		{Name: "small", Generations: 2},
	}})
	state := t.TempDir()
	br, store, err := RunBatch(samples, state, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	byRepo := map[string]RepoOutcome{}
	for _, o := range br.Outcomes {
		byRepo[o.Repo] = o
	}
	if !hasCode(byRepo["big"].Result.Codes, EGenerationBudget) {
		t.Fatalf("big repo should hit %s", EGenerationBudget)
	}
	if !byRepo["small"].OK || byRepo["small"].Result.RestoreGen != 2 {
		t.Fatalf("small repo must be unaffected: %+v", byRepo["small"])
	}
}

func TestReportByteDeterministic(t *testing.T) {
	samples := genSamples(t, nil)
	state := t.TempDir()
	_, store, err := RunBatch(samples, state, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	rep1, err := BuildReport(store)
	if err != nil {
		t.Fatal(err)
	}
	b1, err := MarshalReport(rep1)
	if err != nil {
		t.Fatal(err)
	}
	rep2, err := BuildReport(store)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := MarshalReport(rep2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatal("repeated report runs must be byte-identical")
	}
	store.Close()
	// Regenerating identical input in a fresh location must reproduce the
	// same report bytes (no timestamps, no absolute paths).
	samples2 := genSamples(t, nil)
	state2 := t.TempDir()
	_, store2, err := RunBatch(samples2, state2, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	rep3, _ := BuildReport(store2)
	b3, _ := MarshalReport(rep3)
	if !bytes.Equal(b1, b3) {
		t.Fatal("identical inputs must yield byte-identical reports")
	}
}

func TestReportContainsRequiredFields(t *testing.T) {
	samples := genSamples(t, nil)
	state := t.TempDir()
	_, store, err := RunBatch(samples, state, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rep, err := BuildReport(store)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Tick != fixedTick {
		t.Fatalf("time fields must use the fixed sampling tick, got %q", rep.Tick)
	}
	for i := 1; i < len(rep.Repos); i++ {
		if rep.Repos[i-1].Repo > rep.Repos[i].Repo {
			t.Fatal("repos must be sorted by relative path")
		}
	}
	var gamma *RepoReport
	for i := range rep.Repos {
		if rep.Repos[i].Repo == "gamma" {
			gamma = &rep.Repos[i]
		}
	}
	if gamma == nil || gamma.RestoreGen != 1 || len(gamma.Timeline) == 0 {
		t.Fatalf("report missing tamper localization for gamma: %+v", gamma)
	}
}
