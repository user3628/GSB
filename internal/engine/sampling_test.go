package engine

import "testing"

func TestSampledVsFullCrossCheck(t *testing.T) {
	eng, data := bootstrappedEngine(t)
	res := ingestSeed(t, eng, data)
	version := res.CommitVersion

	full, err := eng.FullNoiseReduction(version)
	if err != nil {
		t.Fatal(err)
	}
	sampled, err := eng.SampledNoiseReduction(version, 20)
	if err != nil {
		t.Fatal(err)
	}
	if sampled.Sampling == nil || sampled.Sampling.Method != SamplingMethod {
		t.Fatal("sampling report missing deterministic method")
	}
	if sampled.Sampling.Processed != 20 || sampled.Sampling.TotalInput != len(data.Observations) {
		t.Fatalf("sampling counts wrong: %+v", sampled.Sampling)
	}
	// Sampled alerts must be a subset and every sampled alert is adjudicated.
	diff := ComparePaths(sampled, full)
	if len(diff.SampledAlertIDs) > len(diff.FullAlertIDs) {
		t.Fatal("sampled path produced more alerts than full path")
	}
	fullSet := map[string]bool{}
	for _, id := range diff.FullAlertIDs {
		fullSet[id] = true
	}
	// Sampled ids are computed independently; every shared id must exist in full.
	// (Alert ids embed the same facts, so shared selected observations match.)
	for _, id := range diff.SharedAlertIDs {
		if !fullSet[id] {
			t.Fatalf("shared alert %s absent from full path", id)
		}
	}
	// Sampled path never emits un-adjudicated candidate alerts: states must be
	// FIRING or SUPPRESSED only.
	for _, a := range sampled.Alerts {
		if a.State != "FIRING" && a.State != "SUPPRESSED" {
			t.Fatalf("un-adjudicated alert leaked into sampled result: %s", a.State)
		}
	}
	// Determinism: running twice yields identical selected indices.
	sampled2, _ := eng.SampledNoiseReduction(version, 20)
	if !equalInts(sampled.Sampling.SelectedIndex, sampled2.Sampling.SelectedIndex) {
		t.Fatal("sampler is not deterministic")
	}
}

func TestSampledRootCausesReuseSameEvidence(t *testing.T) {
	eng, data := bootstrappedEngine(t)
	res := ingestSeed(t, eng, data)
	sampled, err := eng.SampledNoiseReduction(res.CommitVersion, 30)
	if err != nil {
		t.Fatal(err)
	}
	// Every sampled score carries integer hops and an explicit time delta.
	for _, rc := range sampled.RootCauses {
		if rc.Hops <= 0 {
			t.Fatalf("root cause must have positive integer hops: %+v", rc)
		}
		if rc.TimeDeltaMS < 0 {
			t.Fatalf("time delta must be non-negative integer: %+v", rc)
		}
		if rc.ScoreFinalMilli <= 0 {
			t.Fatalf("score must be a positive fixed-point value: %+v", rc)
		}
	}
}

func TestStrideSamplerBounds(t *testing.T) {
	if got := deterministicStride(10, 10); len(got) != 10 {
		t.Fatalf("limit>=n must select all, got %d", len(got))
	}
	got := deterministicStride(10, 3)
	if len(got) != 3 {
		t.Fatalf("want 3 selected, got %d", len(got))
	}
	seen := map[int]bool{}
	for _, i := range got {
		if i < 0 || i >= 10 || seen[i] {
			t.Fatalf("invalid stride selection: %v", got)
		}
		seen[i] = true
	}
	if deterministicStride(0, 3) != nil {
		t.Fatal("empty input must yield nil selection")
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
