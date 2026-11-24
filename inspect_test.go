package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func genSamples(t *testing.T, plan *Plan) string {
	t.Helper()
	dir := t.TempDir()
	if err := GenerateSamples(dir, plan); err != nil {
		t.Fatalf("generate samples: %v", err)
	}
	return dir
}

func codesOf(res *InspectResult, gen int) []string {
	for _, g := range res.Generations {
		if g.Generation == gen {
			return g.Codes
		}
	}
	return nil
}

func hasCode(codes []string, code string) bool {
	for _, c := range codes {
		if c == code {
			return true
		}
	}
	return false
}

func TestInspectCleanRepo(t *testing.T) {
	dir := genSamples(t, &Plan{Repos: []PlanRepo{{Name: "clean", Generations: 4}}})
	res, err := InspectRepo(dir, "clean", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !res.ChainIntact || res.RestoreGen != 4 || len(res.Codes) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if len(res.RestoreFiles) != 3 {
		t.Fatalf("expected 3 restore files, got %v", res.RestoreFiles)
	}
}

func TestObjectDigestTamper(t *testing.T) {
	dir := genSamples(t, &Plan{
		Repos:   []PlanRepo{{Name: "r", Generations: 4}},
		Tampers: []PlanTamper{{Repo: "r", Generation: 3, Kind: "object-digest"}},
	})
	res, err := InspectRepo(dir, "r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasCode(codesOf(res, 3), EObjectDigest) {
		t.Fatalf("gen 3 should carry %s: %+v", EObjectDigest, codesOf(res, 3))
	}
	// Object-level tamper taints only its own generation.
	if res.RestoreGen != 4 {
		t.Fatalf("restore point should be gen 4, got %d", res.RestoreGen)
	}
	if len(res.Excluded) != 1 || res.Excluded[0].Generation != 3 {
		t.Fatalf("excluded boundary wrong: %+v", res.Excluded)
	}
}

func TestDropGenerationChainBroken(t *testing.T) {
	dir := genSamples(t, &Plan{
		Repos:   []PlanRepo{{Name: "r", Generations: 4}},
		Tampers: []PlanTamper{{Repo: "r", Generation: 2, Kind: "drop-generation"}},
	})
	res, err := InspectRepo(dir, "r", 0)
	if err != nil {
		t.Fatal(err)
	}
	c3 := codesOf(res, 3)
	if !hasCode(c3, EChainBroken) {
		t.Fatalf("gen 3 should carry %s: %+v", EChainBroken, c3)
	}
	var g3 *GenResult
	for i := range res.Generations {
		if res.Generations[i].Generation == 3 {
			g3 = &res.Generations[i]
		}
	}
	if g3.ExpectedPrevChain == "" || g3.ActualPrevChain == "" || g3.ExpectedPrevChain == g3.ActualPrevChain {
		t.Fatalf("chain break must record expected/actual digests: %+v", g3)
	}
	if res.RestoreGen != 1 {
		t.Fatalf("restore point should fall back to gen 1, got %d", res.RestoreGen)
	}
}

func TestManifestReplacementDetected(t *testing.T) {
	dir := genSamples(t, &Plan{
		Repos:   []PlanRepo{{Name: "r", Generations: 4}},
		Tampers: []PlanTamper{{Repo: "r", Generation: 2, Kind: "replace-manifest"}},
	})
	res, err := InspectRepo(dir, "r", 0)
	if err != nil {
		t.Fatal(err)
	}
	// Same generation number, different content: must be flagged via
	// content-digest divergence, not skipped by number comparison.
	if !hasCode(codesOf(res, 2), EManifestReplaced) {
		t.Fatalf("gen 2 should carry %s: %+v", EManifestReplaced, codesOf(res, 2))
	}
	if res.RestoreGen != 1 {
		t.Fatalf("replaced generation must not be a restore point; got %d", res.RestoreGen)
	}
	for _, g := range res.Generations {
		if g.Generation >= 2 && g.Trusted {
			t.Fatalf("generation %d downstream of replacement must be untrusted", g.Generation)
		}
	}
}

func TestObjectMissing(t *testing.T) {
	dir := genSamples(t, &Plan{
		Repos:   []PlanRepo{{Name: "r", Generations: 3}},
		Tampers: []PlanTamper{{Repo: "r", Generation: 2, Kind: "delete-object"}},
	})
	res, err := InspectRepo(dir, "r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasCode(codesOf(res, 2), EObjectMissing) {
		t.Fatalf("gen 2 should carry %s: %+v", EObjectMissing, codesOf(res, 2))
	}
	if res.RestoreGen != 3 {
		t.Fatalf("restore point should be gen 3, got %d", res.RestoreGen)
	}
}

func TestManifestTruncated(t *testing.T) {
	dir := genSamples(t, &Plan{Repos: []PlanRepo{{Name: "r", Generations: 2}}})
	p := manifestPath(dir, "r", 1)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, raw[:len(raw)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := InspectRepo(dir, "r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasCode(codesOf(res, 1), EManifestTruncated) {
		t.Fatalf("gen 1 should carry %s: %+v", EManifestTruncated, codesOf(res, 1))
	}
	if !hasCode(res.Codes, ENoTrustedRestore) || res.RestoreGen != 0 {
		t.Fatalf("expected %s and no restore point: %+v", ENoTrustedRestore, res)
	}
	if len(res.RestoreFiles) != 0 {
		t.Fatalf("no partial restore set may be emitted: %v", res.RestoreFiles)
	}
}

func TestDigestFormatInvalid(t *testing.T) {
	dir := genSamples(t, &Plan{Repos: []PlanRepo{{Name: "r", Generations: 2}}})
	p := manifestPath(dir, "r", 1)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	m.Entries[0].SHA256 = "not-a-digest"
	out, _ := json.MarshalIndent(m, "", "  ")
	if err := os.WriteFile(p, out, 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := InspectRepo(dir, "r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasCode(codesOf(res, 1), EDigestFormat) {
		t.Fatalf("gen 1 should carry %s: %+v", EDigestFormat, codesOf(res, 1))
	}
}

func TestGenerationBudget(t *testing.T) {
	dir := genSamples(t, &Plan{Repos: []PlanRepo{{Name: "r", Generations: 5}}})
	res, err := InspectRepo(dir, "r", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !hasCode(res.Codes, EGenerationBudget) {
		t.Fatalf("expected %s: %+v", EGenerationBudget, res.Codes)
	}
	if len(res.Generations) != 2 {
		t.Fatalf("budget must stop inspection at 2 generations, got %d", len(res.Generations))
	}
}

func TestNoTrustedRestore(t *testing.T) {
	dir := genSamples(t, &Plan{
		Repos:   []PlanRepo{{Name: "r", Generations: 3}},
		Tampers: []PlanTamper{{Repo: "r", Generation: 1, Kind: "replace-manifest"}},
	})
	res, err := InspectRepo(dir, "r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.RestoreGen != 0 || !hasCode(res.Codes, ENoTrustedRestore) {
		t.Fatalf("expected no trusted restore: %+v", res)
	}
	if res.RestoreFiles != nil {
		t.Fatalf("partial restore set must not be emitted: %v", res.RestoreFiles)
	}
}

func TestManifestPathUsesRepoDir(t *testing.T) {
	// Guard the sample layout contract used across the engine.
	if filepath.Base(manifestPath("/s", "r", 7)) != "gen-000007.json" {
		t.Fatal("manifest path layout changed")
	}
}
