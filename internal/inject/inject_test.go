package inject_test

import (
	"path/filepath"
	"testing"

	"privescworkbench/internal/domain"
	"privescworkbench/internal/inject"
	"privescworkbench/internal/sample"
	"privescworkbench/internal/snapshot"
)

func TestAllInjectionScenarios(t *testing.T) {
	dir := t.TempDir()
	store, err := snapshot.Open(filepath.Join(dir, "inj.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	results, err := inject.NewRunner(store).RunAll()
	if err != nil {
		t.Fatal(err)
	}
	want := map[sample.Scenario]domain.BizCode{
		sample.ScenarioMaskBad:     domain.CodeSampleParse,
		sample.ScenarioOverflow:    domain.CodeScoreOverflow,
		sample.ScenarioHandleAbuse: domain.CodePrivEscalation,
		sample.ScenarioOrphan:      domain.CodeOrphanRef,
		sample.ScenarioGap:         domain.CodeSnapshotVersion,
	}
	got := map[sample.Scenario]domain.BizCode{}
	for _, r := range results {
		got[r.Scenario] = r.Code
		if r.Code != want[r.Scenario] {
			t.Errorf("scenario %s: got %s want %s detail=%q",
				r.Scenario, r.Code, want[r.Scenario], r.Detail)
		}
	}
	// parse and overflow failures must never advance the snapshot
	for _, r := range results {
		if r.Scenario == sample.ScenarioMaskBad || r.Scenario == sample.ScenarioOverflow {
			if r.SnapshotMoved {
				t.Errorf("scenario %s must not move snapshot", r.Scenario)
			}
		}
	}
}

func TestInjectionOnlyViaInput(t *testing.T) {
	// the bad-mask sample is rejected purely from its rendered bytes
	g := sample.Generate(1, sample.ScenarioMaskBad)
	if _, err := sample.ParseEventStream(g.StreamBytes); err == nil {
		t.Fatal("out-of-range mask must be rejected from input alone")
	}
}
