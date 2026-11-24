package pipeline_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"supplyaudit/internal/formats"
	"supplyaudit/internal/model"
	"supplyaudit/internal/pipeline"
	"supplyaudit/internal/sample"
)

func TestSchedulerQueueAndMerge(t *testing.T) {
	workDir := t.TempDir()
	layout, err := sample.Generate(workDir, true)
	if err != nil {
		t.Fatal(err)
	}
	sources, err := formats.Discover(layout.InputDir)
	if err != nil {
		t.Fatal(err)
	}
	config := pipeline.DefaultConfig()
	config.MaxConcurrency = 2
	result, err := pipeline.Run(sources, config)
	if err != nil {
		t.Fatal(err)
	}
	if result.Stats.PeakInFlight != 2 || result.Stats.PeakQueueLength != 2 || result.Stats.SourcesStarted != 4 || result.Stats.SourcesCompleted != 4 {
		t.Fatalf("unexpected scheduler stats: %#v", result.Stats)
	}
	if !result.Merge.SelfConsistent || result.Merge.DeclarationCount != 16 {
		t.Fatalf("unexpected merge result: %#v", result.Merge)
	}
	if !result.HasCode(model.CodeDigestConflict) || !result.HasCode(model.CodeVersionDowngrade) || !result.HasCode(model.CodeWeakDigest) {
		t.Fatalf("expected business findings, got %#v", result.AllCodes())
	}
}

func TestComponentBudgetRetainsParsedPrefix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "packages.jsonl")
	lines := ""
	for index := 1; index <= 3; index++ {
		digest := "sha256:0123456789012345678901234567890123456789012="
		lines += fmt.Sprintf("{\"name\":\"@demo/pkg%d\",\"version\":\"1.0.%d\",\"digest\":%q,\"source\":\"json\"}\n", index, index, digest)
	}
	if err := os.WriteFile(path, []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	specs, err := formats.Discover(dir)
	if err != nil {
		t.Fatal(err)
	}
	config := pipeline.DefaultConfig()
	config.MaxComponentsPerSource = 2
	result, err := pipeline.Run(specs, config)
	if err != nil {
		t.Fatal(err)
	}
	source := result.Sources[0]
	if !source.BudgetStopped || len(source.Declarations) != 2 || !source.HasCode(model.CodeComponentBudget) {
		t.Fatalf("budget was not enforced: %#v", source)
	}
	if result.Merge.DeclarationCount != 2 || result.Merge.ComponentCount != 2 {
		t.Fatalf("unexpected partial merge: %#v", result.Merge)
	}
}

func TestDuplicateExplicitSourceIDsAreMadeStableAndUnique(t *testing.T) {
	root := t.TempDir()
	digest := "sha256:0123456789012345678901234567890123456789012="
	specs := []model.SourceSpec{}
	for index, dirName := range []string{"a", "b"} {
		dir := filepath.Join(root, dirName)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "pkg.jsonl")
		line := fmt.Sprintf("{\"name\":\"@demo/pkg%d\",\"version\":\"1.0.0\",\"digest\":%q}\n", index, digest)
		if err := os.WriteFile(path, []byte(line), 0o644); err != nil {
			t.Fatal(err)
		}
		specs = append(specs, model.SourceSpec{ID: "jsonl:pkg.jsonl", Format: model.FormatJSONL, Path: path, ReportPath: "pkg.jsonl"})
	}
	result, err := pipeline.Run(specs, pipeline.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Merge.SelfConsistent || result.Merge.DeclarationCount != 2 {
		t.Fatalf("duplicate display paths lost declarations: %#v", result.Merge)
	}
	if result.Sources[0].Spec.ID == result.Sources[1].Spec.ID {
		t.Fatalf("duplicate source IDs were not disambiguated: %#v", result.Sources)
	}
}
