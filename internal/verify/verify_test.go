package verify

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"baselineworkbench/internal/engine"
	"baselineworkbench/internal/samples"
)

func TestAllAnomalyInjectionsAreAsserted(t *testing.T) {
	root := t.TempDir()
	sampleRoot := filepath.Join(root, "samples")
	if _, err := samples.Generate(sampleRoot); err != nil {
		t.Fatal(err)
	}
	options := Options{
		Workdir:     filepath.Join(root, "verify"),
		SamplesRoot: sampleRoot,
		Config:      engine.Config{Mode: engine.ModeStrict, MaxConcurrency: 1, MaxItems: 64, MaxErrorBoundMicros: 100},
	}
	first, err := Run(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if !first.AllPassed || len(first.Cases) != 5 {
		t.Fatalf("verification failed: %+v", first)
	}
	firstJSON, err := JSONBytes(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Run(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := JSONBytes(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstJSON, secondJSON) {
		t.Fatal("verification report is not byte deterministic")
	}
	for _, result := range first.Cases {
		if !result.Passed || result.ActualCode != result.ExpectedCode || !result.SnapshotUnchanged || !result.LedgerUnchanged {
			t.Fatalf("failed assertion: %+v", result)
		}
	}
}
