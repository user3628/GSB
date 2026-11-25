package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"searchlab/internal/checkpoint"
	"searchlab/internal/config"
	"searchlab/internal/engine"
	"searchlab/internal/model"
)

func TestGenerateIsByteDeterministicAndBuildable(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	summaryA, err := Generate(dirA, 42)
	if err != nil {
		t.Fatal(err)
	}
	summaryB, err := Generate(dirB, 42)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"config.yaml", "reference.json", "run-summary.json", "shards/part-000.jsonl", "shards/part-001.jsonl"} {
		a, err := os.ReadFile(filepath.Join(dirA, path))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(dirB, path))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("%s is not byte deterministic across runs", path)
		}
	}
	if summaryA.ReferenceSHA != summaryB.ReferenceSHA || summaryA.ShardDigest != summaryB.ShardDigest {
		t.Fatal("generator summary digests differ")
	}
	var reference Reference
	payload, err := os.ReadFile(filepath.Join(dirA, "reference.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &reference); err != nil {
		t.Fatal(err)
	}
	if reference.DocCount < 12 || reference.TermCount < 50 || len(reference.Queries) < 4 {
		t.Fatalf("demo reference is too small: docs=%d terms=%d queries=%d", reference.DocCount, reference.TermCount, len(reference.Queries))
	}
	cfg, _, err := config.Load(filepath.Join(dirA, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	corpus, err := model.LoadShards(filepath.Join(dirA, "shards"))
	if err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir()
	store, err := checkpoint.Open(filepath.Join(stateDir, "state.db"), filepath.Join(stateDir, "state.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	report, err := engine.Build(context.Background(), cfg, corpus, store, true)
	if err != nil {
		t.Fatal(err)
	}
	expected := reference.Artifacts
	for _, stage := range report.Stages {
		switch stage.Stage {
		case engine.StageTokenize:
			if stage.ArtifactDigest != expected.Tokenize {
				t.Fatalf("tokenize digest mismatch")
			}
		case engine.StageStats:
			if stage.ArtifactDigest != expected.Stats {
				t.Fatalf("stats digest mismatch")
			}
		case engine.StagePostings:
			if stage.ArtifactDigest != expected.Postings {
				t.Fatalf("postings digest mismatch")
			}
		case engine.StageDiagnostics:
			if stage.ArtifactDigest != expected.Diagnostics {
				t.Fatalf("diagnostics digest mismatch")
			}
		}
	}
	if report.IndexVersion != expected.Index {
		t.Fatalf("index version mismatch: got %s want %s", report.IndexVersion, expected.Index)
	}
}
