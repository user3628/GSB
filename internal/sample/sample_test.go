package sample

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"localsearch/multirecall/internal/model"
)

func TestGenerateIsDeterministicAndLoadable(t *testing.T) {
	root := t.TempDir()
	firstDir := filepath.Join(root, "first")
	secondDir := filepath.Join(root, "second")
	options := GenerateOptions{Seed: 73, ItemCount: 50, QueryCount: 12, VectorDim: 8, Strategy: "weighted"}
	options.OutDir = firstDir
	firstManifest, err := Generate(options)
	if err != nil {
		t.Fatal(err)
	}
	options.OutDir = secondDir
	secondManifest, err := Generate(options)
	if err != nil {
		t.Fatal(err)
	}
	if firstManifest != secondManifest {
		t.Fatalf("manifest differs: %+v vs %+v", firstManifest, secondManifest)
	}
	for _, name := range []string{ItemsFile, QueriesFile, JudgmentsFile, ConfigFile, ManifestFile} {
		first, err := os.ReadFile(filepath.Join(firstDir, name))
		if err != nil {
			t.Fatal(err)
		}
		second, err := os.ReadFile(filepath.Join(secondDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(first, second) {
			t.Fatalf("%s is not byte-identical across fixed-seed runs", name)
		}
	}
	dataset, err := Load(firstDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(dataset.Items) != 50 || len(dataset.Queries) != 12 || len(dataset.Judgments) == 0 {
		t.Fatalf("unexpected dataset sizes: items=%d queries=%d judgments=%d", len(dataset.Items), len(dataset.Queries), len(dataset.Judgments))
	}
	if dataset.ItemsDigest != firstManifest.ItemsDigest || dataset.QueriesDigest != firstManifest.QueriesDigest {
		t.Fatal("loaded digests do not match generated manifest")
	}
	if dataset.Config.VectorDimension != 8 || !dataset.Config.Fusion.DedupeByItemID {
		t.Fatalf("unexpected loaded config: %+v", dataset.Config)
	}
}

func TestGenerateRejectsInvalidScale(t *testing.T) {
	_, err := Generate(GenerateOptions{OutDir: t.TempDir(), Seed: 1, ItemCount: 0, QueryCount: 1, VectorDim: 4})
	if err == nil || model.AsAppError(err).Code != model.CodeInvalidConfig {
		t.Fatalf("expected invalid config for zero items, got %v", err)
	}
}
