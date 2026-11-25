package samples_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"spmmopt/samples"
)

func TestGeneratorReproducibleAndCoversStructure(t *testing.T) {
	dir1, dir2 := t.TempDir(), t.TempDir()
	m1, err := samples.GenerateAll(dir1, samples.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := samples.GenerateAll(dir2, samples.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	if len(m1.Samples) != 10 || len(m2.Samples) != 10 {
		t.Fatalf("sample count=%d/%d want 10", len(m1.Samples), len(m2.Samples))
	}
	for _, rel := range append([]string{"manifest.json"}, sampleFiles(m1)...) {
		b1, err := os.ReadFile(filepath.Join(dir1, rel))
		if err != nil {
			t.Fatal(err)
		}
		b2, err := os.ReadFile(filepath.Join(dir2, rel))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b1, b2) {
			t.Fatalf("%s was not reproducible byte-for-byte", rel)
		}
	}
	seenRates := map[string]bool{}
	seenShapes := map[string]bool{}
	for _, s := range m1.Samples {
		seenShapes[s.Shape] = true
		if !s.Degenerate {
			seenRates[s.Name[len(s.Name)-2:]] = true
		}
		if s.Stats.StructureFingerprint == "" || s.Stats.DenseFingerprint == "" {
			t.Fatalf("missing fingerprints for %s", s.Name)
		}
		if s.Stats.ExplicitZeros == 0 || s.Stats.DuplicateEntries == 0 || s.Stats.EmptyRows == 0 {
			t.Fatalf("sample %s lacks structural degeneracies: %+v", s.Name, s.Stats)
		}
	}
	for _, rate := range []string{"01", "10", "40"} {
		if !seenRates[rate] {
			t.Fatalf("missing sparsity %s", rate)
		}
	}
	for _, shape := range []string{"square", "tall", "wide"} {
		if !seenShapes[shape] {
			t.Fatalf("missing shape %s", shape)
		}
	}
	degen, err := m1.Load(dir1, "degenerate_mixed")
	if err != nil {
		t.Fatal(err)
	}
	if degen.Stats.EmptyRows < 8 || degen.Stats.OutOfOrderEntries == 0 || degen.Stats.ExplicitZeros == 0 || degen.Stats.DuplicateEntries == 0 {
		t.Fatalf("degenerate sample lacks required cases: %+v", degen.Stats)
	}
}

func sampleFiles(m *samples.Manifest) []string {
	var files []string
	for _, s := range m.Samples {
		files = append(files, s.MatrixFile, s.DenseFile)
	}
	return files
}
