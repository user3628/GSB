package sample_test

import (
	"os"
	"path/filepath"
	"testing"

	"ledgerworkbench/sample"
)

func TestFixedSampleGeneratorIsByteReproducible(t *testing.T) {
	first := filepath.Join(t.TempDir(), "a")
	second := filepath.Join(t.TempDir(), "b")
	manifest1, err := sample.Generate(first, sample.FixedSeed)
	if err != nil {
		t.Fatal(err)
	}
	manifest2, err := sample.Generate(second, sample.FixedSeed)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest1.Files) == 0 {
		t.Fatal("no sample files generated")
	}
	for name, hash1 := range manifest1.Files {
		hash2 := manifest2.Files[name]
		if hash1 != hash2 {
			t.Fatalf("hash mismatch for %s: %s != %s", name, hash1, hash2)
		}
		bytes1, err := os.ReadFile(filepath.Join(first, name))
		if err != nil {
			t.Fatal(err)
		}
		bytes2, err := os.ReadFile(filepath.Join(second, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(bytes1) != string(bytes2) {
			t.Fatalf("sample file %s is not byte-reproducible", name)
		}
	}
}
