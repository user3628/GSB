package samplegen

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateIsByteDeterministic(t *testing.T) {
	first := t.TempDir()
	second := t.TempDir()
	firstPaths, err := Generate(first, Options{IncludeAnomalies: true})
	if err != nil {
		t.Fatalf("generate first suite: %v", err)
	}
	secondPaths, err := Generate(second, Options{IncludeAnomalies: true})
	if err != nil {
		t.Fatalf("generate second suite: %v", err)
	}
	if len(firstPaths) != len(secondPaths) {
		t.Fatalf("path count mismatch: %d vs %d", len(firstPaths), len(secondPaths))
	}
	for i := range firstPaths {
		if firstPaths[i] != secondPaths[i] {
			t.Fatalf("path order mismatch at %d: %q vs %q", i, firstPaths[i], secondPaths[i])
		}
	}
	filesA := collectFiles(t, first)
	filesB := collectFiles(t, second)
	if len(filesA) != len(filesB) {
		t.Fatalf("file count mismatch: %d vs %d", len(filesA), len(filesB))
	}
	for path, dataA := range filesA {
		dataB, ok := filesB[path]
		if !ok {
			t.Fatalf("file %s missing from second suite", path)
		}
		if !bytes.Equal(dataA, dataB) {
			t.Fatalf("file %s is not byte deterministic", path)
		}
	}

	for _, required := range []string{
		"anomaly/missing-intermediate",
		"anomaly/sni-mismatch",
		"anomaly/expired",
		"anomaly/untrusted-root",
	} {
		if _, ok := filesA[filepath.ToSlash(filepath.Join(required, "meta.json"))]; !ok {
			t.Fatalf("required anomaly fixture %s was not generated", required)
		}
	}
}

func collectFiles(t *testing.T, root string) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(relative)] = data
		return nil
	})
	if err != nil {
		t.Fatalf("walk generated suite: %v", err)
	}
	return files
}
