package samplegen

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateIsStableAndUsesPlatformIndependentPaths(t *testing.T) {
	firstDir := filepath.Join(t.TempDir(), "first")
	secondDir := filepath.Join(t.TempDir(), "second")
	first, err := Generate(firstDir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Generate(secondDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range first.Files {
		if strings.Contains(record.Path, `\`) {
			t.Fatalf("manifest path is platform specific: %s", record.Path)
		}
		left, err := os.ReadFile(filepath.Join(firstDir, filepath.FromSlash(record.Path)))
		if err != nil {
			t.Fatal(err)
		}
		right, err := os.ReadFile(filepath.Join(secondDir, filepath.FromSlash(record.Path)))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(left, right) {
			t.Fatalf("sample %s is not stable", record.Path)
		}
	}
	if len(first.Files) != len(second.Files) {
		t.Fatalf("manifest file count differs: %d vs %d", len(first.Files), len(second.Files))
	}
}
