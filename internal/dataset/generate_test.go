package dataset

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateDeterministicManifestDimensions(t *testing.T) {
	first := t.TempDir()
	second := t.TempDir()
	m1, err := GenerateDirectory(first, DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := GenerateDirectory(second, DefaultSeed); err != nil {
		t.Fatal(err)
	}
	want := map[string][2]int{"small": {600, 12}, "medium": {6000, 40}}
	for _, ds := range m1.Datasets {
		if got := want[ds.Key]; got != [2]int{ds.Rows, ds.Columns} {
			t.Fatalf("%s dimensions = %dx%d, want %dx%d", ds.Key, ds.Rows, ds.Columns, got[0], got[1])
		}
		a, err := os.ReadFile(filepath.Join(first, ds.File))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(second, ds.File))
		if err != nil {
			t.Fatal(err)
		}
		if string(a) != string(b) {
			t.Fatalf("%s csv is not deterministic", ds.Key)
		}
	}
	a, _ := os.ReadFile(filepath.Join(first, ManifestName))
	b, _ := os.ReadFile(filepath.Join(second, ManifestName))
	if string(a) != string(b) {
		t.Fatal("manifest is not deterministic")
	}
}
