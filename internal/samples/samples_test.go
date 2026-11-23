package samples

import (
	"path/filepath"
	"testing"
)

func TestGenerateIsByteDeterministic(t *testing.T) {
	root := filepath.Join(t.TempDir(), "samples")
	manifest, err := Generate(root)
	if err != nil {
		t.Fatal(err)
	}
	first, err := Digest(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Generate(root); err != nil {
		t.Fatal(err)
	}
	second, err := Digest(root)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("sample digest changed: %s != %s", first, second)
	}
	loaded, err := LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SampleMarker != SampleMarker || len(loaded.Packages) != 8 {
		t.Fatalf("unexpected manifest: %+v", loaded)
	}
	classes := make(map[string]bool)
	for _, pkg := range manifest.Packages {
		classes[pkg.Class] = true
	}
	for _, required := range []string{"normal", "mixed_units", "truncated_text", "numeric_overflow"} {
		if !classes[required] {
			t.Fatalf("missing sample class %s", required)
		}
	}
	if len(manifest.Injections) != 1 || manifest.Injections[0].Class != "snapshot_version_gap" {
		t.Fatalf("missing version gap injection: %+v", manifest.Injections)
	}
}
