package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

// TestIncrementalReuseOnPixelChange proves that after one view's pixels change,
// unaffected views are reused while the rebuilt global field still equals a
// from-scratch rebuild byte-for-byte (requirement 6).
func TestIncrementalReuseOnPixelChange(t *testing.T) {
	dir, store, _ := newEngineEnv(t, 77, 3, 3, 16)
	cache := NewQueryCache()

	eng := NewEngine(dir, store, cache)
	first, err := eng.Reconstruct("job-first", false)
	if err != nil {
		t.Fatal(err)
	}

	// Mutate one tile's pixels deterministically and rebuild.
	target := first.SceneFP.PerViewImageFP
	var changedView string
	for id := range target {
		changedView = id
		break
	}
	manifest, images, err := LoadDataset(dir)
	if err != nil {
		t.Fatal(err)
	}
	img := images[changedView]
	for i := range img.Gray {
		if img.Gray[i] > 0 {
			img.Gray[i] = img.Gray[i] ^ 0x0F
		}
	}
	if err := overwriteTile(dir, img); err != nil {
		t.Fatal(err)
	}

	eng2 := NewEngine(dir, store, cache)
	second, err := eng2.Reconstruct("job-second", false)
	if err != nil {
		t.Fatal(err)
	}

	// At least one unaffected view must have been reused.
	if len(second.ReusedViews) == 0 {
		t.Fatal("expected reuse of unaffected views")
	}
	for _, id := range second.ReusedViews {
		if id == changedView {
			t.Fatalf("changed view %s was incorrectly reused", id)
		}
	}

	// The rebuilt field must equal a from-scratch rebuild byte-for-byte.
	manifest2, images2, err := LoadDataset(dir)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := ComputeFieldFromScratch(manifest2, images2)
	if err != nil {
		t.Fatal(err)
	}
	got, err := loadGlobalField(second.ArtifactDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyFieldEquivalence(got, ref); err != nil {
		t.Fatalf("incremental rebuild diverged: %v", err)
	}
	_ = manifest
}

func overwriteTile(dir string, img *ViewImage) error {
	gr := newGrayImage(img.Width, img.Height, img.Gray)
	path := filepath.Join(dir, img.ShardID, img.ViewID+".png")
	return writePNG(path, gr)
}

func TestConfigChangePropagatesFingerprint(t *testing.T) {
	dir, store, _ := newEngineEnv(t, 88, 2, 2, 12)
	cache := NewQueryCache()
	eng := NewEngine(dir, store, cache)
	r1, err := eng.Reconstruct("a", false)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest.Config.Truncation += 0.05
	if err := writeJSONFile(filepath.Join(dir, "manifest.json"), manifest); err != nil {
		t.Fatal(err)
	}
	r2, err := eng.Reconstruct("b", false)
	if err != nil {
		t.Fatal(err)
	}
	if r1.ConfigVer == r2.ConfigVer {
		t.Fatal("config version did not change after parameter update")
	}
	if r1.ArtifactDir == r2.ArtifactDir {
		t.Fatal("artifact directory should be keyed by config version")
	}
	// Verify new artifact directory exists and is complete.
	if _, err := os.Stat(filepath.Join(r2.ArtifactDir, "global-field.json")); err != nil {
		t.Fatalf("new config artifacts missing: %v", err)
	}
}
