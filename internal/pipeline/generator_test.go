package pipeline

import (
	"path/filepath"
	"testing"
)

func TestGeneratorDeterministic(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	p := GenerateParams{Seed: 42, Shards: 2, ViewsPer: 3, ImageW: 16, ImageH: 16,
		Config: DefaultConfig()}
	m1, err := GenerateDataset(GenerateParams{OutDir: dir1, Seed: p.Seed, Shards: p.Shards,
		ViewsPer: p.ViewsPer, ImageW: p.ImageW, ImageH: p.ImageH, Config: p.Config})
	if err != nil {
		t.Fatal(err)
	}
	m2, err := GenerateDataset(GenerateParams{OutDir: dir2, Seed: p.Seed, Shards: p.Shards,
		ViewsPer: p.ViewsPer, ImageW: p.ImageW, ImageH: p.ImageH, Config: p.Config})
	if err != nil {
		t.Fatal(err)
	}
	d1, err := Digest(m1)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := Digest(m2)
	if err != nil {
		t.Fatal(err)
	}
	if d1 != d2 {
		t.Fatalf("generator not deterministic: %s != %s", d1, d2)
	}
	// Pixel bytes must be identical too.
	_, imgs1, err := loadRaw(dir1, m1)
	if err != nil {
		t.Fatal(err)
	}
	_, imgs2, err := loadRaw(dir2, m2)
	if err != nil {
		t.Fatal(err)
	}
	for id, a := range imgs1 {
		b := imgs2[id]
		if a.ContentDigest() != b.ContentDigest() {
			t.Fatalf("tile %s differs across deterministic runs", id)
		}
	}
}

func loadRaw(dir string, m *Manifest) (*Manifest, map[string]*ViewImage, error) {
	images := map[string]*ViewImage{}
	for _, shard := range m.Shards {
		for _, cam := range shard.Views {
			img, err := loadViewImage(dir, shard.ShardID, cam)
			if err != nil {
				return nil, nil, err
			}
			images[cam.ViewID] = img
		}
	}
	return m, images, nil
}

func TestLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	m, err := GenerateDataset(GenerateParams{OutDir: dir, Seed: 7, Shards: 1, ViewsPer: 2,
		ImageW: 12, ImageH: 12, Config: DefaultConfig()})
	if err != nil {
		t.Fatal(err)
	}
	loaded, images, err := LoadDataset(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(images) != totalViews(m) {
		t.Fatalf("loaded %d images, want %d", len(images), totalViews(m))
	}
	if got := filepath.Join(dir, "manifest.json"); loaded.Seed != 7 {
		t.Fatalf("seed mismatch, path=%s", got)
	}
}
