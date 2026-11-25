package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNaNPixelRejectedAtProjection(t *testing.T) {
	dir := t.TempDir()
	m, err := GenerateDataset(GenerateParams{OutDir: dir, Seed: 11, Shards: 1, ViewsPer: 1,
		ImageW: 8, ImageH: 8, Config: func() PipelineConfig {
			c := DefaultConfig()
			c.SampleDensity = 1
			return c
		}()})
	if err != nil {
		t.Fatal(err)
	}
	shard := m.Shards[0]
	cam := shard.Views[0]
	nanPath := NaNSidecarPath(dir, shard.ShardID, cam.ViewID)
	if err := os.WriteFile(nanPath, []byte("[[2,3]]"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, images, err := LoadDataset(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ProjectView(m.Config, shard.ShardID, cam, images[cam.ViewID])
	be := asBiz(err)
	if be.Code != CodeNaNInput || be.ViewID != cam.ViewID || be.Field != "pixel" {
		t.Fatalf("unexpected: %+v", be)
	}
}

func TestNumericMismatchReported(t *testing.T) {
	dir, store, _ := newEngineEnv(t, 13, 2, 2, 12)
	cache := NewQueryCache()
	// Tighten distance tolerance so the deliberately small grid cannot match.
	manifest, err := LoadManifest(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest.Config.DistTolerance = 1e-9
	manifest.Config.PixelTolerance = 1e-9
	if err := writeJSONFile(filepath.Join(dir, "manifest.json"), manifest); err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(dir, store, cache)
	_, err = eng.Reconstruct("job-num", true)
	be := asBiz(err)
	if be.Code != CodeNumericMismatch {
		t.Fatalf("expected numeric mismatch, got %v", be)
	}
	if be.ViewID == "" || be.MaxDev <= 0 {
		t.Fatalf("numeric mismatch missing view id / max deviation: %+v", be)
	}
}
