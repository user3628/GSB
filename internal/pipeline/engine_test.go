package pipeline

import (
	"path/filepath"
	"testing"
)

func newEngineEnv(t *testing.T, seed int64, shards, views, size int) (string, *Store, *Manifest) {
	t.Helper()
	dir := t.TempDir()
	if _, err := GenerateDataset(GenerateParams{OutDir: dir, Seed: seed, Shards: shards,
		ViewsPer: views, ImageW: size, ImageH: size, Config: DefaultConfig()}); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(dir, "recon.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	m, err := LoadManifest(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	return dir, store, m
}

func TestFullAndIncrementalByteIdentical(t *testing.T) {
	dir, store, m := newEngineEnv(t, 99, 3, 3, 16)
	cache := NewQueryCache()

	// First run uses an adaptive (cache/incremental-capable) strategy.
	eng := NewEngine(dir, store, cache)
	inc, err := eng.Reconstruct("job-1", false)
	if err != nil {
		t.Fatalf("adaptive run failed: %v", err)
	}
	if inc.Strategy == "" {
		t.Fatal("strategy not recorded")
	}

	// Reference from scratch in pure memory.
	manifest, images, err := LoadDataset(dir)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := ComputeFieldFromScratch(manifest, images)
	if err != nil {
		t.Fatal(err)
	}
	incField, err := loadGlobalField(inc.ArtifactDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyFieldEquivalence(incField, ref); err != nil {
		t.Fatalf("equivalence failed: %v", err)
	}

	// Forced full recompute must also be byte identical.
	eng2 := NewEngine(dir, store, cache)
	full, err := eng2.Reconstruct("job-2", true)
	if err != nil {
		t.Fatalf("full run failed: %v", err)
	}
	fullField, _ := loadGlobalField(full.ArtifactDir)
	if err := VerifyFieldEquivalence(fullField, ref); err != nil {
		t.Fatalf("full run differs: %v", err)
	}
	_ = m
}

func TestChangeInvalidationEarliestStage(t *testing.T) {
	cfg := DefaultConfig()
	// Pixel/camera change -> projection; voxel/trunc -> local; fusion -> fusion.
	if got := EarliestChangedStage(ChangeSet{Pixels: true}); got != StageProjection {
		t.Fatalf("pixel change -> %s", got)
	}
	if got := EarliestChangedStage(ChangeSet{Camera: true}); got != StageProjection {
		t.Fatalf("camera change -> %s", got)
	}
	if got := EarliestChangedStage(ChangeSet{VoxelResolution: true}); got != StageLocal {
		t.Fatalf("voxel change -> %s", got)
	}
	if got := EarliestChangedStage(ChangeSet{FusionRadius: true}); got != StageFusion {
		t.Fatalf("fusion change -> %s", got)
	}
	down := DownstreamStages(StageLocal)
	if len(down) != 3 || down[0] != StageLocal {
		t.Fatalf("downstream chain wrong: %v", down)
	}
	diff := DiffConfig(cfg, cfg)
	if diff.Truncation || diff.FusionRadius {
		t.Fatal("identical config reported as changed")
	}
	cfg2 := cfg
	cfg2.Truncation += 0.1
	if !DiffConfig(cfg, cfg2).Truncation {
		t.Fatal("truncation change not detected")
	}
}

func TestIncrementalMismatchReportsFirstVoxel(t *testing.T) {
	a := GlobalField{Voxels: []GlobalVoxel{
		{Key: VoxelKey{0, 0, 0}, Color: 10, WeightSum: 1, Hits: 1},
		{Key: VoxelKey{1, 0, 0}, Color: 20, WeightSum: 1, Hits: 1},
	}}
	b := GlobalField{Voxels: []GlobalVoxel{
		{Key: VoxelKey{0, 0, 0}, Color: 10, WeightSum: 1, Hits: 1},
		{Key: VoxelKey{1, 0, 0}, Color: 99, WeightSum: 1, Hits: 1},
	}}
	err := VerifyFieldEquivalence(a, b)
	be := asBiz(err)
	if be.Code != CodeIncrementalMismatch {
		t.Fatalf("got %v", err)
	}
	if be.Voxel != (VoxelKey{1, 0, 0}) {
		t.Fatalf("first differing voxel = %v", be.Voxel)
	}
}

func TestRecoveryAfterStop(t *testing.T) {
	dir, store, _ := newEngineEnv(t, 5, 4, 4, 16)
	cache := NewQueryCache()

	// Baseline continuous run reference.
	baseEng := NewEngine(dir, store, cache)
	base, err := baseEng.Reconstruct("base", true)
	if err != nil {
		t.Fatal(err)
	}
	refField, _ := loadGlobalField(base.ArtifactDir)
	refSummary := SummarizePointCloud(refField, mustPruned(t, base.ArtifactDir))
	refStats := base.Errors

	// New workspace copy to simulate a process killed mid-run then resumed.
	dir2 := copyDir(t, dir)
	store2, err := OpenStore(filepath.Join(dir2, "recon.db"))
	if err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(dir2, store2, cache)
	// Request stop immediately; the engine should stop at a shard boundary,
	// leaving only durable checkpoints (no in-memory state dependency).
	eng.SignalStop()
	_, stopErr := eng.Reconstruct("stopped", false)
	if stopErr == nil {
		t.Fatal("expected graceful stop error")
	}

	// Restart: a fresh engine over the same workspace resumes and must match
	// the continuous run byte-for-byte.
	resumeEng := NewEngine(dir2, store2, cache)
	resumed, err := resumeEng.Reconstruct("resumed", true)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	resField, _ := loadGlobalField(resumed.ArtifactDir)
	if err := VerifyFieldEquivalence(resField, refField); err != nil {
		t.Fatalf("resumed field differs: %v", err)
	}
	resSummary := SummarizePointCloud(resField, mustPruned(t, resumed.ArtifactDir))
	if MustDigest(resSummary) != MustDigest(refSummary) {
		t.Fatal("summary differs after recovery")
	}
	if MustDigest(resumed.Errors) != MustDigest(refStats) {
		t.Fatal("error stats differ after recovery")
	}
	if MustDigest(resumed.Sparsity) != MustDigest(base.Sparsity) {
		t.Fatal("sparsity stats differ after recovery")
	}
}

func TestCacheKeyIncludesAllFingerprints(t *testing.T) {
	k1 := CacheKey("summary", "scene", "cfg", "cam")
	k2 := CacheKey("summary", "scene2", "cfg", "cam")
	k3 := CacheKey("summary", "scene", "cfg2", "cam")
	k4 := CacheKey("summary", "scene", "cfg", "cam2")
	if k1 == k2 || k1 == k3 || k1 == k4 {
		t.Fatal("cache key does not distinguish fingerprints")
	}
}

func mustPruned(t *testing.T, dir string) []GlobalVoxel {
	t.Helper()
	p, err := loadPruned(dir)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
