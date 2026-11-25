package pipeline

import "testing"

func TestFusionDeterministicAndNormalized(t *testing.T) {
	cfg := DefaultConfig()
	dir := t.TempDir()
	m, images, err := LoadDataset(mustGen(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	build := func() GlobalField {
		var vols []LocalVolume
		for _, shard := range m.Shards {
			recs, err := ProjectShard(cfg, shard, images)
			if err != nil {
				t.Fatal(err)
			}
			for _, cam := range shard.Views {
				vols = append(vols, BuildLocalVolume(cfg.VoxelResolution, shard.ShardID,
					cam.ViewID, sortedContributions(recs[cam.ViewID])))
			}
		}
		return FuseGlobal(cfg, vols)
	}
	f1 := build()
	f2 := build()
	b1, _ := CanonicalEncode(f1)
	b2, _ := CanonicalEncode(f2)
	if string(b1) != string(b2) {
		t.Fatal("fusion non-deterministic")
	}
	if len(f1.Voxels) == 0 {
		t.Fatal("empty field")
	}
	// Weights must be positive (normalization well-defined).
	for _, v := range f1.Voxels {
		if v.WeightSum <= 0 || v.Color < 0 || v.Color > 255 {
			t.Fatalf("invalid fused voxel: %+v", v)
		}
	}
	kept, pruned := PruneSparse(f1, cfg.SparsityPrune)
	if len(kept.Voxels)+len(pruned) != len(f1.Voxels) {
		t.Fatal("prune counts do not partition the field")
	}
	for _, p := range pruned {
		if !p.Pruned || p.Reason == "" {
			t.Fatalf("pruned voxel missing reason: %+v", p)
		}
	}
}

func TestPlanSmallAvoidSparse(t *testing.T) {
	plan := Plan(PlanInputs{ShardCount: 2, ViewCount: 6, VoxelCount: 1000,
		CameraChangeRange: 0.2, Budget: 1e9})
	if plan.Selected == StrategySparse {
		t.Fatalf("small scene selected sparse strategy: %+v", plan)
	}
	if !plan.Eligible[StrategyFull] {
		t.Fatal("full recompute should be eligible for small scene")
	}
	if plan.RejectedReason[StrategySparse] == "" {
		t.Fatal("sparse rejection reason missing")
	}
}

func TestPlanLargeAvoidFull(t *testing.T) {
	plan := Plan(PlanInputs{ShardCount: 32, ViewCount: 128, VoxelCount: 500000,
		CameraChangeRange: 0.8, Budget: 1e6})
	if plan.Selected == StrategyFull {
		t.Fatalf("large scene selected full recompute: %+v", plan)
	}
	if plan.Eligible[StrategyFull] {
		t.Fatal("full recompute must be ineligible for large scene")
	}
	if plan.RejectedReason[StrategyFull] == "" {
		t.Fatal("full rejection reason missing")
	}
}

func TestPlanDeterministic(t *testing.T) {
	in := PlanInputs{ShardCount: 8, ViewCount: 32, VoxelCount: 80000,
		CameraChangeRange: 0.5, Budget: 5e6}
	p1 := Plan(in)
	p2 := Plan(in)
	if p1.Selected != p2.Selected || p1.Scores[p1.Selected] != p2.Scores[p2.Selected] {
		t.Fatal("planner non-deterministic")
	}
}
