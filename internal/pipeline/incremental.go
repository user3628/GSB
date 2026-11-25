package pipeline

import "reflect"

// ComputeFieldFromScratch rebuilds the global field purely in memory from the
// current manifest and images, touching no cache or checkpoint. It is the
// reference for proving byte-equivalence of incremental and full runs.
func ComputeFieldFromScratch(m *Manifest, images map[string]*ViewImage) (GlobalField, error) {
	if err := ValidateConfigAndManifest(m); err != nil {
		return GlobalField{}, err
	}
	var volumes []LocalVolume
	for _, shard := range m.Shards {
		recs, err := ProjectShard(m.Config, shard, images)
		if err != nil {
			return GlobalField{}, err
		}
		for _, cam := range shard.Views {
			vol := BuildLocalVolume(m.Config.VoxelResolution, shard.ShardID, cam.ViewID,
				sortedContributions(recs[cam.ViewID]))
			volumes = append(volumes, vol)
		}
	}
	field := FuseGlobal(m.Config, volumes)
	kept, _ := PruneSparse(field, m.Config.SparsityPrune)
	return kept, nil
}

// VerifyFieldEquivalence compares an incrementally produced field against a
// from-scratch reference. On mismatch it returns E_INCREMENTAL_MISMATCH with
// the first differing voxel coordinate (requirement 6).
func VerifyFieldEquivalence(incremental, reference GlobalField) error {
	incBytes, err := CanonicalEncode(incremental)
	if err != nil {
		return err
	}
	refBytes, err := CanonicalEncode(reference)
	if err != nil {
		return err
	}
	if string(incBytes) == string(refBytes) {
		return nil
	}
	first := firstDifferentVoxel(incremental, reference)
	return &BizError{
		Code:    CodeIncrementalMismatch,
		Message: "incremental result differs byte-for-byte from full rebuild",
		Voxel:   first,
		Details: map[string]string{
			"incremental_hash": sha256Bytes(incBytes),
			"reference_hash":   sha256Bytes(refBytes),
		},
	}
}

func firstDifferentVoxel(a, b GlobalField) VoxelKey {
	am := map[VoxelKey]GlobalVoxel{}
	bm := map[VoxelKey]GlobalVoxel{}
	var allKeys []VoxelKey
	seen := map[VoxelKey]bool{}
	for _, v := range a.Voxels {
		am[v.Key] = v
		if !seen[v.Key] {
			seen[v.Key] = true
			allKeys = append(allKeys, v.Key)
		}
	}
	for _, v := range b.Voxels {
		bm[v.Key] = v
		if !seen[v.Key] {
			seen[v.Key] = true
			allKeys = append(allKeys, v.Key)
		}
	}
	sortKeys(allKeys)
	for _, k := range allKeys {
		av, inA := am[k]
		bv, inB := bm[k]
		if inA != inB || !reflect.DeepEqual(av, bv) {
			return k
		}
	}
	return VoxelKey{}
}

func sortKeys(keys []VoxelKey) {
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keyLess(keys[j], keys[j-1]); j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
}

// EarliestChangedStage walks the linear stage chain and returns the earliest
// stage invalidated by a change. Pixel/camera changes invalidate projection;
// voxel resolution / truncation changes invalidate local volume; fusion
// radius / sparsity changes invalidate global fusion.
func EarliestChangedStage(change ChangeSet) string {
	if change.Pixels || change.Camera || change.SampleDensity {
		return StageProjection
	}
	if change.VoxelResolution || change.Truncation {
		return StageLocal
	}
	if change.FusionRadius || change.Sparsity {
		return StageFusion
	}
	return ""
}

// ChangeSet flags which reconstruction inputs changed between snapshots.
type ChangeSet struct {
	Pixels          bool
	Camera          bool
	VoxelResolution bool
	Truncation      bool
	FusionRadius    bool
	SampleDensity   bool
	Sparsity        bool
}

// DiffConfig compares two configurations into a ChangeSet.
func DiffConfig(old, new PipelineConfig) ChangeSet {
	return ChangeSet{
		VoxelResolution: old.VoxelResolution != new.VoxelResolution,
		Truncation:      old.Truncation != new.Truncation,
		FusionRadius:    old.FusionRadius != new.FusionRadius,
		SampleDensity:   old.SampleDensity != new.SampleDensity,
		Sparsity:        old.SparsityPrune != new.SparsityPrune,
	}
}
