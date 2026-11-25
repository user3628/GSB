package pipeline

import "sort"

// BuildLocalVolume quantizes one view's contributions into voxels and
// accumulates color and hit counts. Contributions are consumed in their
// deterministic projection order, so repeated hits on the same voxel within a
// view are merged in a reproducible sequence (requirement 2).
func BuildLocalVolume(resolution float64, shardID, viewID string, recs []Contribution) LocalVolume {
	acc := map[VoxelKey]*LocalAccum{}
	var order []VoxelKey
	for _, c := range recs {
		key := quantize(c.World, resolution)
		a, ok := acc[key]
		if !ok {
			a = &LocalAccum{Key: key}
			acc[key] = a
			order = append(order, key)
		}
		// Unit weight per sample; the weight sum is kept explicitly so the
		// later normalization is fully recomputable.
		a.ColorSum += float64(c.Color)
		a.WeightSum += 1
		a.Hits++
	}
	voxels := make([]LocalAccum, 0, len(order))
	for _, key := range order {
		voxels = append(voxels, *acc[key])
	}
	sortLocal(voxels)
	return LocalVolume{ShardID: shardID, ViewID: viewID, Voxels: voxels}
}

func sortLocal(v []LocalAccum) {
	sort.SliceStable(v, func(i, j int) bool { return keyLess(v[i].Key, v[j].Key) })
}

func keyLess(a, b VoxelKey) bool {
	if a[0] != b[0] {
		return a[0] < b[0]
	}
	if a[1] != b[1] {
		return a[1] < b[1]
	}
	return a[2] < b[2]
}

// globalAccum is the running global accumulator keyed by voxel coordinate.
type globalAccum struct {
	Key       VoxelKey
	ColorSum  float64
	WeightSum float64
	Hits      int
}

// FuseGlobal merges local volumes into a global field. Volumes are visited in
// shard order and then in-view order; within each volume voxels iterate in
// sorted key order. Each local voxel contributes to all global voxels within
// the integer fusion radius with an explicit radius weight, so the weighted
// average and its normalization are independently recomputable (requirement 2).
func FuseGlobal(cfg PipelineConfig, volumes []LocalVolume) GlobalField {
	radius := int32(cfg.FusionRadius)
	acc := map[VoxelKey]*globalAccum{}
	var order []VoxelKey

	ensure := func(k VoxelKey) *globalAccum {
		g, ok := acc[k]
		if !ok {
			g = &globalAccum{Key: k}
			acc[k] = g
			order = append(order, k)
		}
		return g
	}

	for _, vol := range volumes {
		for _, lv := range vol.Voxels {
			localColor := lv.ColorSum / lv.WeightSum
			for dz := -radius; dz <= radius; dz++ {
				for dy := -radius; dy <= radius; dy++ {
					for dx := -radius; dx <= radius; dx++ {
						target := VoxelKey{lv.Key[0] + dx, lv.Key[1] + dy, lv.Key[2] + dz}
						dist := chebyshev(lv.Key, target)
						weight := radiusWeight(radius, dist) * lv.WeightSum
						g := ensure(target)
						g.ColorSum += localColor * weight
						g.WeightSum += weight
						g.Hits += lv.Hits
					}
				}
			}
		}
	}

	keys := append([]VoxelKey(nil), order...)
	sort.SliceStable(keys, func(i, j int) bool { return keyLess(keys[i], keys[j]) })

	field := GlobalField{Voxels: make([]GlobalVoxel, 0, len(keys))}
	for _, k := range keys {
		g := acc[k]
		color := g.ColorSum / g.WeightSum
		field.Voxels = append(field.Voxels, GlobalVoxel{
			Key:       k,
			Color:     round6(color),
			WeightSum: round6(g.WeightSum),
			Hits:      g.Hits,
		})
	}
	return field
}

// radiusWeight is the explicit, symmetric fusion kernel. Center weight is 1
// and each Chebyshev ring loses 1/(radius+1).
func radiusWeight(radius, dist int32) float64 {
	if radius == 0 {
		return 1
	}
	w := 1 - float64(dist)/float64(radius+1)
	return round6(w)
}

// PruneSparse applies the sparsity threshold. Voxels whose hit count is below
// the threshold are flagged (and removed from the active set). The pruning
// decision is deterministic and the reason is recorded for rejection queries.
func PruneSparse(field GlobalField, threshold int) (GlobalField, []GlobalVoxel) {
	kept := GlobalField{}
	var pruned []GlobalVoxel
	for _, v := range field.Voxels {
		if v.Hits < threshold {
			v.Pruned = true
			v.Reason = "hit_count_below_sparsity_threshold"
			pruned = append(pruned, v)
			continue
		}
		kept.Voxels = append(kept.Voxels, v)
	}
	return kept, pruned
}
