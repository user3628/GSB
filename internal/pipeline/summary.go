package pipeline

import "sort"

// SummarizePointCloud produces the deterministic point-cloud summary used by
// the query endpoint. All ordering and aggregation are fixed functions of the
// field content.
func SummarizePointCloud(field GlobalField, pruned []GlobalVoxel) PointCloudSummary {
	sum := PointCloudSummary{VoxelCount: len(field.Voxels), PrunedCount: len(pruned)}
	if len(field.Voxels) == 0 {
		return sum
	}
	keys := make([]VoxelKey, 0, len(field.Voxels))
	var colorSum float64
	sum.ColorMin = field.Voxels[0].Color
	sum.ColorMax = field.Voxels[0].Color
	sum.BoundsMin = field.Voxels[0].Key
	sum.BoundsMax = field.Voxels[0].Key
	for _, v := range field.Voxels {
		keys = append(keys, v.Key)
		colorSum += v.Color
		if v.Color < sum.ColorMin {
			sum.ColorMin = v.Color
		}
		if v.Color > sum.ColorMax {
			sum.ColorMax = v.Color
		}
		expandBounds(&sum.BoundsMin, &sum.BoundsMax, v.Key)
	}
	sum.ColorMean = round6(colorSum / float64(len(field.Voxels)))
	sort.SliceStable(keys, func(i, j int) bool { return keyLess(keys[i], keys[j]) })
	limit := 10
	if len(keys) < limit {
		limit = len(keys)
	}
	sum.FirstVoxels = keys[:limit]
	return sum
}

func expandBounds(min, max *VoxelKey, k VoxelKey) {
	for i := 0; i < 3; i++ {
		if k[i] < (*min)[i] {
			(*min)[i] = k[i]
		}
		if k[i] > (*max)[i] {
			(*max)[i] = k[i]
		}
	}
}

// PruneReasons returns a deterministic list of pruned voxel reasons.
func PruneReasons(pruned []GlobalVoxel) []GlobalVoxel {
	out := append([]GlobalVoxel(nil), pruned...)
	sort.SliceStable(out, func(i, j int) bool { return keyLess(out[i].Key, out[j].Key) })
	return out
}
