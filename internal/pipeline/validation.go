package pipeline

import (
	"math"
	"sort"
)

// ValidateReconstruction compares the fused point cloud against the manifest's
// ground-truth samples (the sole truth source). For every truth point it finds
// the nearest surviving voxel center for point deviation and reprojects the
// truth point into its declaring camera for reprojection error.
func ValidateReconstruction(cfg PipelineConfig, m *Manifest, field GlobalField) (ErrorStats, *BizError) {
	stats := ErrorStats{}
	if len(m.Truth) == 0 {
		return stats, nil
	}
	camByView := map[string]Camera{}
	for _, shard := range m.Shards {
		for _, cam := range shard.Views {
			camByView[cam.ViewID] = cam
		}
	}
	keys := make([]VoxelKey, 0, len(field.Voxels))
	for _, v := range field.Voxels {
		keys = append(keys, v.Key)
	}
	sort.SliceStable(keys, func(i, j int) bool { return keyLess(keys[i], keys[j]) })

	var sumDev, sumPx, worstDev float64
	worstView := ""
	for _, tp := range m.Truth {
		cam, ok := camByView[tp.ViewID]
		if !ok {
			return stats, bizError(CodeInvalidRequest, "truth references unknown view %s", tp.ViewID)
		}
		_, nearestDist := nearestVoxel(tp.Point, keys, cfg.VoxelResolution)
		stats.ComparedPoints++
		sumDev += nearestDist
		if nearestDist > worstDev {
			worstDev = nearestDist
			worstView = tp.ViewID
		}
		if nearestDist > cfg.DistTolerance {
			stats.OverTolerance++
		}

		// Reprojection error: project the truth point back into the declaring
		// camera and compare against its manifest pixel coordinates.
		u, v, depth := projectWorld(cam, tp.Point)
		if depth <= 0 {
			return stats, &BizError{Code: CodeNumericMismatch, ViewID: tp.ViewID,
				Message: "truth point projects behind camera"}
		}
		rpx := math.Hypot(u-float64(tp.PixelX), v-float64(tp.PixelY))
		sumPx += rpx
		if rpx > stats.MaxReprojPx {
			stats.MaxReprojPx = rpx
		}
	}
	stats.MaxPointDev = round6(worstDev)
	stats.MeanPointDev = round6(sumDev / float64(stats.ComparedPoints))
	stats.MeanReprojPx = round6(sumPx / float64(stats.ComparedPoints))
	stats.MaxReprojPx = round6(stats.MaxReprojPx)

	if worstDev > cfg.DistTolerance || stats.MaxReprojPx > cfg.PixelTolerance {
		return stats, &BizError{
			Code:    CodeNumericMismatch,
			ViewID:  worstView,
			MaxDev:  stats.MaxPointDev,
			Message: "reconstruction exceeds numeric tolerance",
			Details: map[string]string{
				"dist_tolerance":  floatToString(cfg.DistTolerance),
				"pixel_tolerance": floatToString(cfg.PixelTolerance),
			},
		}
	}
	return stats, nil
}

func nearestVoxel(p Vec3, sortedKeys []VoxelKey, resolution float64) (VoxelKey, float64) {
	best := VoxelKey{}
	bestDist := math.Inf(1)
	for _, k := range sortedKeys {
		c := voxelCenter(k, resolution)
		d := vecNorm(vecSub(p, c))
		if d < bestDist || (d == bestDist && keyLess(k, best)) {
			bestDist = d
			best = k
		}
	}
	return best, round6(bestDist)
}
