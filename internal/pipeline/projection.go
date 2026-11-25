package pipeline

import "sort"

// ValidateConfigAndManifest enforces the global preconditions that are not
// tied to a single shard (requirement 8).
func ValidateConfigAndManifest(m *Manifest) error {
	if len(m.Shards) == 0 || totalViews(m) == 0 {
		return bizError(CodeEmptyViews, "scene contains zero views field views")
	}
	if m.Config.SampleDensity <= 0 {
		return bizError(CodeBadDensity, "sample_density must be positive field sample_density")
	}
	if m.Config.VoxelResolution <= 0 || m.Config.RayStep <= 0 {
		return bizError(CodeInvalidRequest, "voxel_resolution and ray_step must be positive")
	}
	return nil
}

func totalViews(m *Manifest) int {
	n := 0
	for _, s := range m.Shards {
		n += len(s.Views)
	}
	return n
}

// ValidateCamera checks pose orthogonality and intrinsic focal length. Errors
// carry both shard/view identity and the offending field name.
func ValidateCamera(shardID string, cam Camera) error {
	if field, ok := validateRotation(cam.R); !ok {
		return &BizError{Code: CodePoseInvalid, Message: "rotation matrix is not orthonormal",
			ShardID: shardID, ViewID: cam.ViewID, Field: field}
	}
	if field, ok := validateIntrinsics(cam.K); !ok {
		return &BizError{Code: CodePoseInvalid, Message: "intrinsic focal length is zero or non-finite",
			ShardID: shardID, ViewID: cam.ViewID, Field: field}
	}
	if cam.Width <= 0 || cam.Height <= 0 {
		return &BizError{Code: CodePoseInvalid, Message: "image dimensions are invalid",
			ShardID: shardID, ViewID: cam.ViewID, Field: "width"}
	}
	return nil
}

// ProjectView runs the projection-sampling stage for one view. Each sampled
// pixel is unprojected to a world ray; the ray is marched with the configured
// step and every point inside the truncation band of the surface is recorded
// with its shard id, world coordinate and color.
func ProjectView(cfg PipelineConfig, shardID string, cam Camera, img *ViewImage) ([]Contribution, error) {
	if err := ValidateCamera(shardID, cam); err != nil {
		return nil, err
	}
	// Pixel stride derived from density. Density 1 = every pixel; 0.5 = every
	// second pixel; 2 = sub-pixel sampling is not meaningful for integer
	// pixels, so it is clamped to stride 1 but still validated as positive.
	stride := int(1 / cfg.SampleDensity)
	if stride < 1 {
		stride = 1
	}
	var out []Contribution
	for y := 0; y < cam.Height; y += stride {
		for x := 0; x < cam.Width; x += stride {
			color, err := img.Pixel(x, y)
			if err != nil {
				if be, ok := err.(*BizError); ok {
					be.ShardID = shardID
					be.ViewID = cam.ViewID
					return nil, be
				}
				return nil, err
			}
			origin, dir := cameraRay(cam, x, y)
			surfaceDepth := byteToDepth(color, cfg.RayFar)
			if color == 0 {
				// No surface observed along this ray: no contribution. This is
				// legitimate sensor data, not a silently skipped invalid input.
				continue
			}
			near := surfaceDepth - cfg.Truncation
			far := surfaceDepth + cfg.Truncation
			if near < cfg.RayNear {
				near = cfg.RayNear
			}
			for d := near; d <= far+cfg.RayStep/2; d += cfg.RayStep {
				if d > far {
					d = far
				}
				world := vecAdd(origin, vecScale(dir, d))
				out = append(out, Contribution{
					ShardID: shardID,
					ViewID:  cam.ViewID,
					PixelX:  x,
					PixelY:  y,
					Depth:   round6(d),
					World:   roundVec(world),
					Color:   color,
				})
				if d == far {
					break
				}
			}
		}
	}
	return out, nil
}

// ProjectShard processes every view of a shard in declared order and returns
// per-view contribution slices (view order preserved).
func ProjectShard(cfg PipelineConfig, shard ShardSpec, images map[string]*ViewImage) (map[string][]Contribution, error) {
	result := map[string][]Contribution{}
	for _, cam := range shard.Views {
		img, ok := images[cam.ViewID]
		if !ok {
			return nil, &BizError{Code: CodeNotFound, Message: "image tile missing",
				ShardID: shard.ShardID, ViewID: cam.ViewID, Field: "image"}
		}
		recs, err := ProjectView(cfg, shard.ShardID, cam, img)
		if err != nil {
			return nil, err
		}
		result[cam.ViewID] = recs
	}
	return result, nil
}

func byteToDepth(b uint8, far float64) float64 {
	return float64(b) * far / 255.0
}

// round6 rounds to a fixed number of decimal places so the same geometric
// value always serializes identically regardless of summation order.
func round6(v float64) float64 {
	const p = 1e9
	return float64(int64(v*p+sign(v)*0.5)) / p
}

func sign(v float64) float64 {
	if v < 0 {
		return -1
	}
	return 1
}

func roundVec(v Vec3) Vec3 {
	return Vec3{round6(v[0]), round6(v[1]), round6(v[2])}
}

// sortedContributions returns a deterministic ordering copy used for hashing
// and equality comparison (primary order is already deterministic, this is a
// defensive canonical sort).
func sortedContributions(in []Contribution) []Contribution {
	out := append([]Contribution(nil), in...)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.ViewID != b.ViewID {
			return a.ViewID < b.ViewID
		}
		if a.PixelY != b.PixelY {
			return a.PixelY < b.PixelY
		}
		if a.PixelX != b.PixelX {
			return a.PixelX < b.PixelX
		}
		return a.Depth < b.Depth
	})
	return out
}
