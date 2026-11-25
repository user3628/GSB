package pipeline

import "fmt"

// applyConfigPatch updates recognized numeric/int config fields. Unknown
// fields are rejected so operators cannot silently misconfigure a run.
func applyConfigPatch(cfg PipelineConfig, patch map[string]any) (PipelineConfig, error) {
	setFloat := func(name string, dst *float64) error {
		v, ok := patch[name]
		if !ok {
			return nil
		}
		f, ok := toFloat(v)
		if !ok {
			return bizError(CodeInvalidRequest, "field %s must be numeric", name)
		}
		*dst = f
		return nil
	}
	setInt := func(name string, dst *int) error {
		v, ok := patch[name]
		if !ok {
			return nil
		}
		f, ok := toFloat(v)
		if !ok || f != float64(int64(f)) {
			return bizError(CodeInvalidRequest, "field %s must be an integer", name)
		}
		*dst = int(int64(f))
		return nil
	}
	for k := range patch {
		switch k {
		case "voxel_resolution", "truncation", "fusion_radius", "sample_density",
			"ray_step", "ray_near", "ray_far", "dist_tolerance", "pixel_tolerance",
			"sparsity_prune", "query_budget_ms":
		default:
			return cfg, bizError(CodeInvalidRequest, "unknown config field %s", k)
		}
	}
	if err := setFloat("voxel_resolution", &cfg.VoxelResolution); err != nil {
		return cfg, err
	}
	if err := setFloat("truncation", &cfg.Truncation); err != nil {
		return cfg, err
	}
	if err := setFloat("fusion_radius", &cfg.FusionRadius); err != nil {
		return cfg, err
	}
	if err := setFloat("sample_density", &cfg.SampleDensity); err != nil {
		return cfg, err
	}
	if err := setFloat("ray_step", &cfg.RayStep); err != nil {
		return cfg, err
	}
	if err := setFloat("ray_near", &cfg.RayNear); err != nil {
		return cfg, err
	}
	if err := setFloat("ray_far", &cfg.RayFar); err != nil {
		return cfg, err
	}
	if err := setFloat("dist_tolerance", &cfg.DistTolerance); err != nil {
		return cfg, err
	}
	if err := setFloat("pixel_tolerance", &cfg.PixelTolerance); err != nil {
		return cfg, err
	}
	if err := setInt("sparsity_prune", &cfg.SparsityPrune); err != nil {
		return cfg, err
	}
	if v, ok := patch["query_budget_ms"]; ok {
		f, ok := toFloat(v)
		if !ok || f != float64(int64(f)) {
			return cfg, bizError(CodeInvalidRequest, "field query_budget_ms must be an integer")
		}
		cfg.QueryBudgetMs = int64(int64(f))
	}
	if cfg.SampleDensity <= 0 {
		return cfg, &BizError{Code: CodeBadDensity,
			Message: "sample_density must be positive", Field: "sample_density"}
	}
	return cfg, nil
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	default:
		return 0, false
	}
}

var _ = fmt.Sprintf
