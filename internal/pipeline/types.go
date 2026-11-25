package pipeline

// Mat3 is a row-major 3x3 matrix.
type Mat3 [9]float64

// Vec3 is a 3-vector.
type Vec3 [3]float64

// Intrinsics holds the pinhole camera intrinsic matrix:
//
//	fx  0  cx
//	0  fy  cy
//	0   0   1
type Intrinsics struct {
	FX float64 `json:"fx"`
	FY float64 `json:"fy"`
	CX float64 `json:"cx"`
	CY float64 `json:"cy"`
}

// Camera is the extrinsic/intrinsic description of one view.
// World->camera transform is Xc = R * Xw + T.
type Camera struct {
	ViewID string     `json:"view_id"`
	R      Mat3       `json:"r"`
	T      Vec3       `json:"t"`
	K      Intrinsics `json:"k"`
	Width  int        `json:"width"`
	Height int        `json:"height"`
}

// GroundTruthPoint is a manifest-declared sampled 3D coordinate. The manifest
// is the sole source of validation truth.
type GroundTruthPoint struct {
	ViewID string `json:"view_id"`
	PixelX int    `json:"px"`
	PixelY int    `json:"py"`
	Point  Vec3   `json:"point"`
	Color  uint8  `json:"color"`
}

// Manifest is the JSON scene manifest produced by the fixed-seed generator.
type Manifest struct {
	SchemaVersion int                `json:"schema_version"`
	Seed          int64              `json:"seed"`
	Shards        []ShardSpec        `json:"shards"`
	Config        PipelineConfig     `json:"config"`
	Truth         []GroundTruthPoint `json:"truth"`
}

// ShardSpec groups one or more views into an indivisible processing unit.
type ShardSpec struct {
	ShardID string   `json:"shard_id"`
	Views   []Camera `json:"views"`
}

// PipelineConfig contains every parameter that can change reconstruction.
// All numeric fields participate in the config version fingerprint.
type PipelineConfig struct {
	VoxelResolution float64 `json:"voxel_resolution"`
	Truncation      float64 `json:"truncation"`
	FusionRadius    float64 `json:"fusion_radius"`
	SampleDensity   float64 `json:"sample_density"`
	RayStep         float64 `json:"ray_step"`
	RayNear         float64 `json:"ray_near"`
	RayFar          float64 `json:"ray_far"`
	SparsityPrune   int     `json:"sparsity_prune"`
	DistTolerance   float64 `json:"dist_tolerance"`
	PixelTolerance  float64 `json:"pixel_tolerance"`
	QueryBudgetMs   int64   `json:"query_budget_ms"`
}

// Strategy identifies one of the three adaptive execution paths.
type Strategy string

const (
	StrategyFull       Strategy = "FULL_RECOMPUTE"
	StrategyBlockCache Strategy = "BLOCK_CACHE"
	StrategySparse     Strategy = "SPARSE_INCREMENTAL"
)

// Stage names. The dependency chain is strictly linear.
const (
	StageProjection = "projection"
	StageLocal      = "local_volume"
	StageFusion     = "global_fusion"
	StageValidate   = "validation"
)

// DownstreamStages returns the stage itself and every downstream stage.
func DownstreamStages(stage string) []string {
	order := []string{StageProjection, StageLocal, StageFusion, StageValidate}
	for i, s := range order {
		if s == stage {
			return order[i:]
		}
	}
	return order
}

// Contribution is one projection-sampling record (requirement 1 output).
type Contribution struct {
	ShardID string  `json:"shard_id"`
	ViewID  string  `json:"view_id"`
	PixelX  int     `json:"px"`
	PixelY  int     `json:"py"`
	Depth   float64 `json:"depth"`
	World   Vec3    `json:"world"`
	Color   uint8   `json:"color"`
}

// VoxelKey is the integer coordinate of a voxel in the global grid.
type VoxelKey [3]int32

// LocalAccum is the deterministic per-view/per-voxel accumulator. All running
// quantities are ordered inputs to later normalization so the fusion weight
// normalization can be recomputed independently.
type LocalAccum struct {
	Key       VoxelKey `json:"key"`
	ColorSum  float64  `json:"color_sum"`
	WeightSum float64  `json:"weight_sum"`
	Hits      int      `json:"hits"`
}

// LocalVolume is the stage-2 artifact of one view.
type LocalVolume struct {
	ShardID string       `json:"shard_id"`
	ViewID  string       `json:"view_id"`
	Voxels  []LocalAccum `json:"voxels"`
}

// GlobalVoxel is one fused, pruned voxel in the global field.
type GlobalVoxel struct {
	Key       VoxelKey `json:"key"`
	Color     float64  `json:"color"`
	WeightSum float64  `json:"weight_sum"`
	Hits      int      `json:"hits"`
	Pruned    bool     `json:"pruned,omitempty"`
	Reason    string   `json:"reason,omitempty"`
}

// GlobalField is the stage-3 artifact.
type GlobalField struct {
	Voxels []GlobalVoxel `json:"voxels"`
}

// PointCloudSummary is the deterministic queryable summary of a field.
type PointCloudSummary struct {
	VoxelCount  int        `json:"voxel_count"`
	PrunedCount int        `json:"pruned_count"`
	ColorMin    float64    `json:"color_min"`
	ColorMax    float64    `json:"color_max"`
	ColorMean   float64    `json:"color_mean"`
	BoundsMin   VoxelKey   `json:"bounds_min"`
	BoundsMax   VoxelKey   `json:"bounds_max"`
	FirstVoxels []VoxelKey `json:"first_voxels"`
}

// ErrorStats aggregates validation statistics.
type ErrorStats struct {
	ComparedPoints int     `json:"compared_points"`
	MaxPointDev    float64 `json:"max_point_deviation"`
	MeanPointDev   float64 `json:"mean_point_deviation"`
	MaxReprojPx    float64 `json:"max_reprojection_px"`
	MeanReprojPx   float64 `json:"mean_reprojection_px"`
	OverTolerance  int     `json:"over_tolerance"`
}

// SparsityStats describes pruning.
type SparsityStats struct {
	BeforePrune int `json:"before_prune"`
	AfterPrune  int `json:"after_prune"`
	Pruned      int `json:"pruned"`
}

// PlanRecord captures the adaptive strategy decision (requirement 3).
type PlanRecord struct {
	Selected       Strategy             `json:"selected"`
	Reason         string               `json:"reason"`
	Scores         map[Strategy]float64 `json:"scores"`
	Eligible       map[Strategy]bool    `json:"eligible"`
	RejectedReason map[Strategy]string  `json:"rejected_reason"`
	Inputs         PlanInputs           `json:"inputs"`
}

// PlanInputs are the deterministic inputs of the scoring function.
type PlanInputs struct {
	ShardCount        int     `json:"shard_count"`
	ViewCount         int     `json:"view_count"`
	VoxelCount        int64   `json:"voxel_count"`
	CameraChangeRange float64 `json:"camera_change_range"`
	Budget            float64 `json:"global_budget"`
}

// Checkpoint is the durable, atomically written stage record (requirement 5).
type Checkpoint struct {
	Seq          int64    `json:"seq"`
	ShardID      string   `json:"shard_id"`
	Stage        string   `json:"stage"`
	ViewID       string   `json:"view_id,omitempty"`
	InputDigest  string   `json:"input_digest"`
	ConfigVer    string   `json:"config_version"`
	ArtifactHash string   `json:"artifact_hash"`
	ArtifactPath string   `json:"artifact_path"`
	Strategy     Strategy `json:"strategy,omitempty"`
	UpdatedAt    string   `json:"updated_at"`
}
