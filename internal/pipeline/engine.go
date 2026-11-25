package pipeline

import (
	"os"
	"path/filepath"
	"time"
)

// Engine drives the four reconstruction stages over one workspace directory.
type Engine struct {
	workDir string
	store   *Store
	cache   *QueryCache
	stop    chan struct{}
}

// EngineResult is the complete, deterministic output of one reconstruction.
type EngineResult struct {
	JobID       string            `json:"job_id"`
	Strategy    Strategy          `json:"strategy"`
	Plan        PlanRecord        `json:"plan"`
	ConfigVer   string            `json:"config_version"`
	SceneFP     SceneFingerprints `json:"scene_fingerprints"`
	Summary     PointCloudSummary `json:"summary"`
	Sparsity    SparsityStats     `json:"sparsity"`
	Errors      ErrorStats        `json:"error_stats"`
	Checkpoints []Checkpoint      `json:"checkpoints"`
	ReusedViews []string          `json:"reused_views"`
	FieldHash   string            `json:"field_hash"`
	ArtifactDir string            `json:"artifact_dir"`
}

func NewEngine(workDir string, store *Store, cache *QueryCache) *Engine {
	return &Engine{workDir: workDir, store: store, cache: cache, stop: make(chan struct{})}
}

// SignalStop requests a graceful stop. The current indivisible shard unit is
// completed first (requirement 5).
func (e *Engine) SignalStop() {
	select {
	case <-e.stop:
	default:
		close(e.stop)
	}
}

func (e *Engine) stopRequested() bool {
	select {
	case <-e.stop:
		return true
	default:
		return false
	}
}

func (e *Engine) artifactDir(cfgVer string) string {
	return filepath.Join(e.workDir, "artifacts", cfgVer[:12])
}

// Reconstruct runs (or resumes) a reconstruction. When forceFull is true the
// engine ignores reusable local artifacts and rebuilds everything, which is
// used to prove byte-equivalence with incremental runs.
func (e *Engine) Reconstruct(jobID string, forceFull bool) (*EngineResult, error) {
	manifest, images, err := LoadDataset(e.workDir)
	if err != nil {
		return nil, err
	}
	if err := ValidateConfigAndManifest(manifest); err != nil {
		return nil, err
	}
	fp, err := ComputeFingerprints(manifest, images)
	if err != nil {
		return nil, err
	}
	cfgVer := ConfigVersion(manifest.Config)
	artDir := e.artifactDir(cfgVer)
	if err := os.MkdirAll(artDir, 0o755); err != nil {
		return nil, bizError(CodeInternal, "artifact dir: %v", err)
	}

	// Adaptive strategy based on view scale and voxel resolution.
	voxelCount := estimateVoxelCount(manifest)
	camRange := cameraChangeRange(manifest)
	plan := Plan(PlanInputs{
		ShardCount:        len(manifest.Shards),
		ViewCount:         totalViews(manifest),
		VoxelCount:        voxelCount,
		CameraChangeRange: camRange,
		Budget:            float64(globalBudget(manifest)),
	})
	strategy := plan.Selected
	if forceFull {
		strategy = StrategyFull
	}

	result := &EngineResult{
		JobID:       jobID,
		Strategy:    strategy,
		Plan:        plan,
		ConfigVer:   cfgVer,
		SceneFP:     fp,
		ArtifactDir: artDir,
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = e.store.UpsertJob(jobID, string(strategy), "running", cfgVer, fp.ContentFP, now, now, plan.Reason)

	// Stage 1 + 2 are per-view, published as indivisible per-shard units.
	volumes := make([]LocalVolume, 0, totalViews(manifest))
	for _, shard := range manifest.Shards {
		if e.stopRequested() {
			_ = e.store.UpsertJob(jobID, string(strategy), "stopped", cfgVer, fp.ContentFP, now,
				time.Now().UTC().Format(time.RFC3339Nano), "graceful stop before shard "+shard.ShardID)
			return result, bizError(CodeInternal, "reconstruction stopped gracefully at shard boundary")
		}
		shardVols, reused, err := e.processShard(manifest, images, fp, cfgVer, artDir, shard, strategy, forceFull)
		if err != nil {
			return result, err
		}
		volumes = append(volumes, shardVols...)
		result.ReusedViews = append(result.ReusedViews, reused...)
	}
	if e.stopRequested() {
		_ = e.store.UpsertJob(jobID, string(strategy), "stopped", cfgVer, fp.ContentFP,
			time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano),
			"graceful stop after final shard unit")
		return result, bizError(CodeInternal, "reconstruction stopped gracefully at shard boundary")
	}

	// Stage 3: global fusion + sparsification (global unit).
	field, sparsity, pruned, err := e.fuseAndPrune(manifest, fp, cfgVer, artDir, volumes)
	if err != nil {
		return result, err
	}
	fieldBytes, err := CanonicalEncode(field)
	if err != nil {
		return result, err
	}
	result.FieldHash = sha256Bytes(fieldBytes)
	result.Summary = SummarizePointCloud(field, pruned)
	result.Sparsity = sparsity

	// Stage 4: numeric validation against manifest truth.
	stats, numErr := ValidateReconstruction(manifest.Config, manifest, field)
	result.Errors = stats
	if err := e.publishGlobal(StageValidate, cfgVer, fp, artDir, stats); err != nil {
		return result, err
	}
	cps, _ := e.store.ListCheckpoints()
	result.Checkpoints = cps

	now2 := time.Now().UTC().Format(time.RFC3339Nano)
	if numErr != nil {
		_ = e.store.UpsertJob(jobID, string(strategy), "numeric_mismatch", cfgVer, fp.ContentFP, now, now2, numErr.Message)
		numErr.Emit()
		return result, numErr
	}
	_ = e.store.UpsertJob(jobID, string(strategy), "completed", cfgVer, fp.ContentFP, now, now2, "ok")
	return result, nil
}

// processShard handles stages 1 and 2 for every view of one shard and publishes
// a single contiguous checkpoint sequence for that indivisible unit.
func (e *Engine) processShard(manifest *Manifest, images map[string]*ViewImage, fp SceneFingerprints,
	cfgVer, artDir string, shard ShardSpec, strategy Strategy, forceFull bool) ([]LocalVolume, []string, error) {
	var volumes []LocalVolume
	var reused []string

	for _, cam := range shard.Views {
		img := images[cam.ViewID]
		projDigest := projectionInputDigest(cam.ViewID, fp, cfgVer)
		localDigest := localInputDigest(cam.ViewID, projDigest, manifest.Config)

		// Reuse path: only valid when the local checkpoint input digest matches
		// and the strategy permits cache/incremental reuse.
		canReuse := !forceFull && strategy != StrategyFull
		if cp, _ := e.store.GetCheckpoint(shard.ShardID, StageLocal, cam.ViewID); cp != nil &&
			cp.ConfigVer == cfgVer && cp.InputDigest == localDigest && canReuse {
			var vol LocalVolume
			if err := readJSONFile(cp.ArtifactPath, &vol); err == nil {
				if raw, rerr := os.ReadFile(cp.ArtifactPath); rerr == nil &&
					sha256Bytes(raw) == cp.ArtifactHash {
					volumes = append(volumes, vol)
					reused = append(reused, cam.ViewID)
					continue
				}
				// Hash mismatch means the on-disk artifact was tampered with or
				// corrupted; fall through and recompute rather than trust it.
			}
		}

		// Stage 1: projection sampling.
		recs, err := ProjectView(manifest.Config, shard.ShardID, cam, img)
		if err != nil {
			return nil, nil, err
		}
		recs = sortedContributions(recs)
		projPath := filepath.Join(artDir, shard.ShardID, cam.ViewID+".projection.json")
		projHash, err := WriteArtifact(projPath, recs)
		if err != nil {
			return nil, nil, err
		}
		if err := e.publishView(shard.ShardID, StageProjection, cam.ViewID, projDigest, cfgVer, projHash, projPath, strategy); err != nil {
			return nil, nil, err
		}

		// Stage 2: local voxel volume.
		vol := BuildLocalVolume(manifest.Config.VoxelResolution, shard.ShardID, cam.ViewID, recs)
		localPath := filepath.Join(artDir, shard.ShardID, cam.ViewID+".local.json")
		localHash, err := WriteArtifact(localPath, vol)
		if err != nil {
			return nil, nil, err
		}
		if err := e.publishView(shard.ShardID, StageLocal, cam.ViewID, localDigest, cfgVer, localHash, localPath, strategy); err != nil {
			return nil, nil, err
		}
		volumes = append(volumes, vol)
	}
	return volumes, reused, nil
}

func (e *Engine) fuseAndPrune(manifest *Manifest, fp SceneFingerprints, cfgVer, artDir string,
	volumes []LocalVolume) (GlobalField, SparsityStats, []GlobalVoxel, error) {
	field := FuseGlobal(manifest.Config, volumes)
	kept, pruned := PruneSparse(field, manifest.Config.SparsityPrune)
	stats := SparsityStats{
		BeforePrune: len(field.Voxels),
		AfterPrune:  len(kept.Voxels),
		Pruned:      len(pruned),
	}
	fusionDigest := MustDigest(struct {
		Stage       string
		LocalDigest []string
		Cfg         PipelineConfig
	}{StageFusion, localVolumeDigests(volumes), manifest.Config})
	path := filepath.Join(artDir, "global-field.json")
	hash, err := WriteArtifact(path, kept)
	if err != nil {
		return kept, stats, pruned, err
	}
	if err := e.publishGlobal(StageFusion, cfgVer, fp, artDir, hash, fusionDigest, path); err != nil {
		return kept, stats, pruned, err
	}
	prunePath := filepath.Join(artDir, "pruned.json")
	if _, err := WriteArtifact(prunePath, PruneReasons(pruned)); err != nil {
		return kept, stats, pruned, err
	}
	return kept, stats, pruned, nil
}

// publishView allocates the next contiguous per-shard sequence and publishes
// one view-level checkpoint through the conditional SQLite update.
func (e *Engine) publishView(shardID, stage, viewID, inputDigest, cfgVer, hash, path string, strategy Strategy) error {
	last, err := e.store.LastSeq(shardID)
	if err != nil {
		return err
	}
	cp := Checkpoint{
		Seq:          last + 1,
		ShardID:      shardID,
		Stage:        stage,
		ViewID:       viewID,
		InputDigest:  inputDigest,
		ConfigVer:    cfgVer,
		ArtifactHash: hash,
		ArtifactPath: path,
		Strategy:     strategy,
		UpdatedAt:    time.Now().UTC().Format(time.RFC3339Nano),
	}
	res, err := e.store.PublishCheckpoint(cp)
	if err != nil {
		if be, ok := err.(*BizError); ok {
			be.Emit()
		}
		return err
	}
	_ = res
	return nil
}

// publishGlobal publishes global-stage (fusion/validation) checkpoints under
// the reserved shard id "__global__".
func (e *Engine) publishGlobal(stage, cfgVer string, fp SceneFingerprints, artDir string, payload ...any) error {
	var inputDigest, hash, path string
	if stage == StageFusion {
		fusionDigest := payload[1].(string)
		inputDigest = fusionDigest
		hash = payload[0].(string)
		path = payload[2].(string)
	} else {
		stats := payload[0].(ErrorStats)
		inputDigest = MustDigest(struct {
			Stage  string
			Stats  ErrorStats
			CfgVer string
		}{StageValidate, stats, cfgVer})
		p := filepath.Join(artDir, "validation.json")
		h, err := WriteArtifact(p, stats)
		if err != nil {
			return err
		}
		hash, path = h, p
	}
	last, err := e.store.LastSeq(globalShard)
	if err != nil {
		return err
	}
	cp := Checkpoint{
		Seq:          last + 1,
		ShardID:      globalShard,
		Stage:        stage,
		InputDigest:  inputDigest,
		ConfigVer:    cfgVer,
		ArtifactHash: hash,
		ArtifactPath: path,
		UpdatedAt:    time.Now().UTC().Format(time.RFC3339Nano),
	}
	if _, err := e.store.PublishCheckpoint(cp); err != nil {
		if be, ok := err.(*BizError); ok {
			be.Emit()
		}
		return err
	}
	return nil
}

const globalShard = "__global__"

func localVolumeDigests(vols []LocalVolume) []string {
	out := make([]string, len(vols))
	for i, v := range vols {
		out[i] = MustDigest(v)
	}
	return out
}

func readJSONFile(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return decodeCanonical(b, v)
}

func estimateVoxelCount(m *Manifest) int64 {
	// Deterministic estimate from the ray budget per view and voxel resolution.
	span := (m.Config.RayFar - m.Config.RayNear) / m.Config.VoxelResolution
	perView := int64(float64(totalViews(m)) * span * 4)
	if perView < 1 {
		perView = 1
	}
	return perView
}

func globalBudget(m *Manifest) int64 {
	// Budget grows with views; fixed formula keeps planning reproducible.
	return int64(totalViews(m)) * 50000
}

func cameraChangeRange(m *Manifest) float64 {
	// Range of camera orientations measured by pairwise rotation difference.
	if len(m.Shards) == 0 {
		return 0
	}
	var maxDiff float64
	var cams []Camera
	for _, s := range m.Shards {
		cams = append(cams, s.Views...)
	}
	for i := 0; i < len(cams); i++ {
		for j := i + 1; j < len(cams); j++ {
			d := rotationDistance(cams[i].R, cams[j].R)
			if d > maxDiff {
				maxDiff = d
			}
		}
	}
	return round6(clamp(maxDiff/3.141592653589793, 0, 1))
}

func rotationDistance(a, b Mat3) float64 {
	diff := matMul(a, transpose(b))
	trace := diff[0] + diff[4] + diff[8]
	cosTheta := clamp((trace-1)/2, -1, 1)
	return arccos(cosTheta)
}
