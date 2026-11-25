// Package engine is the application core: it orchestrates the four stages,
// derives version fingerprints, persists transactional snapshots, distinguishes
// normal progression / full replacement / rollback and recovers after crashes.
package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"qaudit/internal/audit"
	"qaudit/internal/calib"
	"qaudit/internal/infer"
	"qaudit/internal/model"
	"qaudit/internal/store"
)

// Stage identifiers stored in pipeline_state.
const (
	StageInit = iota
	StageCalibrated
	StageInferred
	StageAudited
)

// Version kinds recorded in the versions table.
const (
	KindNormal      = "normal"
	KindReplacement = "replacement"
	KindRollback    = "rollback"
)

// Engine is the workbench facade used by both HTTP and CLI.
type Engine struct {
	mu       sync.Mutex
	dataDir  string
	store    *store.Store
	guard    *store.PublishGuard
	stopFile string

	Dataset         *model.Dataset
	CalibSet        *calib.CalibSet
	Fingerprints    model.StageFingerprints
	Version         int64
	Artifacts       []*model.LayerCalibration
	Pipeline        *infer.Pipeline
	Numeric         *infer.NumericReport
	Audit           *audit.Report
	InvalidationLog []Invalidation
	stopRequested   bool

	// CrashAfterLayer, when > 0, simulates a forced kill by returning after the
	// given 1-based layer count is published.
	CrashAfterLayer int
}

// Invalidation records why a layer's prior snapshot was discarded.
type Invalidation struct {
	Layer  string `json:"layer"`
	Reason string `json:"reason"`
}

// New opens the workbench against dataDir with its SQLite snapshot store.
func New(dataDir, dbPath string) (*Engine, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	st, err := store.Open(dbPath)
	if err != nil {
		return nil, err
	}
	guard, err := store.NewPublishGuard(st)
	if err != nil {
		st.Close()
		return nil, err
	}
	e := &Engine{
		dataDir:  dataDir,
		store:    st,
		guard:    guard,
		stopFile: filepath.Join(dataDir, "STOP"),
	}
	if err := e.loadDataset(); err != nil {
		return nil, err
	}
	return e, nil
}

// Close releases persistence handles.
func (e *Engine) Close() error {
	e.guard.Close()
	return e.store.Close()
}

func (e *Engine) loadDataset() error {
	b, err := os.ReadFile(filepath.Join(e.dataDir, "dataset.json"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var ds model.Dataset
	if err := json.Unmarshal(b, &ds); err != nil {
		return err
	}
	e.Dataset = &ds
	return nil
}

// SaveDataset persists the working dataset next to the generated artifacts.
func (e *Engine) SaveDataset(ds *model.Dataset) error {
	b, err := json.MarshalIndent(ds, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(e.dataDir, "dataset.json"), b, 0o644); err != nil {
		return err
	}
	e.Dataset = ds
	return nil
}

// RequestGracefulStop marks the run for graceful shutdown.
func (e *Engine) RequestGracefulStop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stopRequested = true
	return os.WriteFile(e.stopFile, []byte("stop\n"), 0o644)
}

// ClearStop removes a graceful-stop marker so a fresh run can proceed.
func (e *Engine) ClearStop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stopRequested = false
	if err := os.Remove(e.stopFile); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (e *Engine) stopSignaled() bool {
	if e.stopRequested {
		return true
	}
	if _, err := os.Stat(e.stopFile); err == nil {
		e.stopRequested = true
		return true
	}
	return false
}

// ComputeFingerprints derives the four stage digests and the combined version fingerprint.
func ComputeFingerprints(net model.Network, cs *calib.CalibSet, policy model.Policy) (model.StageFingerprints, error) {
	weightsFP, err := model.Digest(net)
	if err != nil {
		return model.StageFingerprints{}, err
	}
	policyFP, err := model.Digest(policy)
	if err != nil {
		return model.StageFingerprints{}, err
	}
	posFP, err := model.Digest(policy.SamplePositions)
	if err != nil {
		return model.StageFingerprints{}, err
	}
	combined := map[string]string{
		"weights":   weightsFP,
		"calib":     cs.ManifestDigest + ":" + cs.FeatureDigest,
		"policy":    policyFP,
		"positions": posFP,
	}
	versionFP, err := model.Digest(combined)
	if err != nil {
		return model.StageFingerprints{}, err
	}
	return model.StageFingerprints{
		WeightsDigest:      weightsFP,
		CalibDigest:        cs.ManifestDigest + ":" + cs.FeatureDigest,
		PolicyDigest:       policyFP,
		PositionsDigest:    posFP,
		VersionFingerprint: versionFP,
	}, nil
}

// Run executes (or resumes) the full four-stage pipeline.
func (e *Engine) Run() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Dataset == nil {
		return model.NewBizError(model.CodeState, "dataset not loaded", nil)
	}
	if err := e.stageCalibration(); err != nil {
		return err
	}
	if e.stopSignaled() {
		return nil
	}
	if err := e.stageInference(); err != nil {
		return err
	}
	if e.stopSignaled() {
		return nil
	}
	return e.stageAudit()
}

func (e *Engine) stageCalibration() error {
	ds := e.Dataset
	side := ds.InputDimRoot()
	cs, err := calib.Select(ds.Shards, e.dataDir, ds.Policy.CalibRatio, ds.Policy.Seed, side, side)
	if err != nil {
		return err
	}
	if err := calib.CheckFinite(cs); err != nil {
		return err
	}
	fps, err := ComputeFingerprints(ds.Network, cs, ds.Policy)
	if err != nil {
		return err
	}
	e.CalibSet = cs
	e.Fingerprints = fps

	latest, err := e.store.LatestVersion()
	if err != nil {
		return err
	}
	kind, targetVersion, err := e.classifyVersion(latest, fps.VersionFingerprint)
	if err != nil {
		return err
	}
	if kind == KindRollback {
		return model.NewBizError(model.CodeStaleVersion, "rollback to a previously superseded fingerprint rejected",
			map[string]string{"fingerprint": fps.VersionFingerprint, "latest_version": fmt.Sprintf("%d", latest)})
	}
	if kind == KindReplacement {
		if err := e.store.UpsertVersion(targetVersion, fps.VersionFingerprint, KindReplacement); err != nil {
			return err
		}
		e.InvalidationLog = e.propagateInvalidation(latest, targetVersion)
	}
	e.Version = targetVersion

	existing, err := e.store.ListSnapshots(targetVersion)
	if err != nil {
		return err
	}
	done := map[string]store.SnapshotRow{}
	for _, row := range existing {
		done[row.LayerName] = row
	}
	artifacts, engines, err := infer.CalibrateAll(ds, cs, fps.CalibDigest, fps.PolicyDigest)
	if err != nil {
		return err
	}
	published := make([]*model.LayerCalibration, 0, len(artifacts))
	for idx, art := range artifacts {
		if row, ok := done[art.LayerName]; ok {
			if row.ArtifactDigest != art.ArtifactDigest {
				return model.NewBizError(model.CodeStaleVersion, "reused artifact digest mismatch",
					map[string]string{"layer": art.LayerName, "version": fmt.Sprintf("%d", targetVersion)})
			}
			published = append(published, art)
			continue
		}
		err := e.guard.Publish(store.PublishRequest{
			Version:     targetVersion,
			ExpectedSeq: int64(len(published) + 1),
			LayerName:   art.LayerName,
			InputFP:     fps.CalibDigest,
			PolicyFP:    fps.PolicyDigest,
			Digest:      art.ArtifactDigest,
			Artifact:    art,
		})
		if err != nil {
			return err
		}
		published = append(published, art)
		// Simulate a forced kill immediately after the Nth layer transaction commits.
		if e.CrashAfterLayer > 0 && idx+1 == e.CrashAfterLayer {
			_ = e.store.SaveState(targetVersion, fps.VersionFingerprint, StageInit)
			return model.NewBizError(model.CodeState, "simulated forced termination after layer publish",
				map[string]string{"layer": art.LayerName, "published": fmt.Sprintf("%d", len(published))})
		}
	}
	if len(published) != len(ds.Network.Layers) {
		return e.store.SaveState(targetVersion, fps.VersionFingerprint, StageInit)
	}
	e.Artifacts = published
	e.Pipeline = &infer.Pipeline{
		Dataset:   e.Dataset,
		CalibSet:  cs,
		Engines:   engines,
		Artifacts: artifacts,
		Tolerance: ds.Policy.Tolerance,
		DataDir:   e.dataDir,
	}
	return e.store.SaveState(targetVersion, fps.VersionFingerprint, StageCalibrated)
}

func (e *Engine) stageInference() error {
	report, err := e.Pipeline.Compare()
	e.Numeric = report
	if err != nil && model.CodeOf(err) != model.CodeNumericMismatch {
		return err
	}
	if saveErr := e.store.SaveState(e.Version, e.Fingerprints.VersionFingerprint, StageInferred); saveErr != nil {
		return saveErr
	}
	// Numeric mismatch is a reported audit finding, not a pipeline abort: stage 4
	// still runs so attribution and bounds are available for review. The mismatch
	// is surfaced through the numeric report and the returned error at Run() end.
	return nil
}

func (e *Engine) stageAudit() error {
	report, err := audit.Run(e.Pipeline, e.Fingerprints.VersionFingerprint)
	if err != nil {
		return err
	}
	e.Audit = report
	if saveErr := e.store.SaveState(e.Version, e.Fingerprints.VersionFingerprint, StageAudited); saveErr != nil {
		return saveErr
	}
	if e.Numeric != nil {
		if worst := worstDeviation(e.Numeric); worst != nil && worst.MaxAbsDev > e.Pipeline.Tolerance {
			return model.NewBizError(model.CodeNumericMismatch, "layer deviation exceeds tolerance",
				map[string]string{
					"layer":     worst.Layer,
					"position":  fmt.Sprintf("%d", worst.Position),
					"max_dev":   fmt.Sprintf("%g", worst.MaxAbsDev),
					"abs_dev":   fmt.Sprintf("%g", worst.MaxAbsDev),
					"tolerance": fmt.Sprintf("%g", e.Pipeline.Tolerance),
				})
		}
	}
	return nil
}

func worstDeviation(r *infer.NumericReport) *infer.LayerDeviation {
	var worst *infer.LayerDeviation
	for i := range r.Deviations {
		d := &r.Deviations[i]
		if worst == nil || d.MaxAbsDev > worst.MaxAbsDev {
			worst = d
		}
	}
	return worst
}
