package engine

import (
	"fmt"

	"qaudit/internal/infer"
	"qaudit/internal/model"
	"qaudit/internal/store"
)

// classifyVersion distinguishes normal progression, full replacement and rollback
// using the recorded (version -> fingerprint) history.
//
//   - no history, or same fingerprint at the latest version: normal
//   - fingerprint seen at an older version: rollback (rejected by caller)
//   - brand-new fingerprint at a later content set: replacement, new monotone version
func (e *Engine) classifyVersion(latest int64, fingerprint string) (kind string, target int64, err error) {
	versions, err := e.store.ListVersions()
	if err != nil {
		return "", 0, err
	}
	if len(versions) == 0 {
		v := int64(1)
		if err := e.store.UpsertVersion(v, fingerprint, KindNormal); err != nil {
			return "", 0, err
		}
		return KindNormal, v, nil
	}
	for _, row := range versions {
		if row.Fingerprint == fingerprint {
			if row.Version == latest {
				return KindNormal, latest, nil
			}
			return KindRollback, row.Version, nil
		}
	}
	target = latest + 1
	return KindReplacement, target, nil
}

// propagateInvalidation lists every layer whose upstream inputs changed. The
// earliest layer whose input/policy fingerprint changed invalidates itself and
// all downstream layers; unaffected upstream layers remain reusable.
func (e *Engine) propagateInvalidation(oldVersion, newVersion int64) []Invalidation {
	log := []Invalidation{}
	oldRows, err := e.store.ListSnapshots(oldVersion)
	if err != nil {
		return log
	}
	oldByLayer := map[string]store.SnapshotRow{}
	for _, r := range oldRows {
		oldByLayer[r.LayerName] = r
	}
	// Any fingerprint change is a global content replacement for the new version;
	// the diagnostic still enumerates each layer with the concrete reason.
	for _, layer := range e.Dataset.Network.Layers {
		reason := "content replacement: version fingerprint changed"
		if prior, ok := oldByLayer[layer.Name]; ok {
			if prior.InputFingerprint != e.Fingerprints.CalibDigest {
				reason = "calibration set changed"
			}
			if prior.PolicyFingerprint != e.Fingerprints.PolicyDigest {
				reason = "quantization policy changed"
			}
			if e.Fingerprints.WeightsDigest != "" {
				reason = reason + "; upstream version " + fmt.Sprintf("%d", oldVersion) + " superseded"
			}
		}
		log = append(log, Invalidation{Layer: layer.Name, Reason: reason})
	}
	return log
}

// Recover rebuilds in-memory progress solely from committed database facts after
// a forced termination. It re-runs calibration derivation, verifies every
// stored artifact digest and reconstructs engines and downstream reports.
func (e *Engine) Recover() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Dataset == nil {
		return model.NewBizError(model.CodeState, "dataset not loaded", nil)
	}
	state, err := e.store.LoadState()
	if err != nil {
		return err
	}
	if state == nil {
		return model.NewBizError(model.CodeState, "no committed state to recover", nil)
	}
	side := e.Dataset.InputDimRoot()
	cs, err := calibSelector(e.Dataset, e.dataDir, side)
	if err != nil {
		return err
	}
	e.CalibSet = cs
	fps, err := ComputeFingerprints(e.Dataset.Network, cs, e.Dataset.Policy)
	if err != nil {
		return err
	}
	e.Fingerprints = fps
	if fps.VersionFingerprint != state.Fingerprint {
		return model.NewBizError(model.CodeStaleVersion, "recovered fingerprint differs from committed state",
			map[string]string{
				"committed": state.Fingerprint,
				"derived":   fps.VersionFingerprint,
			})
	}
	e.Version = state.Version
	rows, err := e.store.ListSnapshots(state.Version)
	if err != nil {
		return err
	}
	artifacts, engines, err := rebuildCalibration(e.Dataset, cs, fps)
	if err != nil {
		return err
	}
	done := map[string]store.SnapshotRow{}
	for _, row := range rows {
		done[row.LayerName] = row
	}
	publishedCount := 0
	for idx, art := range artifacts {
		if row, ok := done[art.LayerName]; ok {
			if row.ArtifactDigest != art.ArtifactDigest {
				return model.NewBizError(model.CodeStaleVersion, "artifact digest mismatch during recovery",
					map[string]string{"layer": art.LayerName})
			}
			publishedCount++
			continue
		}
		// Resume the interrupted calibration by publishing the remaining layers,
		// preserving the monotonic sequence of the original version.
		if err := e.guard.Publish(store.PublishRequest{
			Version:     state.Version,
			ExpectedSeq: int64(publishedCount + 1),
			LayerName:   art.LayerName,
			InputFP:     fps.CalibDigest,
			PolicyFP:    fps.PolicyDigest,
			Digest:      art.ArtifactDigest,
			Artifact:    art,
		}); err != nil {
			return err
		}
		publishedCount++
		_ = idx
	}
	for _, art := range artifacts {
		stored, err := e.store.LoadSnapshot(state.Version, art.LayerName)
		if err != nil {
			return err
		}
		if stored == nil {
			return model.NewBizError(model.CodeStaleVersion, "artifact digest mismatch during recovery",
				map[string]string{"layer": art.LayerName})
		}
	}
	e.Artifacts = artifacts
	e.Pipeline = &infer.Pipeline{
		Dataset:   e.Dataset,
		CalibSet:  cs,
		Engines:   engines,
		Artifacts: artifacts,
		Tolerance: e.Dataset.Policy.Tolerance,
		DataDir:   e.dataDir,
	}
	// Reconstruct every downstream stage deterministically from the recovered
	// calibration, regardless of where the interrupted process stopped.
	report, err := e.Pipeline.Compare()
	if err != nil && model.CodeOf(err) != model.CodeNumericMismatch {
		return err
	}
	e.Numeric = report
	ar, err := auditRun(e.Pipeline, fps.VersionFingerprint)
	if err != nil {
		return err
	}
	e.Audit = ar
	if err := e.store.SaveState(state.Version, fps.VersionFingerprint, StageAudited); err != nil {
		return err
	}
	return nil
}

// LayerStatuses returns the per-layer calibration view for external inspection.
func (e *Engine) LayerStatuses() ([]model.LayerStatus, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Dataset == nil {
		return nil, model.NewBizError(model.CodeState, "dataset not loaded", nil)
	}
	rows := map[string]store.SnapshotRow{}
	if e.Version > 0 {
		listed, err := e.store.ListSnapshots(e.Version)
		if err != nil {
			return nil, err
		}
		for _, r := range listed {
			rows[r.LayerName] = r
		}
	}
	invalid := map[string]string{}
	for _, iv := range e.InvalidationLog {
		invalid[iv.Layer] = iv.Reason
	}
	out := make([]model.LayerStatus, 0, len(e.Dataset.Network.Layers))
	for _, layer := range e.Dataset.Network.Layers {
		pol := e.Dataset.Policy.LayerPolicy(layer.Name)
		st := model.LayerStatus{
			Layer:      layer.Name,
			BitWidth:   pol.BitWidth,
			Symmetric:  pol.Symmetric,
			PerChannel: pol.PerChannel,
		}
		if row, ok := rows[layer.Name]; ok {
			st.Calibrated = true
			st.Version = row.Version
			st.Seq = row.Seq
			st.ArtifactDigest = row.ArtifactDigest
		}
		if reason, ok := invalid[layer.Name]; ok {
			st.Invalidated = true
			st.Reason = reason
		}
		out = append(out, st)
	}
	return out, nil
}
