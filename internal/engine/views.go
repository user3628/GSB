package engine

import (
	"os"
	"path/filepath"

	"qaudit/internal/audit"
	"qaudit/internal/infer"
	"qaudit/internal/model"
)

// Snapshot is the externally visible pipeline state.
type Snapshot struct {
	Version       int64                     `json:"version"`
	Fingerprints  model.StageFingerprints   `json:"fingerprints"`
	CalibEntries  int                       `json:"calib_entries"`
	CalibManifest string                    `json:"calib_manifest_digest"`
	Artifacts     []*model.LayerCalibration `json:"artifacts"`
	Numeric       *infer.NumericReport      `json:"numeric,omitempty"`
	Audit         *audit.Report             `json:"audit,omitempty"`
	Invalidations []Invalidation            `json:"invalidations,omitempty"`
}

// Snapshot returns a point-in-time view of the workbench state.
func (e *Engine) Snapshot() Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	md := ""
	if e.CalibSet != nil {
		n = len(e.CalibSet.Entries)
		md = e.CalibSet.ManifestDigest
	}
	return Snapshot{
		Version:       e.Version,
		Fingerprints:  e.Fingerprints,
		CalibEntries:  n,
		CalibManifest: md,
		Artifacts:     e.Artifacts,
		Numeric:       e.Numeric,
		Audit:         e.Audit,
		Invalidations: e.InvalidationLog,
	}
}

// FingerprintsView returns the current stage digests.
func (e *Engine) FingerprintsView() model.StageFingerprints {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.Fingerprints
}

// AuditView returns the latest audit report plus numeric comparison.
func (e *Engine) AuditView() map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	return map[string]any{
		"version":     e.Version,
		"fingerprint": e.Fingerprints.VersionFingerprint,
		"numeric":     e.Numeric,
		"audit":       e.Audit,
	}
}

// Versions returns recorded version classifications.
func (e *Engine) Versions() ([]storeVersion, error) {
	rows, err := e.store.ListVersions()
	if err != nil {
		return nil, err
	}
	out := make([]storeVersion, 0, len(rows))
	for _, r := range rows {
		out = append(out, storeVersion(r))
	}
	return out, nil
}

type storeVersion = struct {
	Version     int64  `json:"version"`
	Fingerprint string `json:"fingerprint"`
	Kind        string `json:"kind"`
}

// CloneDataset deep-copies the loaded dataset for fault-injection runs that must
// not corrupt the deterministic baseline. A manual float64-preserving copy is
// used because encoding/json cannot represent injected NaN values.
func (e *Engine) CloneDataset() (*model.Dataset, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Dataset == nil {
		return nil, model.NewBizError(model.CodeState, "dataset not loaded", nil)
	}
	return deepCopyDataset(e.Dataset), nil
}

// RunWith executes the pipeline on an externally supplied dataset clone. The
// clone is kept in memory only (it may contain NaN values used by the fault
// injection chain) and never replaces the on-disk deterministic baseline.
func (e *Engine) RunWith(ds *model.Dataset) error {
	e.mu.Lock()
	e.Dataset = ds
	e.mu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
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

// ExportReport writes the deterministic audit report as canonical JSON and
// returns its path and SHA-256 digest.
func (e *Engine) ExportReport() (map[string]any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Audit == nil {
		return nil, model.NewBizError(model.CodeState, "audit report not available", nil)
	}
	rep := map[string]any{
		"version":       e.Version,
		"fingerprints":  e.Fingerprints,
		"numeric":       e.Numeric,
		"audit":         e.Audit,
		"artifacts":     e.Artifacts,
		"invalidations": e.InvalidationLog,
	}
	canonical, err := model.CanonicalJSON(rep)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(e.dataDir, "audit_report.json")
	if err := os.WriteFile(path, canonical, 0o644); err != nil {
		return nil, err
	}
	return map[string]any{
		"path":   path,
		"digest": model.DigestBytes(canonical),
		"report": rep,
	}, nil
}
