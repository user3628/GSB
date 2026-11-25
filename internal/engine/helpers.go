package engine

import (
	"qaudit/internal/audit"
	"qaudit/internal/calib"
	"qaudit/internal/infer"
	"qaudit/internal/model"
	"qaudit/internal/quant"
)

func calibSelector(ds *model.Dataset, dataDir string, side int) (*calib.CalibSet, error) {
	return calib.Select(ds.Shards, dataDir, ds.Policy.CalibRatio, ds.Policy.Seed, side, side)
}

func rebuildCalibration(ds *model.Dataset, cs *calib.CalibSet, fps model.StageFingerprints) ([]*model.LayerCalibration, []*quant.LayerEngine, error) {
	return infer.CalibrateAll(ds, cs, fps.CalibDigest, fps.PolicyDigest)
}

func auditRun(p *infer.Pipeline, fingerprint string) (*audit.Report, error) {
	return audit.Run(p, fingerprint)
}
