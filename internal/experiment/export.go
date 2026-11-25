package experiment

import (
	"os"
	"path/filepath"

	"regworkbench/internal/canonical"
	"regworkbench/internal/model"
)

type TrajectoryExport struct {
	SampleFingerprint string            `json:"sample_fingerprint"`
	ConfigVersion     string            `json:"config_version"`
	Folds             []*FoldArtifact   `json:"folds"`
	FinalTrajectory   []model.Iteration `json:"final_trajectory,omitempty"`
}

func (s *Session) writeExports() error {
	snap := s.Snapshot()
	coefPath, summaryPath, trajectoryPath := s.exportPaths()
	if snap.FinalModel != nil {
		b, err := canonical.JSON(snap.FinalModel)
		if err != nil {
			return err
		}
		if err := canonical.AtomicWriteFile(coefPath, append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	if snap.Validation != nil {
		b, err := canonical.JSON(snap.Validation)
		if err != nil {
			return err
		}
		if err := canonical.AtomicWriteFile(summaryPath, append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	folds := make([]*FoldArtifact, 0, s.Cfg.KFolds)
	for f := 0; f < s.Cfg.KFolds; f++ {
		if art := snap.Folds[f]; art != nil {
			folds = append(folds, art)
		}
	}
	traj := TrajectoryExport{SampleFingerprint: s.DS.RawSHA, ConfigVersion: s.ConfigVer, Folds: folds}
	if snap.FinalModel != nil {
		traj.FinalTrajectory = append([]model.Iteration(nil), snap.FinalModel.Trajectory...)
	}
	b, err := canonical.JSON(traj)
	if err != nil {
		return err
	}
	if err := canonical.AtomicWriteFile(trajectoryPath, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return nil
}

func (s *Session) ExportCoefficients(path string) error {
	model := s.Snapshot().FinalModel
	if model == nil {
		best := s.Snapshot().CurrentBest
		if best == nil {
			return ErrNoProduct("coefficients")
		}
		b, err := canonical.JSON(best)
		if err != nil {
			return err
		}
		return canonical.AtomicWriteFile(path, append(b, '\n'), 0o644)
	}
	b, err := canonical.JSON(model)
	if err != nil {
		return err
	}
	return canonical.AtomicWriteFile(path, append(b, '\n'), 0o644)
}

func (s *Session) ExportTrajectory(path string) error {
	_, _, defaultPath := s.exportPaths()
	if path == "" {
		path = defaultPath
	}
	if path == "" {
		path = filepath.Join(s.Root, "exports", "trajectory.json")
	}
	if err := s.writeExports(); err != nil {
		return err
	}
	b, err := canonical.JSON(TrajectoryExport{})
	_ = b
	_ = err
	return copyFile(defaultPath, path)
}

type noProductError struct{ product string }

func (e noProductError) Error() string { return "no product available: " + e.product }

func ErrNoProduct(product string) error { return noProductError{product: product} }

func copyFile(src, dst string) error {
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return canonical.AtomicWriteFile(dst, b, 0o644)
}
