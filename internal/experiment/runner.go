package experiment

import (
	"context"
	"fmt"
	"math"
	"path/filepath"

	"regworkbench/internal/canonical"
	"regworkbench/internal/checkpoint"
	"regworkbench/internal/cv"
	"regworkbench/internal/model"
	"regworkbench/internal/workerr"
)

type RunReport struct {
	State              string      `json:"state"`
	SessionRoot        string      `json:"session_root"`
	MonotonicSequence  int64       `json:"monotonic_sequence"`
	ReusedCheckpoints  []string    `json:"reused_checkpoints,omitempty"`
	ReusedFolds        []int       `json:"reused_folds,omitempty"`
	ReusedFoldPoints   int         `json:"reused_fold_points"`
	ComputedFoldPoints int         `json:"computed_fold_points"`
	ChangePlan         *ChangePlan `json:"change_plan,omitempty"`
	SelectedLambda     float64     `json:"selected_lambda,omitempty"`
	SelectedL1Ratio    float64     `json:"selected_l1_ratio,omitempty"`
}

type pointKey struct {
	Lambda  float64
	L1Ratio float64
}

func (s *Session) Run(ctx context.Context) (*RunReport, error) {
	if !s.running.CompareAndSwap(false, true) {
		return nil, workerr.New(workerr.EConflict, "session training is already running")
	}
	defer s.running.Store(false)

	report := &RunReport{SessionRoot: s.Root}
	s.update(func(n *Snapshot) {
		n.State = "running"
		n.LastError = nil
	})

	records, err := s.Store.Records()
	if err != nil {
		return s.fail(err)
	}
	if !hasStage(records, "preprocess") {
		payload := struct {
			Columns      []interface{} `json:"columns"`
			FeatureNames []string      `json:"feature_names"`
			Fingerprint  string        `json:"fingerprint"`
		}{Columns: []interface{}{}, FeatureNames: s.Prepared.FeatureNames, Fingerprint: s.Snapshot().Fingerprints["preprocess"]}
		rec, err := s.publish(ctx, "preprocess", -1, s.DS.RawSHA, noCoefficientDigest(), payload)
		if err != nil {
			return s.fail(err)
		}
		report.MonotonicSequence = rec.Seq
	} else {
		report.ReusedCheckpoints = append(report.ReusedCheckpoints, "preprocess")
	}

	records, _ = s.Store.Records()
	if !hasStage(records, "split") {
		rec, err := s.publish(ctx, "split", -1, s.DS.RawSHA, noCoefficientDigest(), s.Split)
		if err != nil {
			return s.fail(err)
		}
		report.MonotonicSequence = rec.Seq
	} else {
		report.ReusedCheckpoints = append(report.ReusedCheckpoints, "split")
	}

	for f := 0; f < s.Cfg.KFolds; f++ {
		if existing := s.Snapshot().Folds[f]; existing != nil && len(existing.Points) > 0 {
			report.ReusedFolds = append(report.ReusedFolds, f)
			report.ReusedFoldPoints += len(existing.Points)
			continue
		}
		if s.Store.StopRequested() {
			return s.stopped(report)
		}
		art, reusedPoints, computedPoints, stopped, err := s.computeFold(ctx, f)
		if err != nil {
			return s.fail(err)
		}
		report.ReusedFoldPoints += reusedPoints
		report.ComputedFoldPoints += computedPoints
		if stopped {
			return s.stopped(report)
		}
		rec, err := s.publish(ctx, "fold", f, s.DS.RawSHA, art.Digest, art)
		if err != nil {
			return s.fail(err)
		}
		report.MonotonicSequence = rec.Seq
		report.ReusedFolds = append(report.ReusedFolds, f)
		s.update(func(n *Snapshot) {
			n.CompletedFolds = appendUniqueInt(n.CompletedFolds, f)
			n.MonotonicSequence = rec.Seq
			n.CurrentBest = s.recomputeCurrentBestInto(n.Folds)
		})
	}

	records, _ = s.Store.Records()
	var summary *ValidationSummary
	if existing := s.Snapshot().Validation; existing != nil && hasStage(records, "selection") {
		summary = existing
		report.ReusedCheckpoints = append(report.ReusedCheckpoints, "selection")
	} else {
		var err error
		summary, err = buildValidationSummary(s.Cfg, s.Snapshot().Folds)
		if err != nil {
			return s.fail(err)
		}
		if s.Store.StopRequested() {
			return s.stopped(report)
		}
		rec, err := s.publish(ctx, "selection", -1, s.DS.RawSHA, noCoefficientDigest(), summary)
		if err != nil {
			return s.fail(err)
		}
		report.MonotonicSequence = rec.Seq
		s.update(func(n *Snapshot) { n.Validation = summary; n.MonotonicSequence = rec.Seq })
	}
	report.SelectedLambda = summary.Basis.SelectedLambda
	report.SelectedL1Ratio = summary.Basis.SelectedL1Ratio

	if existing := s.Snapshot().FinalModel; existing != nil && hasStage(records, "final") {
		report.ReusedCheckpoints = append(report.ReusedCheckpoints, "final")
		report.State = "completed"
		if err := s.writeExports(); err != nil {
			return s.fail(err)
		}
		s.update(func(n *Snapshot) { n.State = "completed"; n.FinalModel = existing })
		return report, nil
	}
	if s.Store.StopRequested() {
		return s.stopped(report)
	}
	finalArt, stopped, err := s.fitFinal(summary.Selected.Lambda, summary.Selected.L1Ratio)
	if err != nil {
		return s.fail(err)
	}
	if stopped {
		return s.stopped(report)
	}
	rec, err := s.publish(ctx, "final", -1, s.DS.RawSHA, finalArt.Digest, finalArt)
	if err != nil {
		return s.fail(err)
	}
	report.MonotonicSequence = rec.Seq
	s.update(func(n *Snapshot) {
		n.FinalModel = finalArt
		n.State = "completed"
		n.MonotonicSequence = rec.Seq
		n.CurrentBest = &finalArt.CoefficientsArtifact
	})
	if err := s.writeExports(); err != nil {
		return s.fail(err)
	}
	report.State = "completed"
	return report, nil
}

func (s *Session) computeFold(ctx context.Context, f int) (*FoldArtifact, int, int, bool, error) {
	fold := s.Split.Folds[f]
	bundle := model.BuildBundle(s.Prepared, fold.Train, fold.Validation, s.Cfg.TargetSmoothing)
	bundleFP, _, err := canonical.SHA256JSON(struct {
		Names []string    `json:"names"`
		Train [][]float64 `json:"train_x"`
		TY    []float64   `json:"train_y"`
		Eval  [][]float64 `json:"eval_x"`
		EY    []float64   `json:"eval_y"`
	}{bundle.Names, bundle.TrainX, bundle.TrainY, bundle.EvalX, bundle.EvalY})
	if err != nil {
		return nil, 0, 0, false, err
	}
	solverFP, _, err := canonical.SHA256JSON(struct {
		MaxIter int     `json:"max_iterations"`
		Tol     float64 `json:"tolerance"`
		Smooth  float64 `json:"target_smoothing"`
	}{s.Cfg.MaxIterations, s.Cfg.Tolerance, s.Cfg.TargetSmoothing})
	if err != nil {
		return nil, 0, 0, false, err
	}
	art := &FoldArtifact{Fold: f, FoldFingerprint: fold.Fingerprint, BundleFingerprint: bundleFP, SolverFingerprint: solverFP}
	old := s.findReusableFold(f, fold.Fingerprint, bundleFP, solverFP)
	oldPoints := map[pointKey]FoldPointArtifact{}
	if old != nil {
		for _, p := range old.Points {
			oldPoints[pointKey{p.Lambda, p.L1Ratio}] = p
		}
	}
	reused, computed := 0, 0
	s.update(func(n *Snapshot) {
		n.State = "training"
		n.Stage = "fold_model"
		n.CurrentFold = f
		n.Folds[f] = art
	})

	for _, lambda := range s.Cfg.Lambdas {
		for _, ratio := range s.Cfg.L1Ratios {
			if ctx.Err() != nil {
				return art, reused, computed, false, ctx.Err()
			}
			key := pointKey{lambda, ratio}
			cacheKey := cv.CacheKey{SampleFingerprint: s.DS.RawSHA, ConfigVersion: s.ConfigVer, Fold: f, Kind: "fold_point", Point: pointID(lambda, ratio)}
			if oldPoint, ok := oldPoints[key]; ok {
				oldPoint.CacheKey = cacheKey.String()
				art.Points = append(art.Points, oldPoint)
				reused++
				s.afterPoint(f, art, oldPoint)
				continue
			}
			if s.Store.StopRequested() {
				return art, reused, computed, true, nil
			}
			s.update(func(n *Snapshot) {
				n.CurrentLambda = lambda
				n.CurrentL1Ratio = ratio
				n.CurrentIteration = 0
				n.Live = &LiveProgress{Fold: f, Lambda: lambda, L1Ratio: ratio}
			})
			fitRes, stopped, err := model.Fit(bundle.TrainX, bundle.TrainY, model.FitOptions{
				Fold: f, Lambda: lambda, L1Ratio: ratio,
				MaxIterations: s.Cfg.MaxIterations, Tolerance: s.Cfg.Tolerance,
				StopAfterRound: func(round int) bool { return s.Store.StopRequested() },
				OnRound: func(it model.Iteration, traj []model.Iteration) {
					s.update(func(n *Snapshot) {
						n.CurrentIteration = it.Iteration
						if n.Live == nil {
							n.Live = &LiveProgress{Fold: f, Lambda: lambda, L1Ratio: ratio}
						}
						n.Live.Iteration = it.Iteration
						n.Live.Trajectory = append([]model.Iteration(nil), traj...)
					})
					if s.Hooks.Round != nil {
						s.Hooks.Round(f, lambda, ratio, it.Iteration)
					}
				},
			})
			if err != nil {
				return nil, reused, computed, false, err
			}
			if stopped {
				return art, reused, computed, true, nil
			}
			pred := model.Predict(bundle.EvalX, fitRes.Coefficients, fitRes.Intercept)
			metrics := model.Evaluate(bundle.EvalY, pred)
			selectedMetric := model.MetricValue(metrics, s.Cfg.Metric)
			if math.IsNaN(selectedMetric) || math.IsInf(selectedMetric, 0) {
				return nil, reused, computed, false, workerr.InvalidFold(f, "metric_nan_or_infinite:"+s.Cfg.Metric)
			}
			pointArt := FoldPointArtifact{
				FitResult: *fitRes, Metrics: metrics, SelectedMetric: selectedMetric,
				BundleFingerprint: bundleFP, CacheKey: cacheKey.String(),
			}
			art.Points = append(art.Points, pointArt)
			computed++
			s.afterPoint(f, art, pointArt)
		}
	}
	digest, _, err := canonical.SHA256JSON(art)
	if err != nil {
		return nil, reused, computed, false, err
	}
	art.Digest = digest
	s.update(func(n *Snapshot) {
		n.Folds[f] = art
		n.Live = nil
		n.CurrentBest = s.recomputeCurrentBestInto(n.Folds)
	})
	return art, reused, computed, false, nil
}

func (s *Session) afterPoint(f int, art *FoldArtifact, point FoldPointArtifact) {
	artCopy := *art
	artCopy.Points = append([]FoldPointArtifact(nil), art.Points...)
	digest, _, _ := canonical.SHA256JSON(artCopy)
	artCopy.Digest = digest
	s.update(func(n *Snapshot) {
		n.Folds[f] = &artCopy
		n.CurrentBest = s.recomputeCurrentBestInto(n.Folds)
	})
}

func (s *Session) findReusableFold(f int, foldFP, bundleFP, solverFP string) *FoldArtifact {
	if s.reuseFolds != nil {
		if art, ok := s.reuseFolds[f]; ok && art.FoldFingerprint == foldFP && art.BundleFingerprint == bundleFP && art.SolverFingerprint == solverFP {
			return art
		}
		for _, art := range s.reuseFolds {
			if art.FoldFingerprint == foldFP && art.BundleFingerprint == bundleFP && art.SolverFingerprint == solverFP {
				return art
			}
		}
	}
	old := s.Snapshot().Folds[f]
	if old != nil && old.FoldFingerprint == foldFP && old.BundleFingerprint == bundleFP && old.SolverFingerprint == solverFP {
		return old
	}
	return nil
}

func (s *Session) fitFinal(lambda, ratio float64) (*ModelArtifact, bool, error) {
	bundle := model.BuildFullBundle(s.Prepared, s.Cfg.TargetSmoothing)
	fitRes, stopped, err := model.Fit(bundle.TrainX, bundle.TrainY, model.FitOptions{
		Fold: -1, Lambda: lambda, L1Ratio: ratio,
		MaxIterations: s.Cfg.MaxIterations, Tolerance: s.Cfg.Tolerance,
		StopAfterRound: func(int) bool { return s.Store.StopRequested() },
		OnRound: func(it model.Iteration, traj []model.Iteration) {
			s.update(func(n *Snapshot) {
				n.Stage = "final_model"
				n.CurrentFold = -1
				n.CurrentIteration = it.Iteration
				n.Live = &LiveProgress{Fold: -1, Lambda: lambda, L1Ratio: ratio, Iteration: it.Iteration, Trajectory: append([]model.Iteration(nil), traj...)}
			})
		},
	})
	if err != nil {
		return nil, false, err
	}
	if stopped {
		return nil, true, nil
	}
	coef := CoefficientsArtifact{
		Scope: "final", Lambda: lambda, L1Ratio: ratio, Unregularized: lambda == 0,
		FeatureNames: append([]string(nil), s.Prepared.FeatureNames...),
		Coefficients: fitRes.Coefficients, Intercept: fitRes.Intercept,
		Digest: fitRes.CoefficientDigest, SampleFingerprint: s.DS.RawSHA, ConfigVersion: s.ConfigVer,
	}
	return &ModelArtifact{CoefficientsArtifact: coef, TrainingRows: s.Prepared.RowCount}, false, nil
}

func (s *Session) publish(ctx context.Context, stage string, fold int, inputFP, coefDigest string, payload any) (*checkpoint.Record, error) {
	raw, err := canonical.JSON(payload)
	if err != nil {
		return nil, err
	}
	records, err := s.Store.Records()
	if err != nil {
		return nil, err
	}
	last := int64(0)
	if len(records) > 0 {
		last = records[len(records)-1].Seq
	}
	rec := checkpoint.Record{
		Seq: last + 1, Stage: stage, Fold: fold, InputFingerprint: inputFP,
		ConfigVersion: s.ConfigVer, CoefficientDigest: coefDigest, Payload: raw,
	}
	out, err := s.Store.Publish(ctx, rec)
	if err != nil {
		return nil, err
	}
	if s.Hooks.AfterCheckpoint != nil {
		s.Hooks.AfterCheckpoint(stage, fold, out.Seq)
	}
	return out, nil
}

func (s *Session) stopped(report *RunReport) (*RunReport, error) {
	report.State = "stopped"
	s.update(func(n *Snapshot) { n.State = "stopped"; n.Live = nil })
	return report, nil
}

func (s *Session) fail(err error) (*RunReport, error) {
	we := workerr.Wrap(workerr.EConflict, err)
	s.update(func(n *Snapshot) {
		n.State = "error"
		n.LastError = we
	})
	return nil, err
}

func appendUniqueInt(xs []int, v int) []int {
	for _, x := range xs {
		if x == v {
			return xs
		}
	}
	return append(xs, v)
}

func pointID(lambda, ratio float64) string {
	return fmt.Sprintf("lambda=%.12g,l1_ratio=%.12g", lambda, ratio)
}

func (s *Session) exportPaths() (coef, summary, trajectory string) {
	dir := filepath.Join(s.Root, "exports")
	return filepath.Join(dir, "coefficients.json"), filepath.Join(dir, "validation_summary.json"), filepath.Join(dir, "trajectory.json")
}
