package experiment

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"regworkbench/internal/config"
	"regworkbench/internal/workerr"
)

func RunIncremental(ctx context.Context, base *Session, newCfg config.Config, newRoot string, hooks Hooks) (*Session, *RunReport, ChangePlan, error) {
	plan, normalized, err := AnalyzeChange(base.Cfg, newCfg, base.Cfg.KFolds)
	if err != nil {
		return nil, nil, plan, err
	}
	next, err := CreateSession(newRoot, normalized, base.DS, hooks)
	if err != nil {
		return nil, nil, plan, err
	}
	next.reuseFolds = base.Snapshot().Folds
	report, err := next.Run(ctx)
	if err != nil {
		_ = next.Close()
		return nil, report, plan, err
	}
	if report != nil {
		report.ChangePlan = &plan
	}
	return next, report, plan, nil
}

func CompareRunDirectories(incrementalRoot, fullRoot string) error {
	incCoef, err := os.ReadFile(filepath.Join(incrementalRoot, "exports", "coefficients.json"))
	if err != nil {
		return err
	}
	fullCoef, err := os.ReadFile(filepath.Join(fullRoot, "exports", "coefficients.json"))
	if err != nil {
		return err
	}
	if !bytes.Equal(incCoef, fullCoef) {
		return coefficientMismatch(incCoef, fullCoef)
	}
	incSummary, err := os.ReadFile(filepath.Join(incrementalRoot, "exports", "validation_summary.json"))
	if err != nil {
		return err
	}
	fullSummary, err := os.ReadFile(filepath.Join(fullRoot, "exports", "validation_summary.json"))
	if err != nil {
		return err
	}
	if !bytes.Equal(incSummary, fullSummary) {
		return summaryMismatch(incSummary, fullSummary)
	}
	return nil
}

func coefficientMismatch(a, b []byte) error {
	var ia, ib ModelArtifact
	if err := json.Unmarshal(a, &ia); err != nil {
		return workerr.New(workerr.EIncrementalMismatch, "incremental coefficient artifact is invalid")
	}
	if err := json.Unmarshal(b, &ib); err != nil {
		return workerr.New(workerr.EIncrementalMismatch, "full coefficient artifact is invalid")
	}
	if ia.Intercept != ib.Intercept {
		e := workerr.New(workerr.EIncrementalMismatch, "intercept differs between incremental and full rebuild")
		e.Field = "intercept"
		return e
	}
	for i := 0; i < len(ia.Coefficients) || i < len(ib.Coefficients); i++ {
		if i >= len(ia.Coefficients) || i >= len(ib.Coefficients) || ia.Coefficients[i] != ib.Coefficients[i] {
			e := workerr.New(workerr.EIncrementalMismatch, "coefficient differs between incremental and full rebuild")
			e.Field = "coefficients[" + uint64String(uint64(i)) + "]"
			return e
		}
	}
	if ia.Digest != ib.Digest {
		e := workerr.New(workerr.EIncrementalMismatch, "coefficient digest differs")
		e.Field = "digest"
		return e
	}
	return workerr.New(workerr.EIncrementalMismatch, "coefficient bytes differ without typed field difference")
}

func summaryMismatch(a, b []byte) error {
	var ia, ib ValidationSummary
	if err := json.Unmarshal(a, &ia); err != nil {
		return workerr.New(workerr.EIncrementalMismatch, "incremental summary is invalid")
	}
	if err := json.Unmarshal(b, &ib); err != nil {
		return workerr.New(workerr.EIncrementalMismatch, "full summary is invalid")
	}
	for c := 0; c < len(ia.Candidates) && c < len(ib.Candidates); c++ {
		ca, cb := ia.Candidates[c], ib.Candidates[c]
		for f := 0; f < len(ca.FoldValues) && f < len(cb.FoldValues); f++ {
			if ca.FoldValues[f] != cb.FoldValues[f] {
				fold := f
				e := workerr.New(workerr.EIncrementalMismatch, "fold validation metric differs")
				e.FirstDiffFold = &fold
				e.Field = "validation_summary.candidates[" + uint64String(uint64(c)) + "].fold_values"
				return e
			}
		}
		if ca.MeanMetric != cb.MeanMetric {
			e := workerr.New(workerr.EIncrementalMismatch, "candidate mean differs")
			e.Field = "validation_summary.candidates[" + uint64String(uint64(c)) + "].mean_metric"
			return e
		}
		if ca.StdMetric != cb.StdMetric {
			e := workerr.New(workerr.EIncrementalMismatch, "candidate std differs")
			e.Field = "validation_summary.candidates[" + uint64String(uint64(c)) + "].std_metric"
			return e
		}
	}
	if ia.Basis.SelectedLambda != ib.Basis.SelectedLambda || ia.Basis.SelectedL1Ratio != ib.Basis.SelectedL1Ratio {
		e := workerr.New(workerr.EIncrementalMismatch, "selected regularization point differs")
		e.Field = "validation_summary.basis.selected"
		return e
	}
	return workerr.New(workerr.EIncrementalMismatch, "validation summary bytes differ")
}
