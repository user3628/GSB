package experiment

import (
	"math"
	"testing"

	"regworkbench/internal/config"
	"regworkbench/internal/workerr"
)

func TestBuildValidationSummaryRejectsNaNMetric(t *testing.T) {
	cfg := config.Default("small")
	cfg.KFolds = 2
	cfg.Lambdas = []float64{0.01}
	cfg.L1Ratios = []float64{0.5}
	folds := map[int]*FoldArtifact{
		0: {Fold: 0, Points: []FoldPointArtifact{{SelectedMetric: math.NaN()}}},
		1: {Fold: 1, Points: []FoldPointArtifact{{SelectedMetric: 0.5}}},
	}
	_, err := buildValidationSummary(cfg, folds)
	we, ok := workerr.As(err)
	if !ok || we.Code != workerr.EInvalidFold || we.Fold == nil || *we.Fold != 0 {
		t.Fatalf("err=%v want E_INVALID_FOLD fold=0", err)
	}
}
