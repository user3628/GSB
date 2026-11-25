package experiment_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"regworkbench/internal/canonical"
	"regworkbench/internal/config"
	"regworkbench/internal/dataset"
	"regworkbench/internal/experiment"
	"regworkbench/internal/workerr"
)

func TestRegularizationPathMonotonicAndZeroLabeled(t *testing.T) {
	dataDir := t.TempDir()
	if _, err := dataset.GenerateDirectory(dataDir, dataset.DefaultSeed); err != nil {
		t.Fatal(err)
	}
	ds, err := dataset.LoadDataset(filepath.Join(dataDir, "manifest.json"), "small")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default("small")
	cfg.KFolds = 2
	cfg.Lambdas = []float64{0, 0.001, 0.01}
	cfg.L1Ratios = []float64{0, 0.5, 1}
	cfg.MaxIterations = 35
	root := filepath.Join(t.TempDir(), "session")
	sess, err := experiment.CreateSession(root, cfg, ds, experiment.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	if _, err := sess.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for f := 0; f < cfg.KFolds; f++ {
		art := sess.Snapshot().Folds[f]
		if art == nil || len(art.Points) != 9 {
			t.Fatalf("fold %d has %+v", f, art)
		}
		lastLambda := -1.0
		for _, p := range art.Points {
			if p.Lambda < lastLambda {
				t.Fatalf("path not monotonic at fold %d", f)
			}
			lastLambda = p.Lambda
			if p.Lambda == 0 && !p.Unregularized {
				t.Fatal("lambda=0 must be marked unregularized")
			}
		}
	}
}

func TestCompareRunDirectoriesReportsFirstMismatch(t *testing.T) {
	inc := t.TempDir()
	full := t.TempDir()
	for _, root := range []string{inc, full} {
		if err := os.MkdirAll(filepath.Join(root, "exports"), 0o755); err != nil {
			t.Fatal(err)
		}
		coef := experiment.ModelArtifact{}
		coef.Scope = "final"
		coef.Coefficients = []float64{1, 2, 3}
		coef.Intercept = 0.25
		summary := experiment.ValidationSummary{Metric: "log_loss", KFolds: 2}
		writeArtifact(t, filepath.Join(root, "exports", "coefficients.json"), coef)
		writeArtifact(t, filepath.Join(root, "exports", "validation_summary.json"), summary)
	}
	if err := experiment.CompareRunDirectories(inc, full); err != nil {
		t.Fatalf("identical artifacts should compare equal: %v", err)
	}
	changed := experiment.ModelArtifact{}
	changed.Scope = "final"
	changed.Coefficients = []float64{1, 2.5, 3}
	changed.Intercept = 0.25
	writeArtifact(t, filepath.Join(inc, "exports", "coefficients.json"), changed)
	err := experiment.CompareRunDirectories(inc, full)
	we, ok := workerr.As(err)
	if !ok || we.Code != workerr.EIncrementalMismatch || we.Field != "coefficients[1]" {
		t.Fatalf("err=%v want E_INCREMENTAL_MISMATCH coefficients[1]", err)
	}
}

func writeArtifact(t *testing.T, path string, v any) {
	t.Helper()
	b, err := canonical.JSON(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
