package model

import (
	"testing"

	"regworkbench/internal/canonical"
)

func TestFitSamePointIsByteDeterministic(t *testing.T) {
	x := [][]float64{{-2}, {-1}, {0.1}, {1}, {2}, {3}}
	y := []float64{0, 0, 0, 1, 1, 1}
	opts := FitOptions{Fold: 0, Lambda: 0.001, L1Ratio: 0.5, MaxIterations: 60, Tolerance: 1e-9}
	a, _, err := Fit(x, y, opts)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := Fit(x, y, opts)
	if err != nil {
		t.Fatal(err)
	}
	ba, err := canonical.JSON(a)
	if err != nil {
		t.Fatal(err)
	}
	bb, err := canonical.JSON(b)
	if err != nil {
		t.Fatal(err)
	}
	if string(ba) != string(bb) || a.CoefficientDigest != b.CoefficientDigest {
		t.Fatal("same regularization point produced different bytes")
	}
	if len(a.Trajectory) == 0 || a.Trajectory[0].Iteration != 1 {
		t.Fatal("trajectory must record iterations")
	}
}

func TestAUCNaNForSingleClass(t *testing.T) {
	if v := AUC([]float64{0, 0}, []float64{0.1, 0.9}); v == v {
		t.Fatalf("AUC = %v, want NaN for one class", v)
	}
}
