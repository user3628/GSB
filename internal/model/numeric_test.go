package model

import (
	"math"
	"testing"

	"regworkbench/internal/workerr"
)

func TestFitReturnsNumericStructuredError(t *testing.T) {
	x := [][]float64{{math.NaN()}, {1}, {2}, {3}}
	y := []float64{0, 0, 1, 1}
	_, _, err := Fit(x, y, FitOptions{Fold: 7, Lambda: 0.01, MaxIterations: 5})
	we, ok := workerr.As(err)
	if !ok || we.Code != workerr.ENumeric || we.Fold == nil || *we.Fold != 7 {
		t.Fatalf("err=%v want E_NUMERIC fold=7", err)
	}
}
