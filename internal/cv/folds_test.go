package cv

import (
	"math"
	"testing"

	"regworkbench/internal/workerr"
)

func TestStratifiedRejectsFoldsLessThanTwo(t *testing.T) {
	_, err := Stratified([]float64{0, 1}, 1, 1, "sample", "config")
	we, ok := workerr.As(err)
	if !ok || we.Code != workerr.EInvalidFold || we.Fold == nil || *we.Fold != -1 {
		t.Fatalf("error = %v, want E_INVALID_FOLD fold=-1", err)
	}
}

func TestStratifiedRejectsSingleClassFold(t *testing.T) {
	y := make([]float64, 30)
	for i := range y {
		y[i] = 0
	}
	_, err := Stratified(y, 3, 7, "sample", "config")
	we, ok := workerr.As(err)
	if !ok || we.Code != workerr.EInvalidFold || we.Fold == nil {
		t.Fatalf("error = %v, want E_INVALID_FOLD with fold", err)
	}
}

func TestStratifiedDeterministicAndShiftRecorded(t *testing.T) {
	y := make([]float64, 120)
	for i := range y {
		if i%3 == 0 {
			y[i] = 1
		}
	}
	a, err := Stratified(y, 4, 42, "s", "c")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Stratified(y, 4, 42, "s", "c")
	if err != nil {
		t.Fatal(err)
	}
	if a.Fingerprint != b.Fingerprint {
		t.Fatal("split fingerprint differs")
	}
	for _, f := range a.Folds {
		if math.IsNaN(f.DistributionShiftAbs) || f.DistributionShiftAbs < 0 {
			t.Fatal("distribution shift must be recorded")
		}
	}
}
