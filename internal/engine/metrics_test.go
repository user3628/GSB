package engine

import (
	"testing"

	"ocrlab/internal/config"
	"ocrlab/internal/model"
)

func TestZeroDenominatorsAreExplicitlyNotApplicable(t *testing.T) {
	cfg := config.Default()
	diagnostic := ImageDiagnostic(cfg, model.ImageSpec{ImageID: "empty-001", Group: "small"},
		model.DetectionProduct{}, model.RecognitionProduct{}, model.StrategyProduct{},
		ProductDigests{Detection: "d", Recognition: "r", Strategy: "s", Input: "i", ConfigVersion: "c"})
	if diagnostic.Detection.Recall.Applicable || diagnostic.Detection.Recall.Value != nil {
		t.Fatal("empty truth set must produce an N/A detection recall")
	}
	if diagnostic.Recognition.Accuracy.Applicable || diagnostic.Recognition.AverageMatchDistance.Applicable {
		t.Fatal("empty candidate/distance sets must produce N/A metrics")
	}
	if diagnostic.NotApplicableCount != 4 || diagnostic.Recognition.NotApplicableCount != 4 {
		t.Fatalf("expected four explicit N/A counters, got %+v", diagnostic)
	}
}

func TestBoxValidationRejectsOutOfBoundsAndZeroSize(t *testing.T) {
	cases := []model.Rect{
		{X: -0.01, Y: 0.1, W: 0.2, H: 0.2},
		{X: 0.9, Y: 0.1, W: 0.2, H: 0.2},
		{X: 0.1, Y: 0.1, W: 0, H: 0.2},
		{X: 0.1, Y: 0.1, W: 0.2, H: 0},
	}
	for _, rect := range cases {
		if reason := ValidateNormalizedBox(rect); reason == "" {
			t.Fatalf("expected invalid box rejection for %+v", rect)
		}
	}
}
