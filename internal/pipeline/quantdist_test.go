package pipeline

import (
	"testing"

	"headprune/internal/model"
)

func TestQuantizedZeroSumDistribution(t *testing.T) {
	err := ValidateQuantizedDistribution([]float64{0, 0, 0}, 2, 3)
	assertCode(t, err, model.CodeQuantization)
	be, _ := model.AsBizError(err)
	if be.Layer == nil || *be.Layer != 2 || be.Head == nil || *be.Head != 3 {
		t.Fatal("E_QUANTIZATION must carry the layer and head coordinates")
	}
	if err := ValidateQuantizedDistribution([]float64{0.2, 0.8}, 0, 0); err != nil {
		t.Fatalf("valid distribution rejected: %v", err)
	}
}
