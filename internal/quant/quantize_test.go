package quant

import (
	"testing"

	"qaudit/internal/model"
)

func TestComputeParamsCoversExtrema(t *testing.T) {
	p, ok := ComputeParams(-2, 4, 8, false)
	if !ok {
		t.Fatal("asymmetric params failed")
	}
	if p.Scale <= 0 {
		t.Fatal("scale must be positive")
	}
	lo, _ := Quantize(p, -2)
	hi, _ := Quantize(p, 4)
	if lo != p.QMin || hi != p.QMax {
		t.Fatalf("extrema must reach domain edges: lo=%d hi=%d", lo, hi)
	}
	sp, ok := ComputeParams(-3, 3, 8, true)
	if !ok || sp.ZeroPoint != 0 {
		t.Fatalf("symmetric params wrong: %+v ok=%v", sp, ok)
	}
}

func TestUnsupportedBitWidthAndZeroChannel(t *testing.T) {
	layer := model.Layer{Name: "L", InDim: 3, OutDim: 2,
		Weights: [][]float64{{1, -1, 0.5}, {0, 0, 0}}, Bias: []float64{0, 0}}
	_, _, err := CalibrateLayer(CalibrationInput{
		Layer: layer, Policy: model.LayerPolicy{BitWidth: 5, Symmetric: true},
		CalibInputs: [][]float64{{1, 2, 3}}, CalibOutputs: [][]float64{{1, 1}},
	})
	if err == nil || model.CodeOf(err) != model.CodeQuantParam {
		t.Fatalf("expected E_QUANT_PARAM for bad bitwidth, got %v", err)
	}
	_, _, err = CalibrateLayer(CalibrationInput{
		Layer: layer, Policy: model.LayerPolicy{BitWidth: 8, Symmetric: false, PerChannel: true},
		CalibInputs: [][]float64{{1, 2, 3}}, CalibOutputs: [][]float64{{1, 1}},
	})
	if err == nil || model.CodeOf(err) != model.CodeQuantParam {
		t.Fatalf("expected E_QUANT_PARAM for zero channel, got %v", err)
	}
}

func TestSymmetricZeroTensorGetsUnitScale(t *testing.T) {
	p, ok := ComputeParams(0, 0, 8, true)
	if !ok || p.Scale != 1 {
		t.Fatalf("zero symmetric tensor must use unit scale, got %+v", p)
	}
}
