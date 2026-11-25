package quant

import (
	"math"
	"testing"

	"imretrace/internal/model"
)

func testVectors(d int) [][]float64 {
	out := make([][]float64, 4)
	for i := range out {
		v := make([]float64, d)
		for j := range v {
			v[j] = math.Sin(float64(i*7+j+1)) * 0.6
		}
		n := math.Sqrt(float64(d) * 0.12)
		for j := range v {
			v[j] /= n
		}
		out[i] = v
	}
	return out
}

func TestQuantDeterminismAndErrorBound(t *testing.T) {
	p := model.DefaultPolicy()
	vectors := testVectors(p.ProjectionDim)
	params, err := Calibrate("text", vectors, p)
	if err != nil {
		t.Fatal(err)
	}
	q1, err := QuantizeVector("item-q", "text", vectors[0], params)
	if err != nil {
		t.Fatal(err)
	}
	q2, err := QuantizeVector("item-q", "text", vectors[0], params)
	if err != nil {
		t.Fatal(err)
	}
	if string(model.MustCanonical(q1)) != string(model.MustCanonical(q2)) {
		t.Fatal("quantized vector is not byte deterministic")
	}
	route, err := DiagnoseRoute("item-q", "query", "text", vectors[0], q1.Dequant, params)
	if err != nil {
		t.Fatal(err)
	}
	if route.VectorL2Actual > route.VectorL2Bound+1e-12 {
		t.Fatalf("actual %.6f exceeds bound %.6f", route.VectorL2Actual, route.VectorL2Bound)
	}
}

func TestQuantInvalidDimensionsZeroScaleAndZeroVector(t *testing.T) {
	p := model.DefaultPolicy()
	m := DeriveMatrix(p)
	bad := [][]float64{{1, 2}, {3, 4}}
	if _, err := Multiply("item-dim", "visual", bad, make([]float64, p.VisualDim), p.ProjectionDim); err == nil {
		t.Fatal("expected matrix dimension error")
	} else if biz, ok := model.AsBiz(err); !ok || biz.Code != model.CodeQuantInvalid || biz.Dim == nil {
		t.Fatalf("unexpected dimension error: %#v", err)
	}

	params, err := Calibrate("text", testVectors(p.ProjectionDim), p)
	if err != nil {
		t.Fatal(err)
	}
	params.Step[3] = 0
	params.Scale[3] = 0
	if _, err := QuantizeVector("item-scale", "text", testVectors(p.ProjectionDim)[0], params); err == nil {
		t.Fatal("expected zero scale error")
	} else if biz, ok := model.AsBiz(err); !ok || biz.ItemID != "item-scale" || biz.Dim == nil || *biz.Dim != 3 {
		t.Fatalf("unexpected scale error: %#v", err)
	}

	good, err := Calibrate("text", testVectors(p.ProjectionDim), p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := QuantizeVector("item-zero", "text", make([]float64, p.ProjectionDim), good); err == nil {
		t.Fatal("expected all-zero quantized vector error")
	} else if biz, ok := model.AsBiz(err); !ok || biz.Code != model.CodeQuantInvalid || biz.ItemID != "item-zero" || biz.Dim == nil {
		t.Fatalf("unexpected zero vector error: %#v", err)
	}
	_ = m
}

func TestFourBitCalibration(t *testing.T) {
	p := model.DefaultPolicy()
	p.QuantBits = 4
	p.QuantGranularity = "vector"
	params, err := Calibrate("visual", testVectors(p.ProjectionDim), p)
	if err != nil {
		t.Fatal(err)
	}
	if params.Levels != 15 || params.ZeroPoint[0] != 8 {
		t.Fatalf("unexpected 4-bit params levels=%d zp=%d", params.Levels, params.ZeroPoint[0])
	}
}
