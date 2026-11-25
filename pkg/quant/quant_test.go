package quant

import (
	"math"
	"reflect"
	"testing"
)

func TestSymmetricPerChannelRoundTrip(t *testing.T) {
	tensor := NewTensor([]int{2, 4}, []float64{
		0, 0, 0, 0,
		-2, -0.5, 0.25, 2,
	})
	cfg := Config{Bits: 4, Mode: Symmetric, Granularity: PerChannel}
	q, err := Quantize(tensor, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Params) != 2 {
		t.Fatalf("got %d channel params", len(q.Params))
	}
	if q.Params[0].Scale != 1 || q.Params[0].ZeroPoint != 0 {
		t.Fatalf("all-zero channel params = %+v", q.Params[0])
	}
	if q.Params[1].Scale != 2.0/7.0 || q.Params[1].ZeroPoint != 0 {
		t.Fatalf("nonzero channel params = %+v", q.Params[1])
	}
	if q.ClampCount != 0 {
		t.Fatalf("calibrated symmetric quantize should not clamp, got %+v", q.Clamps)
	}
	got, err := Dequantize(q)
	if err != nil {
		t.Fatal(err)
	}
	for i := range tensor.Values {
		if math.Abs(got.Values[i]-tensor.Values[i]) > q.Params[i/4].Scale {
			t.Fatalf("position %d roundtrip %.8f != %.8f", i, got.Values[i], tensor.Values[i])
		}
	}
}

func TestAsymmetricClampAudit(t *testing.T) {
	tensor := NewTensor([]int{1, 6}, []float64{-0.4, -0.2, 0, 0.2, 0.4, 1.2})
	cfg := Config{Bits: 8, Mode: Asymmetric, Granularity: PerTensor}
	q, err := QuantizeWithParams(tensor, cfg, []Params{{Scale: 0.005, ZeroPoint: 128}})
	if err != nil {
		t.Fatal(err)
	}
	if q.ClampCount != 1 || len(q.Clamps) != 1 {
		t.Fatalf("got clamp count %d events %+v", q.ClampCount, q.Clamps)
	}
	if q.Clamps[0].Index != 5 || q.Clamps[0].Channel != 0 || q.Clamps[0].Unclamped != 368 {
		t.Fatalf("unexpected clamp event %+v", q.Clamps[0])
	}
	if q.Values[5] != 255 {
		t.Fatalf("clamped integer = %d", q.Values[5])
	}
	deq, err := Dequantize(q)
	if err != nil {
		t.Fatal(err)
	}
	if deq.Values[5] != 0.635 {
		t.Fatalf("dequantized clamped value = %v", deq.Values[5])
	}
}

func TestAllZeroAndExplicitZeroAreDeterministic(t *testing.T) {
	tensor := NewTensor([]int{2, 3}, []float64{0, 0, 0, 0, 0, 0})
	for _, mode := range []Mode{Symmetric, Asymmetric} {
		for _, bits := range []int{4, 8} {
			cfg := Config{Bits: bits, Mode: mode, Granularity: PerChannel}
			a, err := Quantize(tensor, cfg)
			if err != nil {
				t.Fatal(err)
			}
			b, err := Quantize(tensor, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(a, b) {
				t.Fatalf("%s/%d not deterministic", mode, bits)
			}
			if a.ClampCount != 0 {
				t.Fatalf("all-zero tensor clamped: %+v", a.Clamps)
			}
			deq, err := Dequantize(a)
			if err != nil {
				t.Fatal(err)
			}
			for _, v := range deq.Values {
				if v != 0 {
					t.Fatalf("all-zero dequantized to %v", v)
				}
			}
		}
	}
}

func TestLinearZeroPointAndScaleChain(t *testing.T) {
	a := NewTensor([]int{1, 2}, []float64{-1, 2})
	w := NewTensor([]int{1, 2}, []float64{-3, 4})
	cfgA := Config{Bits: 8, Mode: Asymmetric, Granularity: PerTensor}
	cfgW := Config{Bits: 8, Mode: Asymmetric, Granularity: PerTensor}
	res, err := Linear(a, w, cfgA, cfgW)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DequantizeLinear(res)
	if err != nil {
		t.Fatal(err)
	}
	if out.Shape[0] != 1 || out.Shape[1] != 1 {
		t.Fatalf("shape = %v", out.Shape)
	}
	if got := out.Values[0]; math.Abs(got-11) > 0.8 {
		t.Fatalf("zero-point corrected result = %v, want close to 11", got)
	}
	// A naive accumulator without zero-point correction would be far away.
	if res.AZero[0] == 0 || res.WZero[0] == 0 {
		t.Fatalf("expected nonzero asymmetric zero points, got a=%d w=%d", res.AZero[0], res.WZero[0])
	}
}

func TestSanitizeNonFinite(t *testing.T) {
	tensor := NewTensor([]int{1, 4}, []float64{math.NaN(), math.Inf(1), math.Inf(-1), 2})
	got, events := Sanitize(tensor)
	want := []float64{0, 0, 0, 2}
	if !reflect.DeepEqual(got.Values, want) {
		t.Fatalf("sanitized = %v", got.Values)
	}
	if len(events) != 3 || events[0].Kind != "nan" || events[1].Kind != "+inf" || events[2].Kind != "-inf" {
		t.Fatalf("events = %+v", events)
	}
}

func TestOutlierResultIsRepeatable(t *testing.T) {
	a := NewTensor([]int{2, 3}, []float64{0, 1, 2, -12, 0, 0.5})
	w := NewTensor([]int{2, 3}, []float64{1, 0, -1, 8, 0.25, 0})
	cfg := Config{Bits: 4, Mode: Symmetric, Granularity: PerChannel}
	r1, err := Linear(a, w, cfg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := Linear(a, w, cfg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r1, r2) {
		t.Fatal("outlier linear result not deterministic")
	}
}

func TestAsymmetricAllPositiveCalibrationAvoidsClipping(t *testing.T) {
	tensor := NewTensor([]int{1, 2}, []float64{1, 2})
	cfg := Config{Bits: 8, Mode: Asymmetric, Granularity: PerTensor}
	params, err := Calibrate(tensor, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if params[0].ZeroPoint != 0 || math.Abs(params[0].Scale-2.0/255.0) > 1e-15 {
		t.Fatalf("params = %+v", params[0])
	}
	q, err := QuantizeWithParams(tensor, cfg, params)
	if err != nil {
		t.Fatal(err)
	}
	if q.ClampCount != 0 {
		t.Fatalf("all-positive calibrated range clipped: %+v", q.Clamps)
	}
}
