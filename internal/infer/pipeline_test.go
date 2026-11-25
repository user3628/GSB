package infer_test

import (
	"path/filepath"
	"testing"

	"qaudit/internal/calib"
	"qaudit/internal/generator"
	"qaudit/internal/infer"
)

func TestCalibrateAndCompareWithinTolerance(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	ds, err := generator.Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := calib.Select(ds.Shards, dir, ds.Policy.CalibRatio, ds.Policy.Seed,
		generator.ImgWidth, generator.ImgHeight)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, engines, err := infer.CalibrateAll(ds, cs, "in", "pol")
	if err != nil {
		t.Fatalf("calibrate: %v", err)
	}
	if len(artifacts) != len(ds.Network.Layers) || len(engines) != len(ds.Network.Layers) {
		t.Fatal("artifact/engine count mismatch")
	}
	pipe := &infer.Pipeline{Dataset: ds, CalibSet: cs, Engines: engines, Artifacts: artifacts,
		Tolerance: ds.Policy.Tolerance, DataDir: dir}
	report, err := pipe.Compare()
	if err != nil {
		t.Fatalf("compare within tolerance: %v", err)
	}
	if len(report.Traces) != len(ds.Policy.SamplePositions) {
		t.Fatalf("trace count %d", len(report.Traces))
	}
	for _, tr := range report.Traces {
		if tr.PredLabel < 0 || tr.PredLabel >= ds.NumClasses {
			t.Fatalf("illegal prediction %d", tr.PredLabel)
		}
		for name, bits := range tr.AccumBits {
			if bits <= 0 {
				t.Fatalf("layer %s missing accumulator width", name)
			}
		}
	}
}

func TestReferenceForwardAndSoftmax(t *testing.T) {
	ds, err := generator.Generate(filepath.Join(t.TempDir(), "ref"))
	if err != nil {
		t.Fatal(err)
	}
	net := ds.Network
	logits, boundaries := infer.ReferenceForward(net, make([]float64, net.Layers[0].InDim))
	if len(logits) != net.Layers[len(net.Layers)-1].OutDim {
		t.Fatal("logit dimension mismatch")
	}
	if len(boundaries) != len(net.Layers) {
		t.Fatal("boundary count mismatch")
	}
	probs := infer.Softmax(logits)
	sum := 0.0
	for _, p := range probs {
		sum += p
	}
	if sum < 0.999 || sum > 1.001 {
		t.Fatalf("softmax does not normalize: %g", sum)
	}
	if infer.Argmax(probs) < 0 {
		t.Fatal("argmax failed")
	}
}
