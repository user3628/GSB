package calib_test

import (
	"path/filepath"
	"testing"

	"qaudit/internal/calib"
	"qaudit/internal/generator"
	"qaudit/internal/model"
)

func TestSelectDeterministicAndStable(t *testing.T) {
	dir := t.TempDir()
	ds, err := generator.Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	a, err := calib.Select(ds.Shards, dir, ds.Policy.CalibRatio, ds.Policy.Seed, generator.ImgWidth, generator.ImgHeight)
	if err != nil {
		t.Fatal(err)
	}
	b, err := calib.Select(ds.Shards, dir, ds.Policy.CalibRatio, ds.Policy.Seed, generator.ImgWidth, generator.ImgHeight)
	if err != nil {
		t.Fatal(err)
	}
	if a.ManifestDigest != b.ManifestDigest || a.FeatureDigest != b.FeatureDigest {
		t.Fatal("calibration selection must be byte-identical for identical input")
	}
	want := int(ds.Policy.CalibRatio * float64(len(ds.Shards)))
	if len(a.Entries) != want {
		t.Fatalf("want %d entries, got %d", want, len(a.Entries))
	}
	// Stratification: every label represented.
	seen := map[int]bool{}
	for _, e := range a.Entries {
		seen[e.Label] = true
	}
	if len(seen) != ds.NumClasses {
		t.Fatalf("stratified sample must cover all classes, got %v", seen)
	}
}

func TestSelectRejectsBadRatio(t *testing.T) {
	dir := t.TempDir()
	ds, _ := generator.Generate(dir)
	cases := map[string]float64{"zero": 0, "negative": -0.5, "too_large": 5}
	for name, ratio := range cases {
		_, err := calib.Select(ds.Shards, dir, ratio, 1, generator.ImgWidth, generator.ImgHeight)
		if err == nil || model.CodeOf(err) != model.CodeCalibSample {
			t.Fatalf("%s: expected E_CALIB_SAMPLE, got %v", name, err)
		}
	}
}

func TestSelectRejectsDimensionMismatch(t *testing.T) {
	dir := t.TempDir()
	ds, _ := generator.Generate(filepath.Join(dir, "x"))
	_, err := calib.Select(ds.Shards, filepath.Join(dir, "x"), ds.Policy.CalibRatio, ds.Policy.Seed, 7, 7)
	if err == nil || model.CodeOf(err) != model.CodeCalibSample {
		t.Fatalf("expected dimension mismatch E_CALIB_SAMPLE, got %v", err)
	}
}
