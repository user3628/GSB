package noise

import (
	"testing"

	"matbench/internal/recipe"
)

func sampleRecipe(octaves int) recipe.Recipe {
	amps := make([]float64, octaves)
	sum := 0.0
	for i := range amps {
		amps[i] = 1.0 / float64(i+1)
		sum += amps[i]
	}
	for i := range amps {
		amps[i] /= sum
	}
	return recipe.Recipe{
		MaterialID: "MAT-T",
		Noise: recipe.NoiseParams{
			Octaves: octaves, Frequency: 2, Amplitudes: amps, Seed: 42,
		},
		TargetResolution: 16,
	}
}

func TestSingleVsMultiThreadByteIdentical(t *testing.T) {
	r := sampleRecipe(5)
	single, err := Synthesize(r, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []int{2, 4, 8} {
		multi, err := Synthesize(r, w)
		if err != nil {
			t.Fatal(err)
		}
		if single.Fingerprint != multi.Fingerprint {
			t.Fatalf("fingerprint mismatch workers=%d", w)
		}
		for i := range single.Field {
			if single.Field[i] != multi.Field[i] {
				t.Fatalf("field byte mismatch at %d workers=%d", i, w)
			}
		}
	}
}

func TestDegenerateOctaveZero(t *testing.T) {
	r := sampleRecipe(1)
	r.Noise.Octaves = 0
	res, err := Synthesize(r, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Degenerate || res.DegenerateNote == "" {
		t.Fatal("expected degenerate flag for zero octaves")
	}
	if len(res.Octaves) != 0 {
		t.Fatal("no octave diagnostics expected")
	}
}

func TestDegenerateAllZeroAmplitudes(t *testing.T) {
	r := sampleRecipe(3)
	for i := range r.Noise.Amplitudes {
		r.Noise.Amplitudes[i] = 0
	}
	res, err := Synthesize(r, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Degenerate {
		t.Fatal("expected degenerate flag for zero amplitudes")
	}
	nonZero := false
	for _, v := range res.Field {
		if v != 0 {
			nonZero = true
		}
	}
	if nonZero {
		t.Fatal("field must stay zero, not silently blanked")
	}
}

func TestDiagnosticsAndRanks(t *testing.T) {
	r := sampleRecipe(6)
	res, err := Synthesize(r, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Octaves) != 6 {
		t.Fatalf("octaves=%d", len(res.Octaves))
	}
	for _, o := range res.Octaves {
		if o.ErrorBound <= 0 || o.CumulativeBound <= 0 {
			t.Fatalf("octave %d has non-positive error bound", o.Index)
		}
		if o.LogContribution < 0 {
			t.Fatalf("octave %d negative log contribution", o.Index)
		}
		if o.ReviewFlag {
			found := false
			for _, idx := range res.ReviewOctaves {
				if idx == o.Index {
					found = true
				}
			}
			if !found {
				t.Fatalf("review flag not mirrored for octave %d", o.Index)
			}
		}
	}
	if len(res.PerturbReplay) != 6 {
		t.Fatal("perturbation replay missing")
	}
}
