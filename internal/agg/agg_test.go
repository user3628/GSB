package agg

import (
	"math"
	"math/big"
	"math/rand"
	"testing"
)

func exactAbsError(t *testing.T, values []float64, got float64) *big.Rat {
	t.Helper()
	exact := new(big.Rat)
	for _, value := range values {
		exact.Add(exact, new(big.Rat).SetFloat64(value))
	}
	actual := new(big.Rat).SetFloat64(got)
	if actual == nil {
		t.Fatalf("non-finite result %v", got)
	}
	diff := new(big.Rat).Sub(actual, exact)
	return diff.Abs(diff)
}

func TestErrorBoundsCoverActualErrors(t *testing.T) {
	values := []float64{
		1e16, 1, -1e16, 3.141592653589793, 1e-9, -2.718281828459045,
		1e12, 1e-12, -3.333333333333333, 1e8, 1e-8, 7.0,
	}
	for _, algorithm := range []Algorithm{Naive, Kahan, Magnitude} {
		t.Run(string(algorithm), func(t *testing.T) {
			group := NewGroup(algorithm)
			for _, value := range values {
				group.Add("k", value)
			}
			state := group.Keys["k"]
			value := state.Value(algorithm)
			actual := exactAbsError(t, values, value)
			bound := state.AbsErrorBound(algorithm)
			boundRat := new(big.Rat).SetFloat64(bound)
			if boundRat != nil && actual.Cmp(boundRat) > 0 {
				t.Fatalf("actual error %s exceeds declared bound %.17g", actual.FloatString(20), bound)
			}
		})
	}
}

func TestBatchBoundariesDoNotChangeState(t *testing.T) {
	values := []float64{1e-20, 1e20, -1e20, 1, 1e-20, 1e20, 2, -1e20, 1e-5}
	for _, algorithm := range []Algorithm{Naive, Kahan, Magnitude} {
		one := NewGroup(algorithm)
		split := NewGroup(algorithm)
		for _, value := range values {
			one.Add("key", value)
		}
		for i, value := range values {
			split.Add("key", value)
			if i%2 == 0 {
				_ = split.Digest()
			}
		}
		if one.Digest() != split.Digest() {
			t.Fatalf("%s state depends on batch boundaries", algorithm)
		}
	}
}

func TestMagnitudeValueIsFiniteAndStable(t *testing.T) {
	group := NewGroup(Magnitude)
	for _, value := range []float64{1e300, 1e-300, -1e300, 2.5, 0} {
		group.Add("k", value)
	}
	state := group.Keys["k"]
	got := state.Value(Magnitude)
	if math.IsNaN(got) || math.IsInf(got, 0) {
		t.Fatalf("unexpected non-finite value: %v", got)
	}
	if got < 2.49 || got > 2.51 {
		t.Fatalf("unexpected value: %v", got)
	}
}

func TestDeclaredBoundsHoldAcrossRandomWideMagnitudeSamples(t *testing.T) {
	rng := rand.New(rand.NewSource(20261125))
	for trial := 0; trial < 120; trial++ {
		values := make([]float64, 300)
		for i := range values {
			exponent := rng.Intn(401) - 200
			mantissa := 1 + rng.Float64()*9
			values[i] = mantissa * math.Pow10(exponent)
			if rng.Intn(2) == 0 {
				values[i] = -values[i]
			}
		}
		for _, algorithm := range []Algorithm{Naive, Kahan, Magnitude} {
			group := NewGroup(algorithm)
			for _, value := range values {
				group.Add("k", value)
			}
			state := group.Keys["k"]
			result := state.Value(algorithm)
			if math.IsNaN(result) || math.IsInf(result, 0) {
				t.Fatalf("trial %d %s produced non-finite result", trial, algorithm)
			}
			actual := exactAbsError(t, values, result)
			bound := state.AbsErrorBound(algorithm)
			boundRat := new(big.Rat).SetFloat64(bound)
			if boundRat != nil && actual.Cmp(boundRat) > 0 {
				t.Fatalf("trial %d %s error %s exceeds bound %.17g",
					trial, algorithm, actual.FloatString(20), bound)
			}
		}
	}
}
