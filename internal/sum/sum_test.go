package sum

import (
	"math"
	"testing"

	"example.com/joinshuffle/internal/model"
)

func TestDoubleDoubleFixedIsOrderIndependent(t *testing.T) {
	values := []float64{1e15, -1e15, 1e-12, 3e-12, -2e-12, 7.25, -7.0, 1e-300}
	one, err := NewAccumulator(model.SumDoubleDoubleFix, 80)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		one.Add(value)
	}
	single := one.Partial(1, 0)
	singleMerge, err := Merge([]Partial{single}, model.SumDoubleDoubleFix, 80, "magnitude_bucket_partition_asc")
	if err != nil {
		t.Fatal(err)
	}

	var partials []Partial
	for partition, subset := range [][]float64{{-1e15, 1e-12, -7.0}, {1e15, 3e-12, 1e-300}, {-2e-12, 7.25}} {
		acc, err := NewAccumulator(model.SumDoubleDoubleFix, 80)
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range subset {
			acc.Add(value)
		}
		partials = append(partials, acc.Partial(1, partition))
	}
	multiMerge, err := Merge(partials, model.SumDoubleDoubleFix, 80, "magnitude_bucket_partition_asc")
	if err != nil {
		t.Fatal(err)
	}
	if math.Float64bits(singleMerge.Sum) != math.Float64bits(multiMerge.Sum) {
		t.Fatalf("fixed-point sum changed with partition order: single=%v multi=%v", singleMerge.Sum, multiMerge.Sum)
	}
	if multiMerge.AbsErrorBound <= 0 || multiMerge.RelErrorBound <= 0 {
		t.Fatalf("expected positive error bounds: %+v", multiMerge)
	}
	if multiMerge.CompensationHash == "" || multiMerge.MergeOrderHash == "" {
		t.Fatal("missing compensation or merge-order digest")
	}
}

func TestNaiveAndKahanProduceDeclaredState(t *testing.T) {
	values := []float64{1, 1e-16, 1e-16, -1, 4.5}
	for _, algorithm := range []model.SumAlgorithm{model.SumNaive, model.SumKahan} {
		acc, err := NewAccumulator(algorithm, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range values {
			acc.Add(value)
		}
		partial := acc.Partial(9, 0)
		result, err := Merge([]Partial{partial}, algorithm, 0, "partition_ascending")
		if err != nil {
			t.Fatal(err)
		}
		if math.IsNaN(result.Sum) || math.IsInf(result.Sum, 0) {
			t.Fatalf("%s returned non-finite sum", algorithm)
		}
		if result.AbsErrorBound <= 0 || result.CompensationHash == "" {
			t.Fatalf("%s missing quantitative error state: %+v", algorithm, result)
		}
	}
}

func TestQuantizeVerySmallAndLargeValues(t *testing.T) {
	for _, value := range []float64{1e-300, -1e-300, 1e300, -1e300, math.SmallestNonzeroFloat64} {
		n := quantize(value, 80)
		if n == nil {
			t.Fatalf("nil quantized value for %g", value)
		}
	}
	if quantize(1, 80).Sign() != 1 {
		t.Fatal("positive quantization sign lost")
	}
	if quantize(-1, 80).Sign() != -1 {
		t.Fatal("negative quantization sign lost")
	}
}

func TestErrorBoundRemainsFinite(t *testing.T) {
	bound := errorBound(model.SumNaive, 0, 1<<62, math.MaxFloat64, 0)
	if math.IsInf(bound, 0) || math.IsNaN(bound) {
		t.Fatal("naive bound must be finite for JSON transport")
	}
	rel := relativeBound(math.MaxFloat64, math.SmallestNonzeroFloat64)
	if math.IsInf(rel, 0) || math.IsNaN(rel) {
		t.Fatal("relative bound must be finite for JSON transport")
	}
}
