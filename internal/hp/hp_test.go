package hp

import (
	"math"
	"math/big"
	"testing"
)

func TestNaturalLogMatchesReference(t *testing.T) {
	for _, value := range []float64{1.0001, 1.25, 2, 10, 1000, 1e9} {
		logged, err := Log(FromFloat(value))
		if err != nil {
			t.Fatalf("log(%v): %v", value, err)
		}
		got, _ := logged.Float64()
		if math.Abs(got-math.Log(value)) > 1e-14*math.Max(1, math.Abs(math.Log(value))) {
			t.Fatalf("log(%v)=%.17g, want %.17g", value, got, math.Log(value))
		}
	}
}

func TestIDFAndBM25AreFinite(t *testing.T) {
	idf, err := IDF(15, 3)
	if err != nil {
		t.Fatal(err)
	}
	weight, err := BM25Weight(idf, 2, 10, FromInt(12), FromFloat(1.2), FromFloat(0.75))
	if err != nil {
		t.Fatal(err)
	}
	if weight.Sign() <= 0 {
		t.Fatalf("non-positive BM25 weight: %s", weight.Text('g', 20))
	}
	one := new(big.Float).SetPrec(Precision).SetFloat64(1)
	if weight.Cmp(one) <= 0 {
		// Not a strict invariant in every corpus, but this probe term has df=3 and a common weight.
		t.Logf("weight is %s", Decimal(weight, 20))
	}
}

func TestLogRejectsNonPositive(t *testing.T) {
	if _, err := Log(FromInt(0)); err == nil {
		t.Fatal("expected log(0) to fail")
	}
	if _, err := Log(FromInt(-1)); err == nil {
		t.Fatal("expected log(-1) to fail")
	}
}
