package numeric

import (
	"math/big"
	"testing"
)

func TestRoundHalfEvenExactRational(t *testing.T) {
	cases := []struct {
		num, den int64
		scale    int64
		want     int64
	}{
		{1, 2, 1, 0},
		{3, 2, 1, 2},
		{5, 2, 1, 2},
		{-5, 2, 1, -2},
		{155, 200, PPMScaled, 775000},
		{1, 3, PPMScaled, 333333},
		{2, 3, PPMScaled, 666667},
	}
	for _, tc := range cases {
		got, err := RoundHalfEvenRatio(big.NewInt(tc.num), big.NewInt(tc.den), tc.scale)
		if err != nil {
			t.Fatalf("RoundHalfEvenRatio(%d/%d): %v", tc.num, tc.den, err)
		}
		if got.Output != tc.want {
			t.Fatalf("round %d/%d scale %d = %d, want %d", tc.num, tc.den, tc.scale, got.Output, tc.want)
		}
	}
}

func TestQuantiles(t *testing.T) {
	values := []int64{40, 10, 30, 20}
	got, _, err := Quantile(values, 500, QuantileNearestRank)
	if err != nil || got != 20 {
		t.Fatalf("nearest rank = %d, %v; want 20", got, err)
	}
	got, _, err = Quantile(values, 500, QuantileLinear)
	if err != nil || got != 25 {
		t.Fatalf("linear = %d, %v; want 25", got, err)
	}
	got, _, err = Quantile(values, 950, QuantileLinear)
	if err != nil || got != 38 { // 30 + (40-30)*0.85 = 38.5; ties-to-even -> 38
		t.Fatalf("linear p95 = %d, %v; want 38", got, err)
	}
}

func TestCIAndRuntimeBound(t *testing.T) {
	z, err := ZScorePPM(9500)
	if err != nil {
		t.Fatal(err)
	}
	if z != 1959964 {
		t.Fatalf("z=%d, want 1959964", z)
	}
	hw, err := WaldHalfWidthPPM(194, 200, 190, 200, z)
	if err != nil || hw != 38357 {
		t.Fatalf("half width=%d err=%v, want 38357", hw, err)
	}
	bound, err := RuntimeErrorBoundPPM(200, 200, z)
	if err != nil || bound != 97998 {
		t.Fatalf("runtime bound=%d err=%v, want 97998", bound, err)
	}
}

func TestThresholdTolerance(t *testing.T) {
	limit, applied, err := ThresholdWithTolerance(60000, 1000)
	if err != nil || limit != 60060 || applied != 60 {
		t.Fatalf("limit=%d applied=%d err=%v", limit, applied, err)
	}
}
