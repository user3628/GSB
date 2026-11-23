package fixed

import (
	"math/big"
	"testing"
)

func bi(s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic(s)
	}
	return v
}

func TestParseFormatAndRounding(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"0", "0.000000"},
		{"1.25", "1.250000"},
		{"0.0000005", "0.000001"},
		{"-2.0000004", "-2.000000"},
		{"9007199254740993.000001", "9007199254740993.000001"},
	}
	for _, tc := range cases {
		u, err := ParseDecimal(tc.in)
		if err != nil {
			t.Fatalf("parse %s: %v", tc.in, err)
		}
		if got := Format(u); got != tc.want {
			t.Fatalf("parse/format %s = %s, want %s", tc.in, got, tc.want)
		}
	}
}

func TestDivRoundHalfAwayFromZero(t *testing.T) {
	u, r, err := DivRound(big.NewInt(1), big.NewInt(2))
	if err != nil {
		t.Fatal(err)
	}
	if u.Int64() != 500000 || r.Sign() != 0 {
		t.Fatalf("1/2 units=%s rem=%s", u, r)
	}
	u, _, _ = DivRound(big.NewInt(1), big.NewInt(3))
	if u.Int64() != 333333 {
		t.Fatalf("1/3 = %s", u)
	}
	u, _, _ = DivRound(big.NewInt(2), big.NewInt(3))
	if u.Int64() != 666667 {
		t.Fatalf("2/3 = %s", u)
	}
}

func TestWeightedAverageCompensated(t *testing.T) {
	weights := []*big.Int{big.NewInt(1), big.NewInt(1), big.NewInt(1)}
	values := []*big.Int{big.NewInt(1), big.NewInt(1), bi("1000000")}
	got, err := WeightedAverage(weights, values)
	if err != nil {
		t.Fatal(err)
	}
	if got.Units.String() != "333334" || got.NaiveUnits.String() != "333333" {
		t.Fatalf("units=%s naive=%s", got.Units, got.NaiveUnits)
	}
	if got.NaiveUnits.Cmp(got.Units) == 0 {
		t.Fatalf("sample was expected to expose a naive per-term rounding difference")
	}
	if got.NaiveErrorNumerator.Sign() == 0 {
		t.Fatalf("expected nonzero naive error")
	}
	reversedWeights := []*big.Int{weights[2], weights[1], weights[0]}
	reversedValues := []*big.Int{values[2], values[1], values[0]}
	reversed, err := WeightedAverage(reversedWeights, reversedValues)
	if err != nil {
		t.Fatal(err)
	}
	if reversed.Units.Cmp(got.Units) != 0 {
		t.Fatalf("order changed compensated result: %s != %s", reversed.Units, got.Units)
	}
}

func TestToleranceIsInclusive(t *testing.T) {
	bound := bi("250000")
	tol := bi("4000") // 0.004 relative
	lower, err := RelativeToleranceLower(bound, tol)
	if err != nil {
		t.Fatal(err)
	}
	if lower.String() != "249000" {
		t.Fatalf("lower=%s", lower)
	}
	if lower.Cmp(bi("249000")) != 0 {
		t.Fatal("boundary should be included")
	}
}
