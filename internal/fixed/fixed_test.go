package fixed_test

import (
	"errors"
	"math"
	"testing"

	"columnar-executor/internal/fixed"
	"columnar-executor/internal/model"
)

func TestCheckedArithmetic(t *testing.T) {
	if got, err := fixed.Add(1_500_000, 2_250_000); err != nil || got != 3_750_000 {
		t.Fatalf("add = %d, %v", got, err)
	}
	if _, err := fixed.Add(math.MaxInt64, 1); !errors.Is(err, fixed.ErrNumericOverflow) {
		t.Fatalf("expected add overflow, got %v", err)
	}
	if got, err := fixed.Sub(1_500_000, 2_250_000); err != nil || got != -750_000 {
		t.Fatalf("sub = %d, %v", got, err)
	}
	if _, err := fixed.Sub(math.MinInt64, 1); !errors.Is(err, fixed.ErrNumericOverflow) {
		t.Fatalf("expected sub overflow, got %v", err)
	}
	if got, err := fixed.Mul(2_000_000, 3_000_000); err != nil || got != 6_000_000 {
		t.Fatalf("mul = %d, %v", got, err)
	}
	if _, err := fixed.Mul(math.MaxInt64, math.MaxInt64); !errors.Is(err, fixed.ErrNumericOverflow) {
		t.Fatalf("expected mul overflow, got %v", err)
	}
	if got, err := fixed.Div(6_000_000, 2_000_000); err != nil || got != 3_000_000 {
		t.Fatalf("div = %d, %v", got, err)
	}
	if _, err := fixed.Div(1, 0); !errors.Is(err, fixed.ErrDivideByZero) {
		t.Fatalf("expected divide by zero, got %v", err)
	}
}

func TestAverageRoundingIsDeterministic(t *testing.T) {
	tests := []struct {
		sum, count int64
		units      int64
		remainder  int64
	}{
		{sum: 10, count: 4, units: 3, remainder: -2},
		{sum: 11, count: 4, units: 3, remainder: -1},
		{sum: -10, count: 4, units: -3, remainder: 2},
		{sum: 3_000_001, count: 3, units: 1_000_000, remainder: 1},
	}
	for _, test := range tests {
		units, remainder, err := fixed.Average(test.sum, test.count)
		if err != nil {
			t.Fatal(err)
		}
		if units != test.units || remainder != test.remainder {
			t.Fatalf("average(%d,%d) = (%d,%d), want (%d,%d)", test.sum, test.count, units, remainder, test.units, test.remainder)
		}
		units2, remainder2, err := fixed.Average(test.sum, test.count)
		if err != nil || units2 != units || remainder2 != remainder {
			t.Fatalf("average is not deterministic: %d,%d vs %d,%d", units, remainder, units2, remainder2)
		}
	}
	if got := model.FormatFixed(-12_345_678); got != "-12.345678" {
		t.Fatalf("FormatFixed = %s", got)
	}
}

func TestRelativeTolerance(t *testing.T) {
	if !fixed.EqualRelative(1_000_000, 1_000_001, 10) {
		t.Fatal("expected values inside relative tolerance")
	}
	if fixed.EqualRelative(1_000_000, 1_100_000, 10) {
		t.Fatal("expected values outside relative tolerance")
	}
}
