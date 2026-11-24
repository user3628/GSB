package fixed

import (
	"math"
	"strings"
	"testing"
)

func TestMulDivHalfUpAndOverflow(t *testing.T) {
	tests := []struct {
		a, b, d int64
		want    int64
	}{
		{5, 1, 2, 3},
		{-5, 1, 2, -3},
		{4, 1, 2, 2},
		{7, 3, 5, 4},
	}
	for _, tc := range tests {
		got, _, err := MulDivHalfUp(tc.a, tc.b, tc.d)
		if err != nil {
			t.Fatalf("MulDivHalfUp(%d,%d,%d): %v", tc.a, tc.b, tc.d, err)
		}
		if got != tc.want {
			t.Fatalf("MulDivHalfUp(%d,%d,%d)=%d want %d", tc.a, tc.b, tc.d, got, tc.want)
		}
	}
	if _, _, err := MulDivHalfUp(math.MaxInt64, 2, 1); err == nil {
		t.Fatal("expected overflow error")
	}
	if _, _, err := MulDivHalfUp(1, 1, 0); err == nil {
		t.Fatal("expected division by zero")
	}
}

func TestCompareDecimalRelativeBoundary(t *testing.T) {
	within, err := CompareDecimalRelative("100", "101", 10_000)
	if err != nil || !within.WithinTolerance || within.Equal {
		t.Fatalf("1%% boundary should be within tolerance: %+v err=%v", within, err)
	}
	exactBoundary, _ := CompareDecimalRelative("100", "101.01", 10_000)
	if !exactBoundary.WithinTolerance {
		t.Fatalf("exact allowed delta must be accepted: %+v", exactBoundary)
	}
	outside, _ := CompareDecimalRelative("100", "101.011", 10_000)
	if outside.WithinTolerance {
		t.Fatalf("delta above tolerance must drift: %+v", outside)
	}
	equal, _ := CompareDecimalRelative("0.30", "0.300000", 0)
	if !equal.Equal || !equal.WithinTolerance {
		t.Fatalf("decimal equality must not depend on formatting: %+v", equal)
	}
	negative, _ := CompareDecimalRelative("-100", "-101", 10_000)
	if !negative.WithinTolerance {
		t.Fatalf("relative tolerance must work for negatives: %+v", negative)
	}
}

func TestCompareTimeBudgetTolerance(t *testing.T) {
	atLimit, err := CompareTimeBudget(1_005_000, 1_000_000, 5_000, 3)
	if err != nil {
		t.Fatal(err)
	}
	if atLimit.Decision != "accept" || !atLimit.WithinTolerance || atLimit.ToleranceMicros != 5_000 {
		t.Fatalf("unexpected tolerance decision: %+v", atLimit)
	}
	over, err := CompareTimeBudget(1_005_001, 1_000_000, 5_000, 4)
	if err != nil {
		t.Fatal(err)
	}
	if over.Decision != "skip" || over.Tolerance != "0.005000ms" {
		t.Fatalf("unexpected over-budget decision: %+v", over)
	}
	if !strings.Contains(over.Explanation, "exceeds") {
		t.Fatalf("missing reproducible explanation: %s", over.Explanation)
	}
}

func TestFixedFormatting(t *testing.T) {
	if got := FormatMicros(1_234_567); got != "1.234567" {
		t.Fatalf("FormatMicros=%s", got)
	}
	if got := FormatMicros(-1); got != "-0.000001" {
		t.Fatalf("negative FormatMicros=%s", got)
	}
	if _, err := MicrosFromMs(math.MaxInt64); err == nil {
		t.Fatal("expected millisecond overflow")
	}
}
