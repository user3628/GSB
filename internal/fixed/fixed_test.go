package fixed

import (
	"math"
	"testing"

	"baselineworkbench/internal/codes"
)

func TestParseDecimalToMicros(t *testing.T) {
	tests := []struct {
		input      string
		value      int64
		discard    bool
		errorUpper int64
	}{
		{"3.141592", 3_141_592, false, 0},
		{"3.1415927", 3_141_592, true, 1},
		{"-3.1415927", -3_141_592, true, 1},
		{"9000000000000.000000", 9_000_000_000_000_000_000, false, 0},
		{"0.0000010", 1, false, 0},
	}
	for _, test := range tests {
		result, err := ParseDecimalToMicros(test.input)
		if err != nil {
			t.Fatalf("%s: %v", test.input, err)
		}
		if result.Value != test.value || result.HasDiscard != test.discard || result.ErrorUpper != test.errorUpper {
			t.Fatalf("%s: got value=%d discard=%t error=%d", test.input, result.Value, result.HasDiscard, result.ErrorUpper)
		}
	}
}

func TestParseDecimalOverflow(t *testing.T) {
	if _, err := ParseDecimalToMicros("9223372036854.775808"); err == nil {
		t.Fatal("expected overflow")
	} else if business, ok := err.(*codes.BusinessError); !ok || business.Code != codes.NumericOverflow {
		t.Fatalf("expected E_NUMERIC_OVERFLOW, got %v", err)
	}
}

func TestCheckedArithmetic(t *testing.T) {
	if _, err := MulInt64(math.MaxInt64, 2); err == nil {
		t.Fatal("expected multiplication overflow")
	}
	if _, err := AddInt64(math.MaxInt64, 1); err == nil {
		t.Fatal("expected addition overflow")
	}
	if _, err := SubInt64(math.MinInt64, 1); err == nil {
		t.Fatal("expected subtraction overflow")
	}
	value, _, err := MulDivInt64(-10_000_001, 1_000_000, Scale)
	if err != nil {
		t.Fatal(err)
	}
	if value != -10_000_001 {
		t.Fatalf("unexpected value %d", value)
	}
}

func TestFormatMicros(t *testing.T) {
	if got := FormatMicros(math.MinInt64); got != "-9223372036854.775808" {
		t.Fatalf("unexpected MinInt64 formatting: %s", got)
	}
	if got := FormatMicros(-64_800_000_000); got != "-64800.000000" {
		t.Fatalf("unexpected formatting: %s", got)
	}
}
