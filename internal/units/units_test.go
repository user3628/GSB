package units

import (
	"testing"

	"baselineworkbench/internal/codes"
)

func TestEquivalentDurationUnits(t *testing.T) {
	tests := []struct {
		unit  string
		value string
		want  int64
	}{
		{"second", "900", 900},
		{"minute", "15", 900},
		{"hour", "0.25", 900},
		{"day", "60", 5_184_000},
	}
	for _, test := range tests {
		if test.unit == "hour" {
			if _, err := Convert(test.unit, test.value); err == nil {
				t.Fatal("fractional durations must be rejected as malformed input")
			}
			continue
		}
		got, err := Convert(test.unit, test.value)
		if err != nil {
			t.Fatalf("%s %s: %v", test.unit, test.value, err)
		}
		if got.Normalized != test.want {
			t.Fatalf("%s %s: got %d want %d", test.unit, test.value, got.Normalized, test.want)
		}
	}
	hour, err := Convert("hour", "1")
	if err != nil || hour.Normalized != 3600 {
		t.Fatalf("hour conversion failed: %+v %v", hour, err)
	}
}

func TestFiletimeConversionAndTruncation(t *testing.T) {
	result, err := Convert("filetime", "133485408000000000")
	if err != nil {
		t.Fatal(err)
	}
	if result.Normalized != 1_704_067_200 || result.ErrorUpperScaled != 0 {
		t.Fatalf("unexpected FILETIME conversion: %+v", result)
	}
	result, err = Convert("filetime", "133485408000000001")
	if err != nil {
		t.Fatal(err)
	}
	if result.Normalized != 1_704_067_200 || result.ErrorUpperScaled != 1_000_000 {
		t.Fatalf("expected sub-second truncation upper bound, got %+v", result)
	}
}

func TestScoreTruncation(t *testing.T) {
	result, err := Convert("score", "3.1415927")
	if err != nil {
		t.Fatal(err)
	}
	if result.Normalized != 3_141_592 || result.ErrorUpperScaled != 1 {
		t.Fatalf("unexpected score conversion: %+v", result)
	}
}

func TestUnitAndNumericErrors(t *testing.T) {
	if _, err := Convert("fortnight", "1"); err == nil || err.Code != codes.UnitUnknown {
		t.Fatalf("expected E_UNIT_UNKNOWN, got %v", err)
	}
	if _, err := Convert("filetime", "9223372036854775808"); err == nil || err.Code != codes.NumericOverflow {
		t.Fatalf("expected E_NUMERIC_OVERFLOW, got %v", err)
	}
	if _, err := Convert("day", "106751991168"); err == nil || err.Code != codes.NumericOverflow {
		t.Fatalf("expected duration overflow, got %v", err)
	}
}
