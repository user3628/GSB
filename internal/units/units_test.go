package units

import (
	"testing"

	"configaudit/internal/fixedpoint"
)

func TestLegacySuffixes(t *testing.T) {
	cases := []struct {
		text  string
		scale int
		want  int64
		enum  string
	}{
		{"512Mi", 0, 536870912, "MiB"},
		{"1.5 GiB", 3, 1610612736000, "GiB"},
		{"250m", 6, 250000, "m"},
		{"500M", 3, 500000000000, "MB"},
		{"100k", 3, 100000000, "kB"},
	}
	for _, c := range cases {
		rec, err := ParseLegacy(c.text, c.scale)
		if err != nil {
			t.Fatalf("parse %q: %v", c.text, err)
		}
		if rec.Normalized.Mantissa != c.want {
			t.Fatalf("%q mantissa = %d want %d", c.text, rec.Normalized.Mantissa, c.want)
		}
		if rec.UnitEnum != c.enum {
			t.Fatalf("%q enum = %s want %s", c.text, rec.UnitEnum, c.enum)
		}
		if rec.RawText != c.text {
			t.Fatalf("raw text = %q", rec.RawText)
		}
	}
}

func TestGenerationsNormalizeIdentically(t *testing.T) {
	legacy, err := ParseLegacy("1.5 GiB", 3)
	if err != nil {
		t.Fatal(err)
	}
	structured, err := ParseStructured(`{"value":"1.5","unit":"GiB"}`, Structured{Value: "1.5", Unit: "GiB"}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Normalized != structured.Normalized {
		t.Fatalf("normalization differs: %+v vs %+v", legacy.Normalized, structured.Normalized)
	}
}

func TestUnknownSuffix(t *testing.T) {
	if _, err := ParseLegacy("9 XyZ", 3); fixedpoint.CodeOf(err) != fixedpoint.EUnitInvalid {
		t.Fatalf("code = %v", fixedpoint.CodeOf(err))
	}
	if _, err := ParseStructured("x", Structured{Value: "1", Unit: "Nope"}, 3); fixedpoint.CodeOf(err) != fixedpoint.EUnitInvalid {
		t.Fatalf("code = %v", fixedpoint.CodeOf(err))
	}
}

func TestRoundingMetadata(t *testing.T) {
	// 1/3 second expressed in seconds at scale 6: rounds and records error.
	rec, err := ParseLegacy("0.3333334m", 6)
	if err != nil {
		t.Fatal(err)
	}
	if rec.CumErrorULP == 0 {
		t.Fatalf("expected rounding error, got %+v", rec)
	}
}
