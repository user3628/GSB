package fixed

import (
	"math"
	"testing"

	"fwadjudge/internal/codes"
)

func TestParseDecimalExact(t *testing.T) {
	v, errU, err := ParseDecimal("2.5")
	if err != nil {
		t.Fatal(err)
	}
	if v != Fixed(2_500_000) || errU != 0 {
		t.Fatalf("got %v errU=%d", v, errU)
	}
	if v.String() != "2.500000" {
		t.Fatalf("string %q", v.String())
	}
}

func TestParseDecimalTruncationAccounting(t *testing.T) {
	// 1.0000005 truncates the 7th fractional digit: error 5e-7 = 5e5 units of 1e-12.
	v, errU, err := ParseDecimal("1.0000005")
	if err != nil {
		t.Fatal(err)
	}
	if v != Fixed(1_000_000) {
		t.Fatalf("value %v", v)
	}
	if errU != 500_000 {
		t.Fatalf("errU %d", errU)
	}
	// All-zero tail: no error.
	if _, errU, _ = ParseDecimal("1.0000000"); errU != 0 {
		t.Fatalf("zero tail errU %d", errU)
	}
	// Long tail: ceiling accounting.
	_, errU, err = ParseDecimal("0.000000123456789")
	if err != nil {
		t.Fatal(err)
	}
	if errU != 123457 {
		t.Fatalf("long tail errU %d", errU)
	}
}

func TestParseDecimalOverflow(t *testing.T) {
	big := "9223372036854775.808" // > MaxInt64 units
	_, _, err := ParseDecimal(big)
	ce, ok := err.(*codes.Error)
	if !ok || ce.Code != codes.EFixedOverflow {
		t.Fatalf("want E_FIXED_OVERFLOW, got %v", err)
	}
}

func TestMulOverflow(t *testing.T) {
	a := Fixed(5_000_000_000_000_000) // 5e9
	_, _, err := Mul(a, a)
	ce, ok := err.(*codes.Error)
	if !ok || ce.Code != codes.EFixedOverflow {
		t.Fatalf("want E_FIXED_OVERFLOW, got %v", err)
	}
}

func TestMulExactAndRemainder(t *testing.T) {
	// 2.5 * 4 = 10 exact
	q, r, err := Mul(Fixed(2_500_000), Fixed(4_000_000))
	if err != nil || q != Fixed(10_000_000) || r != 0 {
		t.Fatalf("q=%v r=%d err=%v", q, r, err)
	}
	// 1e-6 * 1e-6 = 1e-12 -> truncated to 0 with remainder 1 (1e-12 unit)
	q, r, err = Mul(Fixed(1), Fixed(1))
	if err != nil || q != 0 || r != 1 {
		t.Fatalf("q=%v r=%d err=%v", q, r, err)
	}
	// MaxInt64-scale boundary must not overflow.
	maxv := Fixed(math.MaxInt64)
	if _, _, err = Mul(maxv, Fixed(1_000_000)); err != nil {
		t.Fatalf("boundary mul: %v", err)
	}
}

func TestParseCIDRRange(t *testing.T) {
	lo, hi, err := ParseCIDR("10.0.0.0/24")
	if err != nil {
		t.Fatal(err)
	}
	ipLo, _ := ParseIP("10.0.0.0")
	ipHi, _ := ParseIP("10.0.0.255")
	ipOut, _ := ParseIP("10.0.1.0")
	if lo.Cmp(ipLo) != 0 || hi.Cmp(ipHi) != 0 {
		t.Fatalf("range %v..%v", lo, hi)
	}
	if ipOut.Cmp(hi) <= 0 {
		t.Fatal("10.0.1.0 must be outside /24")
	}
	if _, _, err = ParseCIDR("10.0.0.0/33"); err == nil {
		t.Fatal("want error for /33")
	}
}
