package fixedpoint

import (
	"math/big"
	"testing"
)

func TestParseDecimalAndRescale(t *testing.T) {
	cases := []struct {
		in    string
		scale int
		want  int64
		text  string
	}{
		{"1.5", 3, 1500, "1.500"},
		{"1.0005", 3, 1001, "1.001"}, // half away rounds up
		{"-1.0005", 3, -1001, "-1.001"},
		{"0.123456789", 9, 123456789, "0.123456789"},
		{"42", 0, 42, "42"},
	}
	for _, c := range cases {
		v, err := ParseDecimal(c.in, c.scale)
		if err != nil {
			t.Fatalf("parse %q: %v", c.in, err)
		}
		if v.Mantissa != c.want {
			t.Fatalf("parse %q mantissa = %d want %d", c.in, v.Mantissa, c.want)
		}
		if v.Decimal() != c.text {
			t.Fatalf("decimal %q = %q want %q", c.in, v.Decimal(), c.text)
		}
	}
}

func TestOverflow(t *testing.T) {
	if _, err := MulExact(FromInt(MaxInt64), 2); CodeOf(err) != ENumericOverflow {
		t.Fatalf("mul overflow code = %v", CodeOf(err))
	}
	if _, err := Add(FromInt(MaxInt64), FromInt(1)); CodeOf(err) != ENumericOverflow {
		t.Fatalf("add overflow code = %v", CodeOf(err))
	}
	if _, err := Rescale(Value{Mantissa: MaxInt64, Scale: 0}, 9); CodeOf(err) != ENumericOverflow {
		t.Fatalf("rescale overflow code = %v", CodeOf(err))
	}
}

func TestEvalChainErrorPropagation(t *testing.T) {
	// 1/3 at scale 6, then *7: division introduces 0.5 ulp error.
	start := Value{Mantissa: 1000000, Scale: 6}
	chain := []ChainStep{{Op: "div", Factor: 3}, {Op: "mul", Factor: 7}}
	res, err := EvalChain(start, chain)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Steps) != 2 {
		t.Fatalf("steps = %d", len(res.Steps))
	}
	if res.Steps[0].Rounding != "round_half_away" {
		t.Fatalf("rounding mode = %q", res.Steps[0].Rounding)
	}
	// exact final = 7/3, fixed final = 0.333333*7 = 2.333331
	wantExact := big.NewRat(7, 3)
	if res.ExactFinal.Cmp(wantExact) != 0 {
		t.Fatalf("exact = %s want %s", res.ExactFinal, wantExact)
	}
	actual := res.ActualAbsError()
	if actual.Sign() <= 0 {
		t.Fatalf("actual error should be positive, got %s", actual)
	}
	if actual.Cmp(res.BoundRat) > 0 {
		t.Fatalf("actual %s exceeds synthesized bound %s", actual, res.BoundRat)
	}
}

func TestEvalChainOverflowStep(t *testing.T) {
	start := FromInt(MaxInt64)
	_, err := EvalChain(start, []ChainStep{{Op: "mul", Factor: 2}})
	if CodeOf(err) != ENumericOverflow {
		t.Fatalf("code = %v", CodeOf(err))
	}
	ce := err.(*Error)
	if ce.Step != 1 {
		t.Fatalf("step = %d want 1", ce.Step)
	}
}

func TestExactMulHasZeroError(t *testing.T) {
	start := Value{Mantissa: 123456, Scale: 3}
	res, err := EvalChain(start, []ChainStep{{Op: "mul", Factor: 1024}})
	if err != nil {
		t.Fatal(err)
	}
	if res.BoundRat.Sign() != 0 {
		t.Fatalf("exact mul bound should be zero, got %s", res.BoundRat)
	}
	if res.ActualAbsError().Sign() != 0 {
		t.Fatalf("exact mul actual error = %s", res.ActualAbsError())
	}
}

func TestChainOverflowReportsStep(t *testing.T) {
	// The first multiplication overflows before the division is reached.
	_, err := EvalChain(FromInt(MaxInt64), []ChainStep{{Op: "mul", Factor: 2}, {Op: "div", Factor: 3}})
	if CodeOf(err) != ENumericOverflow {
		t.Fatalf("code = %v", CodeOf(err))
	}
	if err.(*Error).Step != 1 {
		t.Fatalf("step = %d", err.(*Error).Step)
	}
}
