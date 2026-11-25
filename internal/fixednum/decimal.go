package fixednum

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

type Rational struct {
	N *big.Int
	D *big.Int
}

func Rat(n, d int64) Rational {
	if d == 0 {
		panic("zero denominator")
	}
	num := big.NewInt(n)
	den := big.NewInt(d)
	if den.Sign() < 0 {
		num.Neg(num)
		den.Neg(den)
	}
	return Rational{N: num, D: den}
}

func Parse(s string) (Rational, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Rational{}, fmt.Errorf("empty number")
	}
	if strings.Contains(s, "/") {
		parts := strings.SplitN(s, "/", 2)
		n, okN := new(big.Int).SetString(strings.TrimSpace(parts[0]), 10)
		d, okD := new(big.Int).SetString(strings.TrimSpace(parts[1]), 10)
		if !okN || !okD || d.Sign() == 0 {
			return Rational{}, fmt.Errorf("invalid fraction %q", s)
		}
		r := Rational{N: n, D: d}
		r.normalizeSign()
		return r, nil
	}
	r, ok := parseDecimal(s)
	if !ok {
		return Rational{}, fmt.Errorf("invalid decimal %q", s)
	}
	return r, nil
}

func parseDecimal(s string) (Rational, bool) {
	negative := false
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		negative = s[0] == '-'
		s = s[1:]
	}
	if strings.HasPrefix(strings.ToLower(s), "0x") {
		return Rational{}, false
	}
	parts := strings.SplitN(s, ".", 2)
	integer := parts[0]
	if integer == "" {
		integer = "0"
	}
	if _, err := strconv.ParseInt(integer, 10, 64); err != nil {
		return Rational{}, false
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
		if fraction == "" {
			fraction = "0"
		}
		if _, ok := new(big.Int).SetString(fraction, 10); !ok {
			return Rational{}, false
		}
	}
	num, _ := new(big.Int).SetString(integer+fraction, 10)
	if num == nil {
		return Rational{}, false
	}
	den := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(len(fraction))), nil)
	if negative {
		num.Neg(num)
	}
	r := Rational{N: num, D: den}
	return r, true
}

func FromValue(v any) (Rational, bool) {
	switch x := v.(type) {
	case json.Number:
		if r, err := Parse(x.String()); err == nil {
			return r, true
		}
		if i, err := x.Int64(); err == nil {
			return Rat(i, 1), true
		}
		if f, err := x.Float64(); err == nil {
			return ratFromFloat(f)
		}
	case int:
		return Rat(int64(x), 1), true
	case int8:
		return Rat(int64(x), 1), true
	case int16:
		return Rat(int64(x), 1), true
	case int32:
		return Rat(int64(x), 1), true
	case int64:
		return Rat(x, 1), true
	case uint:
		return newUintRat(uint64(x)), true
	case uint64:
		return newUintRat(x), true
	case float64:
		return ratFromFloat(x)
	case float32:
		return ratFromFloat(float64(x))
	}
	return Rational{}, false
}

func ratFromFloat(f float64) (Rational, bool) {
	r := new(big.Rat).SetFloat64(f)
	if r == nil {
		return Rational{}, false
	}
	return Rational{N: new(big.Int).Set(r.Num()), D: new(big.Int).Set(r.Denom())}, true
}

func newUintRat(x uint64) Rational {
	n := new(big.Int).SetUint64(x)
	return Rational{N: n, D: big.NewInt(1)}
}

func (r Rational) normalizeSign() {
	if r.D.Sign() < 0 {
		r.N.Neg(r.N)
		r.D.Neg(r.D)
	}
}

func (r Rational) Clone() Rational {
	return Rational{N: new(big.Int).Set(r.N), D: new(big.Int).Set(r.D)}
}

func (r Rational) Abs() Rational {
	c := r.Clone()
	if c.N.Sign() < 0 {
		c.N.Abs(c.N)
	}
	return c
}

func (r Rational) Mul(o Rational) Rational {
	n := new(big.Int).Mul(new(big.Int).Set(r.N), o.N)
	d := new(big.Int).Mul(new(big.Int).Set(r.D), o.D)
	return Rational{N: n, D: d}
}

func (r Rational) Sub(o Rational) Rational {
	n := new(big.Int).Sub(
		new(big.Int).Mul(r.N, o.D),
		new(big.Int).Mul(o.N, r.D),
	)
	d := new(big.Int).Mul(new(big.Int).Set(r.D), o.D)
	return Rational{N: n, D: d}
}

func (r Rational) Cmp(o Rational) int {
	left := new(big.Int).Mul(new(big.Int).Set(r.N), o.D)
	right := new(big.Int).Mul(new(big.Int).Set(o.N), r.D)
	return left.Cmp(right)
}

func (r Rational) String() string {
	if r.D.Cmp(big.NewInt(1)) == 0 {
		return r.N.String()
	}
	gr := new(big.Int).GCD(nil, nil, new(big.Int).Abs(new(big.Int).Set(r.N)), new(big.Int).Set(r.D))
	n := new(big.Int).Quo(r.N, gr)
	d := new(big.Int).Quo(r.D, gr)
	if d.Cmp(big.NewInt(1)) == 0 {
		return n.String()
	}
	return n.String() + "/" + d.String()
}

type BoundaryResult struct {
	Within          bool
	Difference      Rational
	Allowed         Rational
	Inclusive       bool
	BoundaryPolicy  string
	DifferenceExact string
	AllowedExact    string
}

func RelativeWithin(actual, desired, tolerance Rational, inclusive bool) BoundaryResult {
	diff := actual.Sub(desired).Abs()
	base := desired.Abs()
	allowed := tolerance.Abs().Mul(base)
	cmp := diff.Cmp(allowed)
	within := cmp < 0 || (inclusive && cmp == 0)
	policy := "exclusive <"
	if inclusive {
		policy = "inclusive <="
	}
	return BoundaryResult{
		Within:          within,
		Difference:      diff,
		Allowed:         allowed,
		Inclusive:       inclusive,
		BoundaryPolicy:  policy,
		DifferenceExact: diff.String(),
		AllowedExact:    allowed.String(),
	}
}
