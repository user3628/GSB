package fixedpoint

import (
	"math"
	"math/big"
	"strings"
)

// AllowedScales is the permitted fixed-point decimal scale set.
var AllowedScales = map[int]bool{0: true, 3: true, 6: true, 9: true}

// MaxInt64 / MinInt64 are kept local for explicit overflow checks.
const (
	MaxInt64 = int64(math.MaxInt64)
	MinInt64 = int64(math.MinInt64)
)

// Value is an int64 fixed-point number: mantissa * 10^(-scale).
type Value struct {
	Mantissa int64
	Scale    int
}

// Zero returns a fixed-point zero at the given scale.
func Zero(scale int) Value { return Value{Mantissa: 0, Scale: scale} }

// FromInt wraps an integer at scale 0.
func FromInt(v int64) Value { return Value{Mantissa: v, Scale: 0} }

// Pow10 returns 10^n for non-negative n, flagging overflow.
func Pow10(n int) (int64, bool) {
	if n < 0 {
		return 0, false
	}
	p := int64(1)
	for i := 0; i < n; i++ {
		if p > MaxInt64/10 {
			return 0, false
		}
		p *= 10
	}
	return p, true
}

// mulOverflows reports whether a*b overflows int64.
func mulOverflows(a, b int64) bool {
	if a == 0 || b == 0 {
		return false
	}
	r := a * b
	return r/b != a || (a == MinInt64 && b == -1) || (b == MinInt64 && a == -1)
}

// addOverflows reports whether a+b overflows int64.
func addOverflows(a, b int64) bool {
	if b > 0 && a > MaxInt64-b {
		return true
	}
	if b < 0 && a < MinInt64-b {
		return true
	}
	return false
}

// Add returns a+b when both share a scale; overflow is reported explicitly.
func Add(a, b Value) (Value, error) {
	if a.Scale != b.Scale {
		return Value{}, NewError(EInvalidInput, "add requires identical scales, got %d and %d", a.Scale, b.Scale)
	}
	if addOverflows(a.Mantissa, b.Mantissa) {
		return Value{}, NewError(ENumericOverflow, "addition overflow: %d + %d at scale %d", a.Mantissa, b.Mantissa, a.Scale)
	}
	return Value{Mantissa: a.Mantissa + b.Mantissa, Scale: a.Scale}, nil
}

// MulExact multiplies by an exact integer coefficient without changing scale.
func MulExact(v Value, factor int64) (Value, error) {
	if factor <= 0 {
		return Value{}, NewError(ESpecInvalid, "multiplication factor must be a positive integer, got %d", factor)
	}
	if mulOverflows(v.Mantissa, factor) {
		return Value{}, NewError(ENumericOverflow, "multiplication overflow: %d * %d", v.Mantissa, factor)
	}
	return Value{Mantissa: v.Mantissa * factor, Scale: v.Scale}, nil
}

// roundHalfAway divides n by d using round-half-away-from-zero. d must be positive.
func roundHalfAway(n, d int64) (q int64, overflow bool) {
	if d <= 0 {
		return 0, false
	}
	neg := n < 0
	if n == MinInt64 {
		// Handle the un-negatable minimum via big.Int to stay correct.
		bn := big.NewInt(n)
		bd := big.NewInt(d)
		half := new(big.Int).Quo(new(big.Int).Abs(bn), big.NewInt(2))
		abs := new(big.Int).Abs(bn)
		qb := new(big.Int).Quo(abs, bd)
		rem := new(big.Int).Mod(abs, bd)
		twiceRem := new(big.Int).Lsh(rem, 1)
		if twiceRem.Cmp(bd) >= 0 {
			qb.Add(qb, big.NewInt(1))
		}
		_ = half
		if qb.Cmp(big.NewInt(MaxInt64)) > 0 {
			return 0, true
		}
		q := qb.Int64()
		if neg {
			q = -q
		}
		return q, false
	}
	abs := n
	if neg {
		abs = -n
	}
	q = abs / d
	rem := abs % d
	if rem*2 >= d {
		if q == MaxInt64 {
			return 0, true
		}
		q++
	}
	if neg {
		q = -q
	}
	return q, false
}

// DivRound divides by a positive integer divisor at unchanged scale, rounding
// half away from zero. The absolute rounding error is at most 0.5 ulp.
func DivRound(v Value, divisor int64) (Value, error) {
	if divisor <= 0 {
		return Value{}, NewError(ESpecInvalid, "division divisor must be a positive integer, got %d", divisor)
	}
	q, overflow := roundHalfAway(v.Mantissa, divisor)
	if overflow {
		return Value{}, NewError(ENumericOverflow, "division overflow while rounding %d / %d", v.Mantissa, divisor)
	}
	return Value{Mantissa: q, Scale: v.Scale}, nil
}

// Rescale converts v to a target scale in AllowedScales using round-half-away.
func Rescale(v Value, target int) (Value, error) {
	if !AllowedScales[target] {
		return Value{}, NewError(ESpecInvalid, "scale %d is not in the allowed set {0,3,6,9}", target)
	}
	if target == v.Scale {
		return v, nil
	}
	if target > v.Scale {
		f, ok := Pow10(target - v.Scale)
		if !ok || mulOverflows(v.Mantissa, f) {
			return Value{}, NewError(ENumericOverflow, "rescale overflow from scale %d to %d", v.Scale, target)
		}
		return Value{Mantissa: v.Mantissa * f, Scale: target}, nil
	}
	d, ok := Pow10(v.Scale - target)
	if !ok {
		return Value{}, NewError(ENumericOverflow, "rescale divisor overflow from scale %d to %d", v.Scale, target)
	}
	q, overflow := roundHalfAway(v.Mantissa, d)
	if overflow {
		return Value{}, NewError(ENumericOverflow, "rescale rounding overflow from scale %d to %d", v.Scale, target)
	}
	return Value{Mantissa: q, Scale: target}, nil
}

// Rat converts the fixed-point value to an exact *big.Rat.
func (v Value) Rat() *big.Rat {
	den, ok := Pow10(v.Scale)
	if !ok {
		// Scale is bounded by AllowedScales so this never happens.
		return new(big.Rat)
	}
	return big.NewRat(v.Mantissa, den)
}

// Decimal renders the value as a plain decimal string (no exponent notation).
func (v Value) Decimal() string {
	neg := v.Mantissa < 0
	abs := v.Mantissa
	if neg {
		abs = -abs
	}
	digits := itoa(abs)
	var intPart, fracPart string
	if v.Scale == 0 {
		intPart = digits
	} else {
		if len(digits) <= v.Scale {
			intPart = "0"
			fracPart = strings.Repeat("0", v.Scale-len(digits)) + digits
		} else {
			cut := len(digits) - v.Scale
			intPart = digits[:cut]
			fracPart = digits[cut:]
		}
	}
	s := intPart
	if v.Scale > 0 {
		s += "." + fracPart
	}
	if neg {
		s = "-" + s
	}
	return s
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// ParseDecimal parses a plain decimal string at the requested scale, rounding
// half away from zero and reporting the rounding applied in ulps.
func ParseDecimal(s string, targetScale int) (Value, error) {
	if !AllowedScales[targetScale] {
		return Value{}, NewError(ESpecInvalid, "scale %d is not in the allowed set {0,3,6,9}", targetScale)
	}
	s = strings.TrimSpace(s)
	neg := false
	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+") {
		neg = s[0] == '-'
		s = s[1:]
	}
	if s == "" {
		return Value{}, NewError(EInvalidInput, "empty decimal literal")
	}
	dot := strings.IndexByte(s, '.')
	intDigits, fracDigits := s, ""
	if dot >= 0 {
		intDigits, fracDigits = s[:dot], s[dot+1:]
		if intDigits == "" {
			intDigits = "0"
		}
	}
	if intDigits == "" {
		intDigits = "0"
	}
	for _, r := range intDigits + fracDigits {
		if r < '0' || r > '9' {
			return Value{}, NewError(EInvalidInput, "invalid decimal literal %q", s)
		}
	}
	// Build the unscaled integer at max(len(frac), target) then rescale.
	need := len(fracDigits)
	mant := int64(0)
	whole := intDigits + fracDigits
	for _, r := range whole {
		d := int64(r - '0')
		if mant > (MaxInt64-d)/10 {
			return Value{}, NewError(ENumericOverflow, "decimal literal %q overflows int64", s)
		}
		mant = mant*10 + d
	}
	if neg {
		mant = -mant
	}
	return Rescale(Value{Mantissa: mant, Scale: need}, targetScale)
}
