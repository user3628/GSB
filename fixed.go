package tokenforge

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Scale is the fixed-point denominator used by every monetary and quota value.
// An int64 value of 1 represents 1e-9 of the base unit.
const Scale int64 = 1_000_000_000

// Fixed is a scale=1e9 signed fixed-point value. The component never converts
// monetary values to float64 or float32.
type Fixed int64

func (f Fixed) Int64() int64 { return int64(f) }

// String renders a Fixed value without using floating point. The returned
// representation is stable and suitable for logs and reports.
func (f Fixed) String() string {
	n := int64(f)
	sign := ""
	if n < 0 {
		sign = "-"
		if n == math.MinInt64 {
			// Avoid negating MinInt64. Format via the positive magnitude.
			u := uint64(math.MaxInt64) + 1
			whole := u / uint64(Scale)
			frac := u % uint64(Scale)
			if frac == 0 {
				return sign + strconv.FormatUint(whole, 10) + ".000000000"
			}
			return sign + strconv.FormatUint(whole, 10) + "." + leftPad9(int64(frac))
		}
		n = -n
	}
	whole := n / Scale
	frac := n % Scale
	if frac == 0 {
		return sign + strconv.FormatInt(whole, 10) + ".000000000"
	}
	return sign + strconv.FormatInt(whole, 10) + "." + leftPad9(frac)
}

func leftPad9(n int64) string {
	s := strconv.FormatInt(n, 10)
	if len(s) >= 9 {
		return s
	}
	return strings.Repeat("0", 9-len(s)) + s
}

// ParseFixed parses a decimal fixed-point string using integer arithmetic only.
func ParseFixed(s string) (Fixed, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, codeError(CodeInvalidArgument, "empty fixed-point value")
	}
	neg := false
	switch s[0] {
	case '-':
		neg = true
		s = s[1:]
	case '+':
		s = s[1:]
	}
	if s == "" {
		return 0, codeError(CodeInvalidArgument, "invalid fixed-point value")
	}
	parts := strings.Split(s, ".")
	if len(parts) > 2 || parts[0] == "" {
		return 0, codeError(CodeInvalidArgument, "invalid fixed-point value")
	}
	whole, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || whole < 0 {
		return 0, codeError(CodeInvalidArgument, "invalid fixed-point whole part")
	}
	if whole > math.MaxInt64/Scale {
		return 0, codeError(CodeQuotaOverflow, "fixed-point value overflows int64")
	}
	var frac int64
	if len(parts) == 2 {
		f := parts[1]
		if len(f) > 9 {
			return 0, codeError(CodeInvalidArgument, "fixed-point precision exceeds scale=1e9")
		}
		if f != "" {
			for len(f) < 9 {
				f += "0"
			}
			frac, err = strconv.ParseInt(f, 10, 64)
			if err != nil {
				return 0, codeError(CodeInvalidArgument, "invalid fixed-point fraction")
			}
		}
	}
	mag := whole*Scale + frac
	if neg {
		if mag == 0 {
			return 0, nil
		}
		return Fixed(-mag), nil
	}
	return Fixed(mag), nil
}

// CheckedAdd returns a+b only when the signed int64 addition cannot overflow.
func CheckedAdd(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, codeError(CodeQuotaOverflow, "int64 addition overflow")
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, codeError(CodeQuotaOverflow, "int64 addition underflow")
	}
	return a + b, nil
}

// CheckedMul returns a*b only when the signed int64 multiplication cannot
// overflow. Monetary inputs are required to be non-negative.
func CheckedMul(a, b int64) (int64, error) {
	if a < 0 || b < 0 {
		return 0, codeError(CodeInvalidArgument, "negative monetary multiplication")
	}
	if a != 0 && b > math.MaxInt64/a {
		return 0, codeError(CodeQuotaOverflow, "int64 multiplication overflow")
	}
	return a * b, nil
}

// MulDivScale computes floor((unitCost * usage) / Scale) and returns the
// division remainder. Both multiplication and the caller's later add operations
// can therefore be checked before any ledger write occurs.
func MulDivScale(unitCost, usage int64) (product, charge, remainder int64, err error) {
	if unitCost < 0 || usage < 0 {
		return 0, 0, 0, codeError(CodeInvalidArgument, "negative fixed-point charge input")
	}
	product, err = CheckedMul(unitCost, usage)
	if err != nil {
		return 0, 0, 0, err
	}
	return product, product / Scale, product % Scale, nil
}

// ErrorBoundFixedUpper converts accumulated sub-scale truncation remainders to
// an inclusive upper bound in fixed-point integer units. Each ledger row loses
// less than one integer unit, so ceil(sumRemainders/Scale) is a safe aggregate
// bound. Overflow is checked before the rounding addition.
func ErrorBoundFixedUpper(sumRemainders int64) (int64, error) {
	if sumRemainders < 0 {
		return 0, codeError(CodeInvalidArgument, "negative truncation remainder")
	}
	if sumRemainders == 0 {
		return 0, nil
	}
	n, err := CheckedAdd(sumRemainders, Scale-1)
	if err != nil {
		return 0, err
	}
	return n / Scale, nil
}

func validateFixedNonNegative(name string, v int64) error {
	if v < 0 {
		return codeError(CodeInvalidArgument, fmt.Sprintf("%s must be non-negative", name))
	}
	return nil
}
