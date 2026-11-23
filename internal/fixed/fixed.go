// Package fixed implements the component's only numeric representation for
// ratios: non-negative (and, for inputs, optionally signed) integers scaled by
// 10^6. No floating point value is used anywhere in this package.
package fixed

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
)

const Scale = 6

var scaleFactor = new(big.Int).Exp(big.NewInt(10), big.NewInt(Scale), nil)

var (
	ErrInvalidDecimal = errors.New("invalid fixed-point decimal")
	ErrDivisionByZero = errors.New("fixed-point division by zero")
)

func ScaleFactor() *big.Int { return new(big.Int).Set(scaleFactor) }

// ParseDecimal parses a decimal into fixed scale units. Digits beyond the
// declared scale are rounded half away from zero. E-notation is intentionally
// rejected so request semantics remain unambiguous.
func ParseDecimal(input string) (*big.Int, error) {
	s := strings.TrimSpace(input)
	if s == "" {
		return nil, ErrInvalidDecimal
	}
	sign := 1
	if s[0] == '+' || s[0] == '-' {
		if s[0] == '-' {
			sign = -1
		}
		s = s[1:]
	}
	if s == "" || strings.ContainsAny(s, "eE") {
		return nil, ErrInvalidDecimal
	}
	parts := strings.Split(s, ".")
	if len(parts) > 2 {
		return nil, ErrInvalidDecimal
	}
	intPart := parts[0]
	fracPart := ""
	if len(parts) == 2 {
		fracPart = parts[1]
	}
	if intPart == "" && fracPart == "" {
		return nil, ErrInvalidDecimal
	}
	if !allDigits(intPart) || !allDigits(fracPart) {
		return nil, ErrInvalidDecimal
	}
	if intPart == "" {
		intPart = "0"
	}

	units := new(big.Int)
	if _, ok := units.SetString(intPart, 10); !ok {
		return nil, ErrInvalidDecimal
	}
	units.Mul(units, scaleFactor)

	if len(fracPart) <= Scale {
		padded := fracPart + strings.Repeat("0", Scale-len(fracPart))
		if padded != "" {
			frac := new(big.Int)
			frac.SetString(padded, 10)
			units.Add(units, frac)
		}
	} else {
		keep := fracPart[:Scale]
		frac := new(big.Int)
		if keep != "" {
			frac.SetString(keep, 10)
			units.Add(units, frac)
		}
		nextDigit := fracPart[Scale] - '0'
		if nextDigit >= 5 {
			units.Add(units, big.NewInt(1))
		}
	}
	if sign < 0 {
		units.Neg(units)
	}
	return units, nil
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Format renders scale units with exactly Scale fractional digits.
func Format(units *big.Int) string {
	if units == nil {
		return "0.000000"
	}
	negative := units.Sign() < 0
	abs := new(big.Int).Abs(new(big.Int).Set(units))
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(abs, scaleFactor, r)
	frac := r.String()
	if len(frac) < Scale {
		frac = strings.Repeat("0", Scale-len(frac)) + frac
	}
	s := q.String() + "." + frac
	if negative {
		return "-" + s
	}
	return s
}

// DivRound divides num/den, multiplies by 10^Scale and rounds half away from
// zero. It returns the rounded units plus the exact division remainder. The
// remainder is expressed against the scaled numerator denominator.
func DivRound(num, den *big.Int) (units, remainder *big.Int, err error) {
	if den == nil || den.Sign() == 0 {
		return nil, nil, ErrDivisionByZero
	}
	scaled := new(big.Int).Mul(new(big.Int).Set(num), scaleFactor)
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(scaled, den, r)
	twice := new(big.Int).Lsh(new(big.Int).Abs(new(big.Int).Set(r)), 1)
	if twice.Cmp(new(big.Int).Abs(new(big.Int).Set(den))) >= 0 {
		if scaled.Sign()*den.Sign() >= 0 {
			q.Add(q, big.NewInt(1))
		} else {
			q.Sub(q, big.NewInt(1))
		}
	}
	return q, r, nil
}

func Ratio(num, den *big.Int) (*big.Int, *big.Int, error) {
	return DivRound(num, den)
}

// RoundQuotient rounds the unscaled integer division num/den half away from
// zero. It is used to consume accumulated division residuals without applying
// the fixed-point scale a second time.
func RoundQuotient(num, den *big.Int) (*big.Int, error) {
	if den == nil || den.Sign() == 0 {
		return nil, ErrDivisionByZero
	}
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(new(big.Int).Set(num), den, r)
	twice := new(big.Int).Lsh(new(big.Int).Abs(new(big.Int).Set(r)), 1)
	if twice.Cmp(new(big.Int).Abs(new(big.Int).Set(den))) >= 0 {
		if num.Sign()*den.Sign() >= 0 {
			q.Add(q, big.NewInt(1))
		} else {
			q.Sub(q, big.NewInt(1))
		}
	}
	return q, nil
}

// CeilDiv returns ceil(num/den) for a positive denominator.
func CeilDiv(num, den *big.Int) (*big.Int, error) {
	if den == nil || den.Sign() <= 0 {
		return nil, ErrDivisionByZero
	}
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(new(big.Int).Set(num), den, r)
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	return q, nil
}

// RelativeToleranceLower widens a lower bound by relativeTolerance (scale
// units). The widening is rounded upward so values exactly inside the declared
// tolerance are treated as satisfying the contract.
func RelativeToleranceLower(bound, relativeTolerance *big.Int) (*big.Int, error) {
	if relativeTolerance.Sign() < 0 {
		return nil, fmt.Errorf("negative tolerance")
	}
	delta, err := CeilDiv(new(big.Int).Mul(new(big.Int).Abs(new(big.Int).Set(bound)), relativeTolerance), scaleFactor)
	if err != nil {
		return nil, err
	}
	return new(big.Int).Sub(new(big.Int).Set(bound), delta), nil
}

// RelativeToleranceUpper widens an upper bound by relativeTolerance (scale
// units), rounded upward for an inclusive comparison.
func RelativeToleranceUpper(bound, relativeTolerance *big.Int) (*big.Int, error) {
	if relativeTolerance.Sign() < 0 {
		return nil, fmt.Errorf("negative tolerance")
	}
	delta, err := CeilDiv(new(big.Int).Mul(new(big.Int).Abs(new(big.Int).Set(bound)), relativeTolerance), scaleFactor)
	if err != nil {
		return nil, err
	}
	return new(big.Int).Add(new(big.Int).Set(bound), delta), nil
}

// CompensatedSum is an arbitrary-precision compensated accumulator. Integer
// inputs have no representation loss, so the compensation is zero; the same
// type also carries scaled division residuals in WeightedAverage. This is the
// integer-exact equivalent of Kahan summation and never introduces a float.
type CompensatedSum struct {
	Total        *big.Int
	Compensation *big.Int
}

func NewCompensatedSum() *CompensatedSum {
	return &CompensatedSum{Total: new(big.Int), Compensation: new(big.Int)}
}

func (s *CompensatedSum) Add(x *big.Int) {
	y := new(big.Int).Sub(new(big.Int).Set(x), s.Compensation)
	t := new(big.Int).Add(s.Total, y)
	s.Compensation.Sub(new(big.Int).Sub(t, s.Total), y)
	s.Total.Set(t)
}

func (s *CompensatedSum) Value() *big.Int { return new(big.Int).Set(s.Total) }

// AverageTerm describes one independently rounded share in a weighted average.
type AverageTerm struct {
	Weight      *big.Int
	ValueUnits  *big.Int
	Quotient    *big.Int
	Remainder   *big.Int
	NaiveUnits  *big.Int
	Compensated *big.Int
}

// WeightedAverageResult contains both the exact implementation path and a
// deliberately naive per-term rounding path used only for diagnostics.
type WeightedAverageResult struct {
	Units                  *big.Int
	ExactWeightedNumerator *big.Int
	TotalWeight            *big.Int
	NaiveUnits             *big.Int
	NaiveErrorNumerator    *big.Int // (naiveUnits*totalWeight - exactNumerator)
	NaiveErrorUpperUnits   *big.Int // n/2, rounded upward as a bound
	CompensatedErrorUnits  *big.Int // always <= 1 unit after final rounding
	Terms                  []AverageTerm
}

// WeightedAverage computes round(sum(w[i]*v[i])/sum(w[i])) with exact big
// integers. The compensated path carries every division residual instead of
// discarding it after each term. That makes the result independent of the
// number of terms and of the order in which equal weights are supplied.
func WeightedAverage(weights, values []*big.Int) (*WeightedAverageResult, error) {
	if len(weights) != len(values) {
		return nil, errors.New("weight/value length mismatch")
	}
	totalWSum := NewCompensatedSum()
	exactSum := NewCompensatedSum()
	terms := make([]AverageTerm, 0, len(weights))
	quotientSum := new(big.Int)
	residualSum := new(big.Int)
	naive := new(big.Int)
	for i := range weights {
		if weights[i] == nil || values[i] == nil {
			return nil, errors.New("nil weight or value")
		}
		if weights[i].Sign() < 0 || values[i].Sign() < 0 {
			return nil, errors.New("weights and values must be non-negative")
		}
		totalWSum.Add(weights[i])
		exactSum.Add(new(big.Int).Mul(weights[i], values[i]))
	}
	totalW := totalWSum.Value()
	exact := exactSum.Value()
	if totalW.Sign() == 0 {
		zero := new(big.Int)
		return &WeightedAverageResult{
			Units: zero, ExactWeightedNumerator: new(big.Int), TotalWeight: new(big.Int),
			NaiveUnits: zero, NaiveErrorNumerator: new(big.Int), NaiveErrorUpperUnits: new(big.Int),
			CompensatedErrorUnits: new(big.Int), Terms: terms,
		}, nil
	}
	for i := range weights {
		prod := new(big.Int).Mul(weights[i], values[i])
		q, r := new(big.Int), new(big.Int)
		q.QuoRem(new(big.Int).Set(prod), totalW, r)
		qTerm, _ := RoundQuotient(prod, totalW)
		quotientSum.Add(quotientSum, q)
		residualSum.Add(residualSum, r)
		naive.Add(naive, qTerm)
		terms = append(terms, AverageTerm{
			Weight: new(big.Int).Set(weights[i]), ValueUnits: new(big.Int).Set(values[i]),
			Quotient: q, Remainder: r, NaiveUnits: qTerm,
			Compensated: new(big.Int).Set(qTerm),
		})
	}
	residualUnits, err := RoundQuotient(residualSum, totalW)
	if err != nil {
		return nil, err
	}
	compensated := new(big.Int).Add(quotientSum, residualUnits)
	exactResult, _ := RoundQuotient(exact, totalW)
	if compensated.Cmp(exactResult) != 0 {
		return nil, fmt.Errorf("compensated weighted sum mismatch: %s != %s", compensated, exactResult)
	}
	naiveError := new(big.Int).Sub(new(big.Int).Mul(naive, totalW), exact)
	naiveError.Abs(naiveError)
	upper, _ := CeilDiv(new(big.Int).Mul(big.NewInt(int64(len(weights))), big.NewInt(1)), big.NewInt(2))
	return &WeightedAverageResult{
		Units: compensated, ExactWeightedNumerator: exact, TotalWeight: totalW,
		NaiveUnits: naive, NaiveErrorNumerator: naiveError,
		NaiveErrorUpperUnits: upper, CompensatedErrorUnits: big.NewInt(1),
		Terms: terms,
	}, nil
}
