package fixed

import (
	"fmt"
	"math/big"
	"strings"
)

const Scale int64 = 1_000_000

// MulDivHalfUp returns round-half-away-from-zero(a*b/denominator) without
// allowing an intermediate int64 overflow. The remainder is the exact residual
// numerator after rounding; callers use it to expose cumulative rounding error.
func MulDivHalfUp(a, b, denominator int64) (int64, *big.Int, error) {
	if denominator == 0 {
		return 0, nil, fmt.Errorf("division by zero")
	}
	numerator := new(big.Int).Mul(big.NewInt(a), big.NewInt(b))
	den := big.NewInt(denominator)
	quotient := new(big.Int)
	remainder := new(big.Int)
	quotient.QuoRem(numerator, den, remainder)
	if remainder.Sign() != 0 {
		absRem := new(big.Int).Abs(remainder)
		absRem.Mul(absRem, big.NewInt(2))
		if absRem.Cmp(new(big.Int).Abs(den)) >= 0 {
			if numerator.Sign() >= 0 {
				quotient.Add(quotient, big.NewInt(1))
			} else {
				quotient.Sub(quotient, big.NewInt(1))
			}
		}
	}
	if !quotient.IsInt64() {
		return 0, nil, fmt.Errorf("fixed-point result overflows int64")
	}
	return quotient.Int64(), remainder, nil
}

func MicrosFromMs(ms int64) (int64, error) {
	if ms > (1<<63-1)/1000 || ms < -(1<<63-1)/1000 {
		return 0, fmt.Errorf("milliseconds overflow fixed microseconds")
	}
	return ms * 1000, nil
}

func FormatMicros(micros int64) string {
	sign := ""
	if micros < 0 {
		sign = "-"
		micros = -micros
	}
	return fmt.Sprintf("%s%d.%06d", sign, micros/Scale, micros%Scale)
}

func FormatRatioPPM(ppm int64) string {
	return fmt.Sprintf("%d.%06d", ppm/Scale, ppm%Scale)
}

type BudgetComparison struct {
	CandidateMicros  int64  `json:"candidateMicros"`
	Candidate        string `json:"candidate"`
	LimitMicros      int64  `json:"limitMicros"`
	Limit            string `json:"limit"`
	TolerancePPM     int64  `json:"tolerancePpm"`
	ToleranceMicros  int64  `json:"toleranceMicros"`
	Tolerance        string `json:"tolerance"`
	DifferenceMicros int64  `json:"differenceMicros"`
	Decision         string `json:"decision"`
	WithinTolerance  bool   `json:"withinTolerance"`
	Explanation      string `json:"explanation"`
}

// CompareTimeBudget uses exact integer micros and a policy-declared relative
// tolerance. Values within the tolerance are accepted; callers break equal or
// near-equal priorities by batch index.
func CompareTimeBudget(candidate, limitMicros, tolerancePPM int64, batchIndex int) (BudgetComparison, error) {
	if candidate < 0 || limitMicros < 0 || tolerancePPM < 0 {
		return BudgetComparison{}, fmt.Errorf("budget values must be non-negative")
	}
	toleranceMicros, _, err := MulDivHalfUp(limitMicros, tolerancePPM, Scale)
	if err != nil {
		return BudgetComparison{}, err
	}
	if toleranceMicros > (1<<63-1)-limitMicros {
		return BudgetComparison{}, fmt.Errorf("time budget tolerance overflows int64")
	}
	allowedLimit := limitMicros + toleranceMicros
	diff := candidate - limitMicros
	decision := "accept"
	within := false
	explanation := "candidate is within the exact round time budget"
	if candidate > allowedLimit {
		decision = "skip"
		explanation = "candidate exceeds budget plus relative tolerance"
	} else if candidate > limitMicros {
		within = true
		explanation = fmt.Sprintf("candidate exceeds nominal budget but is inside tolerance; stable batch-index tie-break applies (batch %d)", batchIndex)
	}
	return BudgetComparison{
		CandidateMicros:  candidate,
		Candidate:        FormatMicros(candidate) + "ms",
		LimitMicros:      limitMicros,
		Limit:            FormatMicros(limitMicros) + "ms",
		TolerancePPM:     tolerancePPM,
		ToleranceMicros:  toleranceMicros,
		Tolerance:        FormatMicros(toleranceMicros) + "ms",
		DifferenceMicros: diff,
		Decision:         decision,
		WithinTolerance:  within,
		Explanation:      explanation,
	}, nil
}

// DecimalRelativeCompare uses exact rational arithmetic, never float64.
type DecimalRelativeComparison struct {
	Equal           bool   `json:"equal"`
	WithinTolerance bool   `json:"withinTolerance"`
	TolerancePPM    int64  `json:"tolerancePpm"`
	Tolerance       string `json:"tolerance"`
	AllowedDelta    string `json:"allowedDelta"`
	ActualDelta     string `json:"actualDelta"`
	Comparison      string `json:"comparison"`
}

func CompareDecimalRelative(expected, actual string, tolerancePPM int64) (DecimalRelativeComparison, error) {
	if tolerancePPM < 0 {
		return DecimalRelativeComparison{}, fmt.Errorf("negative tolerance")
	}
	e, ok := new(big.Rat).SetString(expected)
	if !ok {
		return DecimalRelativeComparison{}, fmt.Errorf("invalid decimal %q", expected)
	}
	a, ok := new(big.Rat).SetString(actual)
	if !ok {
		return DecimalRelativeComparison{}, fmt.Errorf("invalid decimal %q", actual)
	}
	if e.Cmp(a) == 0 {
		return DecimalRelativeComparison{
			Equal:           true,
			WithinTolerance: true,
			TolerancePPM:    tolerancePPM,
			Tolerance:       fmt.Sprintf("%d ppm relative", tolerancePPM),
			AllowedDelta:    "0",
			ActualDelta:     "0",
			Comparison:      "exact_equal",
		}, nil
	}
	diff := new(big.Rat).Sub(e, a)
	if diff.Sign() < 0 {
		diff.Neg(diff)
	}
	absE := new(big.Rat).Abs(e)
	absA := new(big.Rat).Abs(a)
	base := absE
	if absA.Cmp(base) > 0 {
		base = absA
	}
	allowed := new(big.Rat).Mul(base, new(big.Rat).SetFrac64(tolerancePPM, Scale))
	within := diff.Cmp(allowed) <= 0
	comparison := "outside_relative_tolerance"
	if within {
		comparison = "inside_relative_tolerance"
	}
	return DecimalRelativeComparison{
		Equal:           false,
		WithinTolerance: within,
		TolerancePPM:    tolerancePPM,
		Tolerance:       fmt.Sprintf("%d ppm relative", tolerancePPM),
		AllowedDelta:    formatRat(allowed),
		ActualDelta:     formatRat(diff),
		Comparison:      comparison,
	}, nil
}

func formatRat(r *big.Rat) string {
	if r == nil {
		return "0"
	}
	if r.IsInt() {
		return r.Num().String()
	}
	s := r.FloatString(18)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "-0" || s == "" {
		return "0"
	}
	return s
}
