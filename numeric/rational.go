// Package numeric implements the deterministic, auditable arithmetic used by
// release gates. Final values are integers in fixed-point units. Exact
// rationals are retained only for audit trails and residual accounting.
package numeric

import (
	"errors"
	"math/big"
	"strings"
)

const (
	PPMScaled     int64 = 1_000_000
	DecimalPlaces       = 6
	AuditDecimals       = 9
	RoundingMode        = "round_half_even"
	CanonicalPath       = "integer counts -> exact rational ratios -> one final half-even rounding per output; displayed ratio rounding is never reused"
)

var (
	ErrInvalidScale = errors.New("scale must be positive")
	ErrIntegerRange = errors.New("rounded value is outside int64")
)

// RationalJSON is a lossless-enough JSON rendering of an exact rational. The
// numerator and denominator are authoritative; decimal is an audit aid.
type RationalJSON struct {
	Numerator   string `json:"numerator"`
	Denominator string `json:"denominator"`
	Decimal     string `json:"decimal"`
}

// RoundingStep records one and only one final rounding operation.
type RoundingStep struct {
	Name                          string       `json:"name"`
	Operation                     string       `json:"operation"`
	Numerator                     string       `json:"numerator"`
	Denominator                   string       `json:"denominator"`
	Scale                         int64        `json:"scale"`
	OutputUnit                    string       `json:"output_unit"`
	OutputScaled                  int64        `json:"output_scaled"`
	Residual                      RationalJSON `json:"residual"`
	RoundingErrorUpperBoundScaled RationalJSON `json:"rounding_error_upper_bound_scaled"`
}

// Audit collects all rounding steps for one metric family.
type Audit struct {
	Scale                             int64          `json:"scale"`
	DecimalPlaces                     int            `json:"decimal_places"`
	RoundingMode                      string         `json:"rounding_mode"`
	CanonicalPath                     string         `json:"canonical_path"`
	RoundingSteps                     []RoundingStep `json:"rounding_steps"`
	AccumulatedRoundingErrorScaled    RationalJSON   `json:"accumulated_rounding_error_scaled"`
	AccumulatedLatencyRoundingErrorUS RationalJSON   `json:"accumulated_latency_rounding_error_us"`
}

// AuditBuilder is intentionally mutable while a metric is being assembled.
type AuditBuilder struct {
	steps       []RoundingStep
	scaledError *big.Rat
	latencyErr  *big.Rat
}

func NewAuditBuilder() *AuditBuilder {
	return &AuditBuilder{scaledError: new(big.Rat), latencyErr: new(big.Rat)}
}

// Add records a step and accumulates |exact-output|. Unit "microseconds"
// contributes to the separate latency residual accumulator.
func (b *AuditBuilder) Add(step RoundingStep) {
	b.steps = append(b.steps, step)
	residual, ok := parseRational(step.Residual)
	if !ok {
		return
	}
	abs := new(big.Rat).Abs(residual)
	if step.OutputUnit == "microseconds" {
		b.latencyErr.Add(b.latencyErr, abs)
	} else {
		b.scaledError.Add(b.scaledError, abs)
	}
}

func (b *AuditBuilder) Build() Audit {
	steps := append([]RoundingStep(nil), b.steps...)
	return Audit{
		Scale:                             PPMScaled,
		DecimalPlaces:                     DecimalPlaces,
		RoundingMode:                      RoundingMode,
		CanonicalPath:                     CanonicalPath,
		RoundingSteps:                     steps,
		AccumulatedRoundingErrorScaled:    ToRationalJSON(b.scaledError),
		AccumulatedLatencyRoundingErrorUS: ToRationalJSON(b.latencyErr),
	}
}

func parseRational(r RationalJSON) (*big.Rat, bool) {
	n, ok := new(big.Int).SetString(r.Numerator, 10)
	if !ok {
		return nil, false
	}
	d, ok := new(big.Int).SetString(r.Denominator, 10)
	if !ok || d.Sign() == 0 {
		return nil, false
	}
	return new(big.Rat).SetFrac(n, d), true
}

// RoundResult is the result of a binary rational rounded half-to-even at a
// requested scale.
type RoundResult struct {
	Output   int64
	Residual *big.Rat // exact x - output/scale
}

// RoundHalfEvenRatio rounds num/den to an integer in units of 1/scale using
// round-half-to-even. It never uses floating point.
func RoundHalfEvenRatio(num, den *big.Int, scale int64) (RoundResult, error) {
	if scale <= 0 {
		return RoundResult{}, ErrInvalidScale
	}
	if den.Sign() == 0 {
		return RoundResult{}, errors.New("zero denominator")
	}
	if den.Sign() < 0 {
		num = new(big.Int).Neg(num)
		den = new(big.Int).Neg(den)
	}

	scaled := new(big.Int).Mul(num, big.NewInt(scale))
	absNum := new(big.Int).Abs(scaled)
	q, r := new(big.Int).QuoRem(absNum, den, new(big.Int))
	twice := new(big.Int).Lsh(r, 1)
	cmp := twice.Cmp(den)
	if cmp > 0 || (cmp == 0 && q.Bit(0) == 1) {
		q.Add(q, big.NewInt(1))
	}
	if scaled.Sign() < 0 {
		q.Neg(q)
	}
	if !q.IsInt64() {
		return RoundResult{}, ErrIntegerRange
	}

	// residual = num/den - output/scale
	left := new(big.Int).Mul(num, big.NewInt(scale))
	right := new(big.Int).Mul(q, den)
	resNum := new(big.Int).Sub(left, right)
	resDen := new(big.Int).Mul(den, big.NewInt(scale))
	residual := new(big.Rat).SetFrac(resNum, resDen)
	return RoundResult{Output: q.Int64(), Residual: residual}, nil
}

// RoundHalfEvenFloat is reserved for square roots, which cannot be expressed
// as integer ratios. Counts enter the operation exactly; only the final scaled
// integer is rounded.
func RoundHalfEvenFloat(x float64) int64 {
	if x >= 0 {
		return int64(roundToEvenFloat(x))
	}
	return -int64(roundToEvenFloat(-x))
}

func roundToEvenFloat(x float64) float64 {
	if x < 0 {
		x = -x
	}
	floor := float64(int64(x))
	frac := x - floor
	switch {
	case frac < 0.5:
		return floor
	case frac > 0.5:
		return floor + 1
	default:
		if int64(floor)%2 == 0 {
			return floor
		}
		return floor + 1
	}
}

// MakeStep builds a rounding audit step from an exact rational and a final
// fixed-point output. residual = exact - output/scale.
func MakeStep(name string, num, den *big.Int, scale, output int64, unit string) RoundingStep {
	resNumerator := new(big.Int).Mul(num, big.NewInt(scale))
	resNumerator.Sub(resNumerator, new(big.Int).Mul(big.NewInt(output), den))
	resDenominator := new(big.Int).Mul(den, big.NewInt(scale))
	residual := new(big.Rat).SetFrac(resNumerator, resDenominator)
	return RoundingStep{
		Name:                          name,
		Operation:                     "round_half_even_exact_rational",
		Numerator:                     num.String(),
		Denominator:                   den.String(),
		Scale:                         scale,
		OutputUnit:                    unit,
		OutputScaled:                  output,
		Residual:                      ToRationalJSON(residual),
		RoundingErrorUpperBoundScaled: ToRationalJSON(new(big.Rat).SetFrac64(1, 2*scale)),
	}
}

// ToRationalJSON returns a reduced exact rational plus a 9-place decimal.
func ToRationalJSON(r *big.Rat) RationalJSON {
	if r == nil {
		r = new(big.Rat)
	}
	r = new(big.Rat).Set(r)
	if r.Denom().Sign() < 0 {
		r.Neg(r)
	}
	decimal, _ := RoundHalfEvenRatio(r.Num(), r.Denom(), 1_000_000_000)
	return RationalJSON{
		Numerator:   r.Num().String(),
		Denominator: r.Denom().String(),
		Decimal:     FormatScaled(decimal.Output, 1_000_000_000, 9),
	}
}

// FormatScaled renders a fixed-point integer without floating point. It keeps
// trailing zeros so the declared precision is visible in every response.
func FormatScaled(value, scale int64, decimals int) string {
	if scale <= 0 {
		return "invalid-scale"
	}
	negative := value < 0
	abs := new(big.Int).SetInt64(value)
	abs.Abs(abs)
	s := abs.String()
	if decimals <= 0 {
		if negative {
			return "-" + s
		}
		return s
	}
	if len(s) <= decimals {
		s = strings.Repeat("0", decimals-len(s)+1) + s
	}
	cut := len(s) - decimals
	out := s[:cut] + "." + s[cut:]
	if negative {
		out = "-" + out
	}
	return out
}

func FormatPPM(value int64) string {
	return FormatScaled(value, PPMScaled, DecimalPlaces)
}
