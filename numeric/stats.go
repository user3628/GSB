package numeric

import (
	"errors"
	"math"
	"math/big"
)

var ErrInsufficientSamples = errors.New("insufficient samples for requested confidence interval")

// SuccessRatePPM returns an exact-rational half-even rounded rate and its audit
// step. A rate is represented once, at 1e-6 precision.
func SuccessRatePPM(successes, samples int64) (int64, RoundingStep, error) {
	if samples <= 0 || successes < 0 || successes > samples {
		return 0, RoundingStep{}, ErrInsufficientSamples
	}
	num := big.NewInt(successes)
	den := big.NewInt(samples)
	rr, err := RoundHalfEvenRatio(num, den, PPMScaled)
	if err != nil {
		return 0, RoundingStep{}, err
	}
	return rr.Output, MakeStep("success.rate", num, den, PPMScaled, rr.Output, "ppm"), nil
}

// DifferencePPM returns the signed canary-minus-stable difference once, from
// exact group counts. Displayed rates are never subtracted.
func DifferencePPM(canarySuccesses, canarySamples, stableSuccesses, stableSamples int64) (int64, RoundingStep, error) {
	if canarySamples <= 0 || stableSamples <= 0 || canarySuccesses < 0 || stableSuccesses < 0 ||
		canarySuccesses > canarySamples || stableSuccesses > stableSamples {
		return 0, RoundingStep{}, ErrInsufficientSamples
	}
	// difference = cs/cn - ss/sn = (cs*sn - ss*cn)/(cn*sn)
	num := new(big.Int).Mul(big.NewInt(canarySuccesses), big.NewInt(stableSamples))
	num.Sub(num, new(big.Int).Mul(big.NewInt(stableSuccesses), big.NewInt(canarySamples)))
	den := new(big.Int).Mul(big.NewInt(canarySamples), big.NewInt(stableSamples))
	rr, err := RoundHalfEvenRatio(num, den, PPMScaled)
	if err != nil {
		return 0, RoundingStep{}, err
	}
	return rr.Output, MakeStep("success.absolute_difference", num, den, PPMScaled, rr.Output, "ppm"), nil
}

// RelativeDifferencePPM returns (canary_rate-stable_rate)/stable_rate.
func RelativeDifferencePPM(canarySuccesses, canarySamples, stableSuccesses, stableSamples int64) (int64, RoundingStep, error) {
	if canarySamples <= 0 || stableSamples <= 0 || canarySuccesses < 0 || stableSuccesses < 0 ||
		canarySuccesses > canarySamples || stableSuccesses > stableSamples || stableSuccesses == 0 {
		return 0, RoundingStep{}, ErrInsufficientSamples
	}
	// ((cs*sn)/(cn*sn) - ss/sn) / (ss/sn)
	// = (cs*sn - ss*cn) / (ss*cn)
	num := new(big.Int).Mul(big.NewInt(canarySuccesses), big.NewInt(stableSamples))
	num.Sub(num, new(big.Int).Mul(big.NewInt(stableSuccesses), big.NewInt(canarySamples)))
	den := new(big.Int).Mul(big.NewInt(stableSuccesses), big.NewInt(canarySamples))
	rr, err := RoundHalfEvenRatio(num, den, PPMScaled)
	if err != nil {
		return 0, RoundingStep{}, err
	}
	return rr.Output, MakeStep("success.relative_difference", num, den, PPMScaled, rr.Output, "ppm"), nil
}

// ZScorePPM returns the two-sided normal quantile for a central confidence
// level. For example 9500 (95%) returns z=1.959964.
func ZScorePPM(confidencePPM int64) (int64, error) {
	if confidencePPM <= 0 || confidencePPM >= 10000 {
		return 0, errors.New("confidence_level_ppm must be in (0,10000)")
	}
	p := 0.5 + float64(confidencePPM)/20000.0
	z := inverseNormal(p)
	return RoundHalfEvenFloat(z * float64(PPMScaled)), nil
}

// WaldHalfWidthPPM computes z*sqrt(pc(1-pc)/nc + ps(1-ps)/ns)*1e6.
// Each group needs at least two samples because a confidence interval on a
// proportion is undefined from a single observation.
func WaldHalfWidthPPM(canarySuccesses, canarySamples, stableSuccesses, stableSamples, zPPM int64) (int64, error) {
	if canarySamples < 2 || stableSamples < 2 || canarySuccesses < 0 || stableSuccesses < 0 ||
		canarySuccesses > canarySamples || stableSuccesses > stableSamples {
		return 0, ErrInsufficientSamples
	}
	cn := big.NewInt(canarySamples)
	sn := big.NewInt(stableSamples)
	cs := big.NewInt(canarySuccesses)
	ss := big.NewInt(stableSuccesses)
	cn3 := new(big.Int).Mul(cn, new(big.Int).Mul(cn, cn))
	sn3 := new(big.Int).Mul(sn, new(big.Int).Mul(sn, sn))
	termC := new(big.Int).Mul(cs, new(big.Int).Sub(cn, cs))
	termC.Mul(termC, sn3)
	termS := new(big.Int).Mul(ss, new(big.Int).Sub(sn, ss))
	termS.Mul(termS, cn3)
	numerator := new(big.Int).Add(termC, termS)
	if numerator.Sign() == 0 {
		return 0, nil
	}
	denominator := new(big.Int).Mul(cn3, sn3)
	return RoundHalfEvenSqrt(zPPM, numerator, denominator)
}

// RuntimeErrorBoundPPM is deliberately computed from sample sizes at runtime:
// z*sqrt(0.25/n_canary + 0.25/n_stable). It is not a compile-time constant.
func RuntimeErrorBoundPPM(canarySamples, stableSamples, zPPM int64) (int64, error) {
	if canarySamples < 2 || stableSamples < 2 {
		return 0, ErrInsufficientSamples
	}
	numerator := big.NewInt(canarySamples + stableSamples)
	denominator := new(big.Int).Mul(big.NewInt(4), new(big.Int).Mul(big.NewInt(canarySamples), big.NewInt(stableSamples)))
	return RoundHalfEvenSqrt(zPPM, numerator, denominator)
}

// RoundHalfEvenSqrt returns round_half_even(zPPM * sqrt(numerator/denominator))
// without floating point. It is used for confidence intervals and the runtime
// error bound so results are identical across operating systems.
func RoundHalfEvenSqrt(zPPM int64, numerator, denominator *big.Int) (int64, error) {
	if zPPM < 0 || numerator.Sign() < 0 || denominator.Sign() <= 0 {
		return 0, errors.New("sqrt inputs out of range")
	}
	if numerator.Sign() == 0 || zPPM == 0 {
		return 0, nil
	}
	z2 := new(big.Int).Mul(big.NewInt(zPPM), big.NewInt(zPPM))
	a := new(big.Int).Mul(z2, numerator)
	ratio := new(big.Int).Quo(a, denominator)
	q := new(big.Int).Sqrt(ratio)
	lhs := new(big.Int).Lsh(a, 2)
	twoQPlusOne := new(big.Int).Lsh(q, 1)
	twoQPlusOne.Add(twoQPlusOne, big.NewInt(1))
	rhs := new(big.Int).Mul(twoQPlusOne, twoQPlusOne)
	rhs.Mul(rhs, denominator)
	cmp := lhs.Cmp(rhs)
	if cmp > 0 || (cmp == 0 && q.Bit(0) == 1) {
		q.Add(q, big.NewInt(1))
	}
	if !q.IsInt64() {
		return 0, ErrIntegerRange
	}
	return q.Int64(), nil
}

// ThresholdWithTolerance applies a relative ppm tolerance to a non-negative
// gate threshold with integer half-even rounding. A measurement is considered
// inside the threshold when it is <= this returned limit.
func ThresholdWithTolerance(thresholdPPM, tolerancePPM int64) (limit, toleranceApplied int64, err error) {
	if thresholdPPM < 0 || tolerancePPM < 0 {
		return 0, 0, errors.New("threshold and tolerance must be non-negative")
	}
	product := new(big.Int).Mul(big.NewInt(thresholdPPM), big.NewInt(tolerancePPM))
	rr, err := RoundHalfEvenRatio(product, big.NewInt(PPMScaled), 1)
	if err != nil {
		return 0, 0, err
	}
	total := new(big.Int).Add(big.NewInt(thresholdPPM), big.NewInt(rr.Output))
	if !total.IsInt64() {
		return 0, 0, ErrIntegerRange
	}
	return total.Int64(), rr.Output, nil
}

// inverseNormal implements Acklam's rational approximation. It is used only to
// obtain a stable six-decimal z-score; resampling and error bounds remain
// count-driven and exact.
func inverseNormal(p float64) float64 {
	if p <= 0 {
		return math.Inf(-1)
	}
	if p >= 1 {
		return math.Inf(1)
	}
	a := [...]float64{-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00}
	b := [...]float64{-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01}
	c := [...]float64{-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00}
	d := [...]float64{7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00}
	const plow = 0.02425
	const phigh = 1 - plow
	var q, r float64
	if p < plow {
		q = math.Sqrt(-2 * math.Log(p))
		return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q + c[5]) /
			((((d[0]*q+d[1])*q+d[2])*q+d[3])*q + 1)
	}
	if p > phigh {
		q = math.Sqrt(-2 * math.Log(1-p))
		return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q + c[5]) /
			((((d[0]*q+d[1])*q+d[2])*q+d[3])*q + 1)
	}
	q = p - 0.5
	r = q * q
	return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r + a[5]) * q /
		(((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r + 1)
}

// CompensatedCount is an exact integer accumulator with a Neumaier-style
// compensation slot. Integer count addition is exact, so the slot remains zero
// unless overflow is detected; exposing it keeps the contract explicit and
// leaves room for weighted embodiments without changing the wire format.
type CompensatedCount struct {
	Sum          int64
	Compensation int64
}

func (c *CompensatedCount) Add(v int64) error {
	if v < 0 {
		return errors.New("count must be non-negative")
	}
	if c.Sum > math.MaxInt64-v {
		return errors.New("count overflow")
	}
	c.Sum += v
	return nil
}
