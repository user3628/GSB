package model

// This file implements explicit fixed-point arithmetic. No bare floating
// point is used anywhere in scoring. All scores are stored as milli-units
// (scale 1000, i.e. three decimal places). Per-step rounding mode and the
// cumulative rounding error (nano-units, scale 1e9) are recorded alongside
// every score.

// Fixed scales.
const (
	MilliScale = 1000
	NanoScale  = 1_000_000_000
	// TimeWindowMS is the proximity normalization window (30s).
	TimeWindowMS int64 = 30_000
	// BaseScoreMilli is the unscaled base contribution, 1000.000.
	BaseScoreMilli int64 = 1000 * MilliScale
)

// RoundHalfAwayFromZero divides numerator by denominator, rounding half away
// from zero. Both inputs are assumed non-negative in the scoring pipeline.
func RoundHalfAwayFromZero(num, den int64) (int64, error) {
	if den == 0 {
		return 0, ErrZeroDenominator
	}
	if num < 0 {
		v, err := RoundHalfAwayFromZero(-num, den)
		return -v, err
	}
	q := num / den
	r := num % den
	if r*2 >= den {
		q++
	}
	return q, nil
}

// RoundingErrorNano returns (exact*1e9 - rounded*1e6) for a division, i.e. the
// accumulated rounding error expressed in nano-units for the operation
// round(num/den) expressed at milli scale.
func RoundingErrorNano(num, den, roundedMilli int64) (int64, error) {
	if den == 0 {
		return 0, ErrZeroDenominator
	}
	// exact milli value * 1e6 == exact nano value = num*NanoScale/den.
	exactNano, err := RoundHalfAwayFromZero(num*NanoScale, den)
	if err != nil {
		return 0, err
	}
	return exactNano - roundedMilli*(NanoScale/MilliScale), nil
}

// HopDecayMilli computes round_half_away(1000000 * (1 - 0.15*hops)) at milli
// scale, with a floor of zero. Returns the component and its rounding error.
// 0.15 is represented exactly as the rational 150/1000.
func HopDecayMilli(hops int) (decayMilli, errNano int64) {
	if hops < 0 {
		hops = 0
	}
	h := int64(hops)
	// decay = 1_000_000 - 1_000_000*150*hops/1000 ; the rational term is exact
	// at milli scale because 1_000_000*150/1000 = 150_000 is an integer.
	sub := BaseScoreMilli * 150 * h / 1000
	decayMilli = BaseScoreMilli - sub
	if decayMilli < 0 {
		decayMilli = 0
	}
	return decayMilli, 0
}

// TimeProximityMilli computes round_half_away(1_000_000 * max(0, window-delta)/window)
// at milli scale and returns the component plus the rounding error in nano.
func TimeProximityMilli(deltaMS int64) (proxMilli, errNano int64, err error) {
	d := deltaMS
	if d < 0 {
		d = -d
	}
	if d >= TimeWindowMS {
		return 0, 0, nil
	}
	num := BaseScoreMilli * (TimeWindowMS - d)
	proxMilli, err = RoundHalfAwayFromZero(num, TimeWindowMS)
	if err != nil {
		return 0, 0, err
	}
	errNano, err = RoundingErrorNano(num, TimeWindowMS, proxMilli)
	if err != nil {
		return 0, 0, err
	}
	return proxMilli, errNano, nil
}

// ScoreWeights at milli scale: 60% hop decay, 40% time proximity.
const (
	WeightHopMilli  int64 = 600
	WeightTimeMilli int64 = 400
)

// ScoreResult carries a score and its rounding audit trail.
type ScoreResult struct {
	HopDecayMilli      int64
	TimeProximityMilli int64
	HopErrorNano       int64
	TimeErrorNano      int64
	FinalMilli         int64
	TotalErrorNano     int64
}

// ComputeScore combines hop decay and time proximity:
//
//	final = round(0.6*hopDecay + 0.4*timeProx)
//
// Both multiplications are exact at milli scale (weights are per-mille), the
// final division by 1000 uses round-half-away-from-zero and its rounding error
// is accumulated with the time-proximity rounding error.
func ComputeScore(hops int, deltaMS int64) (ScoreResult, error) {
	hopDecay, hopErr := HopDecayMilli(hops)
	prox, proxErr, err := TimeProximityMilli(deltaMS)
	if err != nil {
		return ScoreResult{}, err
	}
	num := hopDecay*WeightHopMilli + prox*WeightTimeMilli
	final, err := RoundHalfAwayFromZero(num, MilliScale)
	if err != nil {
		return ScoreResult{}, err
	}
	finalErr, err := RoundingErrorNano(num, MilliScale, final)
	if err != nil {
		return ScoreResult{}, err
	}
	return ScoreResult{
		HopDecayMilli:      hopDecay,
		TimeProximityMilli: prox,
		HopErrorNano:       hopErr,
		TimeErrorNano:      proxErr,
		FinalMilli:         final,
		TotalErrorNano:     hopErr + proxErr + finalErr,
	}, nil
}

// EquivalentWithTolerance compares two milli scores using a relative tolerance
// expressed in per-mille: |a-b| <= tolerance * max(|a|,|b|) / 1000. The
// comparison is purely integer; no floating point is involved.
func EquivalentWithTolerance(a, b, tolerancePerMille int64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	maxv := a
	if b > maxv {
		maxv = b
	}
	if maxv < 0 {
		maxv = -maxv
	}
	allowed, err := RoundHalfAwayFromZero(maxv*tolerancePerMille, 1000)
	if err != nil {
		return false
	}
	return diff <= allowed
}
