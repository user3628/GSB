package ledger

import (
	"math"
	"strconv"
)

type neumaierAccumulator struct {
	sum    float64
	c      float64
	sumAbs float64
	n      int64
}

func (a *neumaierAccumulator) Add(x float64) {
	t := a.sum + x
	if math.Abs(a.sum) >= math.Abs(x) {
		a.c += (a.sum - t) + x
	} else {
		a.c += (x - t) + a.sum
	}
	a.sum = t
	a.sumAbs += math.Abs(x)
	a.n++
}

func (a *neumaierAccumulator) Result() CompensatedSum {
	value := a.sum + a.c
	u := math.Pow(2, -53)
	gamma := 0.0
	if a.n > 0 {
		nu := float64(a.n) * u
		if nu < 1 {
			gamma = nu / (1 - nu)
		} else {
			gamma = math.Inf(1)
		}
	}
	bound := gamma * a.sumAbs
	relBound := 0.0
	if math.Abs(value) > 0 {
		relBound = bound / math.Abs(value) * 1_000_000
	}
	return CompensatedSum{
		Method:                   "Neumaier compensated float64 summation; adjudication uses exact int64 arithmetic",
		ValueMicros:              fixedFloat(value, 6),
		NaiveValueMicros:         fixedFloat(a.sum, 6),
		CompensationMicros:       fixedFloat(a.c, 6),
		AbsoluteErrorBoundMicros: fixedFloat(bound, 6),
		RelativeErrorBoundPPM:    fixedFloat(relBound, 9),
	}
}

func fixedFloat(v float64, decimals int) string {
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return "unbounded"
	}
	return strconv.FormatFloat(v, 'f', decimals, 64)
}
