package agg

import (
	"math"

	"logagg/internal/errs"
)

type RoundingMode string

const (
	RoundHalfUp   RoundingMode = "half_up"
	RoundHalfEven RoundingMode = "half_even"
	RoundDown     RoundingMode = "down"
)

type Precision struct {
	InputScale  int          `json:"input_scale"`
	ResultScale int          `json:"result_scale"`
	Mode        RoundingMode `json:"rounding_mode"`
}

type RoundingStep struct {
	Step                 int    `json:"step"`
	Window               string `json:"window"`
	Group                int64  `json:"group"`
	InputRaw             int64  `json:"input_raw"`
	InputValue           string `json:"input_value"`
	FromScale            int    `json:"from_scale"`
	ToScale              int    `json:"to_scale"`
	Mode                 string `json:"mode"`
	RoundedRaw           int64  `json:"rounded_raw"`
	RoundedValue         string `json:"rounded_value"`
	ResidualRaw          int64  `json:"residual_raw_in_input_scale"`
	CumulativeErrorRaw   int64  `json:"cumulative_error_raw"`
	CumulativeErrorValue string `json:"cumulative_error_value"`
}

type ResultGroup struct {
	Group         int64  `json:"group"`
	Count         int64  `json:"count"`
	SumRaw        int64  `json:"sum_raw"`
	SumExact      string `json:"sum_exact"`
	SumRoundedRaw int64  `json:"sum_rounded_raw"`
	SumRounded    string `json:"sum_rounded"`
	MinRaw        int64  `json:"min_raw"`
	Min           string `json:"min"`
	MaxRaw        int64  `json:"max_raw"`
	Max           string `json:"max"`
}

type QueryResult struct {
	Precision                  Precision      `json:"precision"`
	Groups                     []ResultGroup  `json:"groups"`
	RoundingTrace              []RoundingStep `json:"rounding_trace"`
	CumulativeRoundingErrorRaw int64          `json:"cumulative_rounding_error_raw"`
	CumulativeRoundingError    string         `json:"cumulative_rounding_error"`
	MaxAbsRoundingErrorValue   string         `json:"max_abs_rounding_error_value"`
}

func Finalize(total *TotalSum, precision Precision) (*QueryResult, error) {
	if precision.Mode == "" {
		precision.Mode = RoundHalfUp
	}
	if precision.InputScale < 0 || precision.InputScale > 18 || precision.ResultScale < 0 || precision.ResultScale > 18 {
		return nil, errs.New(errs.EInvalidRequest, "scales must be between 0 and 18")
	}
	result := &QueryResult{Precision: precision, Groups: []ResultGroup{}, RoundingTrace: []RoundingStep{}}
	if total == nil {
		return result, nil
	}
	keys := SortedKeys(total.Groups)
	cumulative := int64(0)
	maxAbsResidual := int64(0)
	for stepIndex, key := range keys {
		acc := total.Groups[key]
		rounded, residual, modeUsed, err := RoundScale(acc.Sum, precision.InputScale, precision.ResultScale, precision.Mode)
		if err != nil {
			return nil, err.(*errs.Error).WithDetail("group", key)
		}
		nextCumulative, ok := addInt64(cumulative, residual)
		if !ok {
			return nil, errs.NumericOverflow("rounding-trace", key, "cumulative rounding error overflow")
		}
		cumulative = nextCumulative
		absResidual := residual
		if absResidual < 0 {
			if absResidual == math.MinInt64 {
				return nil, errs.NumericOverflow("rounding-trace", key, "rounding residual absolute overflow")
			}
			absResidual = -absResidual
		}
		if absResidual > maxAbsResidual {
			maxAbsResidual = absResidual
		}
		step := RoundingStep{
			Step:                 stepIndex + 1,
			Window:               "final-query-result",
			Group:                key,
			InputRaw:             acc.Sum,
			InputValue:           Decimal(acc.Sum, precision.InputScale),
			FromScale:            precision.InputScale,
			ToScale:              precision.ResultScale,
			Mode:                 modeUsed,
			RoundedRaw:           rounded,
			RoundedValue:         Decimal(rounded, precision.ResultScale),
			ResidualRaw:          residual,
			CumulativeErrorRaw:   cumulative,
			CumulativeErrorValue: Decimal(cumulative, precision.InputScale),
		}
		result.RoundingTrace = append(result.RoundingTrace, step)
		result.Groups = append(result.Groups, ResultGroup{
			Group:         key,
			Count:         acc.Count,
			SumRaw:        acc.Sum,
			SumExact:      Decimal(acc.Sum, precision.InputScale),
			SumRoundedRaw: rounded,
			SumRounded:    Decimal(rounded, precision.ResultScale),
			MinRaw:        acc.Min,
			Min:           Decimal(acc.Min, precision.InputScale),
			MaxRaw:        acc.Max,
			Max:           Decimal(acc.Max, precision.InputScale),
		})
	}
	result.CumulativeRoundingErrorRaw = cumulative
	result.CumulativeRoundingError = Decimal(cumulative, precision.InputScale)
	result.MaxAbsRoundingErrorValue = Decimal(maxAbsResidual, precision.InputScale)
	return result, nil
}

func RoundScale(value int64, fromScale, toScale int, mode RoundingMode) (rounded int64, residualInSource int64, usedMode string, err error) {
	if toScale >= fromScale {
		scaleDelta := toScale - fromScale
		factor := pow10Int64(scaleDelta)
		if factor == 0 {
			return 0, 0, "", errs.New(errs.ENumericOverflow, "scale factor overflow")
		}
		scaled, ok := mulInt64(value, factor)
		if !ok {
			return 0, 0, "", errs.NumericOverflow("rounding", 0, "widening scale overflows int64")
		}
		return scaled, 0, "exact_widening", nil
	}
	divisor := pow10Int64(fromScale - toScale)
	if divisor <= 0 {
		return 0, 0, "", errs.New(errs.ENumericOverflow, "rounding divisor overflow")
	}
	quotient := value / divisor
	remainder := value % divisor
	absRemainder := remainder
	negative := value < 0
	if absRemainder < 0 {
		absRemainder = -absRemainder
	}
	addOne := false
	switch mode {
	case RoundDown:
		usedMode = string(RoundDown)
	case RoundHalfUp, "":
		usedMode = string(RoundHalfUp)
		twice, ok := mulInt64(absRemainder, 2)
		if !ok {
			return 0, 0, "", errs.NumericOverflow("rounding", 0, "half-up comparison overflow")
		}
		addOne = twice >= divisor
	case RoundHalfEven:
		usedMode = string(RoundHalfEven)
		twice, ok := mulInt64(absRemainder, 2)
		if !ok {
			return 0, 0, "", errs.NumericOverflow("rounding", 0, "half-even comparison overflow")
		}
		if twice > divisor || (twice == divisor && quotient%2 != 0) {
			addOne = true
		}
	default:
		return 0, 0, "", errs.New(errs.EInvalidRequest, "unsupported rounding mode %q", mode)
	}
	if addOne {
		var ok bool
		if negative {
			quotient, ok = addInt64(quotient, -1)
			if !ok {
				return 0, 0, "", errs.NumericOverflow("rounding", 0, "negative rounding overflows")
			}
		} else {
			quotient, ok = addInt64(quotient, 1)
			if !ok {
				return 0, 0, "", errs.NumericOverflow("rounding", 0, "positive rounding overflows")
			}
		}
	}
	back, ok := mulInt64(quotient, divisor)
	if !ok {
		return 0, 0, "", errs.NumericOverflow("rounding", 0, "rounded value cannot be represented at input scale")
	}
	residual := value - back
	return quotient, residual, usedMode, nil
}

func NaiveFloatSum(values []int64, scale int) float64 {
	divisor := float64(pow10Int64(scale))
	sum := 0.0
	for _, value := range values {
		sum += float64(value) / divisor
	}
	return sum
}

func FloatAccumulationBound(values []int64, scale int) float64 {
	n := len(values)
	if n <= 1 {
		return 0
	}
	divisor := float64(pow10Int64(scale))
	absSum := 0.0
	for _, value := range values {
		absSum += math.Abs(float64(value) / divisor)
	}
	epsilon := math.Ldexp(1, -52)
	gamma := float64(n-1) * epsilon
	if gamma >= 1 {
		gamma = 1
	}
	return gamma * absSum / (1 - gamma)
}
