package numeric

import (
	"errors"
	"fmt"
	"math/big"
	"sort"
)

const (
	QuantileNearestRank = "nearest_rank"
	QuantileLinear      = "linear"
)

var ErrQuantileInsufficient = errors.New("insufficient samples for requested quantile")

// Quantile computes a quantile from integer microsecond samples. The input is
// copied and sorted; callers can therefore pass batches in any order.
func Quantile(values []int64, qPerMille int64, method string) (int64, RoundingStep, error) {
	if qPerMille < 1 || qPerMille > 1000 {
		return 0, RoundingStep{}, fmt.Errorf("quantile must be in [1,1000]")
	}
	if len(values) == 0 {
		return 0, RoundingStep{}, ErrQuantileInsufficient
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	n := int64(len(sorted))

	switch method {
	case QuantileNearestRank:
		// ceil(q*n/1000), with integer arithmetic.
		rankNum := new(big.Int).Mul(big.NewInt(qPerMille), big.NewInt(n))
		q, r := new(big.Int).QuoRem(rankNum, big.NewInt(1000), new(big.Int))
		if r.Sign() != 0 {
			q.Add(q, big.NewInt(1))
		}
		idx := q.Int64() - 1
		if idx < 0 || idx >= n {
			return 0, RoundingStep{}, ErrQuantileInsufficient
		}
		out := sorted[idx]
		step := MakeStep("latency.quantile", big.NewInt(out), big.NewInt(1), 1, out, "microseconds")
		step.Operation = "nearest_rank_exact_integer"
		return out, step, nil

	case QuantileLinear:
		if n < 2 {
			return 0, RoundingStep{}, ErrQuantileInsufficient
		}
		// position = q*(n-1)/1000 in zero-based sample units.
		pos := new(big.Int).Mul(big.NewInt(qPerMille), big.NewInt(n-1))
		lower := new(big.Int).Quo(pos, big.NewInt(1000))
		rem := new(big.Int).Rem(pos, big.NewInt(1000)).Int64()
		li := lower.Int64()
		if rem == 0 {
			out := sorted[li]
			step := MakeStep("latency.quantile", big.NewInt(out), big.NewInt(1), 1, out, "microseconds")
			step.Operation = "linear_interpolation_exact_integer"
			return out, step, nil
		}
		ui := li + 1
		if ui >= n {
			ui = n - 1
		}
		// exact microseconds numerator = lo*(1000-rem) + hi*rem, den=1000
		num := new(big.Int).Mul(big.NewInt(sorted[li]), big.NewInt(1000-rem))
		num.Add(num, new(big.Int).Mul(big.NewInt(sorted[ui]), big.NewInt(rem)))
		den := big.NewInt(1000)
		rr, err := RoundHalfEvenRatio(num, den, 1)
		if err != nil {
			return 0, RoundingStep{}, err
		}
		step := MakeStep("latency.quantile", num, den, 1, rr.Output, "microseconds")
		step.Operation = "linear_interpolation_round_half_even_exact_rational"
		return rr.Output, step, nil
	default:
		return 0, RoundingStep{}, fmt.Errorf("unsupported quantile method %q", method)
	}
}
