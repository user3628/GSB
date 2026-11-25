package fixedpoint

import (
	"math/big"
)

// StepResult records one chain step: the int64 fixed-point operation, its
// rounding mode, single-step absolute error bound in ulps and the exact
// rational value after the step.
type StepResult struct {
	Index        int      `json:"index"`
	Op           string   `json:"op"`
	Factor       int64    `json:"factor"`
	Input        Value    `json:"input"`
	Output       Value    `json:"output"`
	Rounding     string   `json:"rounding"`
	StepErrorULP int64    `json:"step_error_ulp"`
	ExactAfter   *big.Rat `json:"-"`
}

// ChainResult is the full evaluation of a conversion chain.
type ChainResult struct {
	Final         Value        `json:"final"`
	Steps         []StepResult `json:"steps"`
	AccumErrorULP int64        `json:"accumulated_error_ulp"`
	// BoundRat is the exact rational absolute error bound: AccumErrorULP ulps
	// plus the residual uncertainty propagated from previous steps, expressed
	// as a fraction of one ulp at the final scale.
	BoundRat   *big.Rat `json:"-"`
	ExactFinal *big.Rat `json:"-"`
}

// mulStep applies an exact integer multiplication, propagating prior error.
func mulStep(in Value, exactIn *big.Rat, accULP int64, factor int64, index int) (Value, *big.Rat, int64, StepResult, error) {
	out, err := MulExact(in, factor)
	if err != nil {
		if ce, ok := err.(*Error); ok {
			return Value{}, nil, 0, StepResult{}, ce.WithStep(index)
		}
		return Value{}, nil, 0, StepResult{}, err
	}
	exactOut := new(big.Rat).Mul(exactIn, big.NewRat(factor, 1))
	// Existing uncertainty scales by the exact factor; no new rounding occurs.
	newAcc := accULP * factor
	step := StepResult{
		Index: index, Op: "mul", Factor: factor, Input: in, Output: out,
		Rounding: "exact", StepErrorULP: 0, ExactAfter: exactOut,
	}
	return out, exactOut, newAcc, step, nil
}

// divStep applies integer division with round-half-away, adding 0.5 ulp error.
func divStep(in Value, exactIn *big.Rat, accULP int64, divisor int64, index int) (Value, *big.Rat, int64, StepResult, error) {
	out, err := DivRound(in, divisor)
	if err != nil {
		if ce, ok := err.(*Error); ok {
			return Value{}, nil, 0, StepResult{}, ce.WithStep(index)
		}
		return Value{}, nil, 0, StepResult{}, err
	}
	exactOut := new(big.Rat).Quo(exactIn, big.NewRat(divisor, 1))
	step := StepResult{
		Index: index, Op: "div", Factor: divisor, Input: in, Output: out,
		// One division contributes at most 0.5 ulp of new rounding error.
		Rounding: "round_half_away", StepErrorULP: 1, ExactAfter: exactOut,
	}
	return out, exactOut, accULP, step, nil
}

// ChainStep is one declared conversion operation: multiply or divide by a
// positive integer coefficient.
type ChainStep struct {
	Op     string `json:"op"`
	Factor int64  `json:"factor"`
}

// EvalChain runs the declared chain purely in int64 fixed point and tracks the
// exact rational result alongside. The returned bound is the synthesized
// absolute error bound in final-scale units as an exact rational.
func EvalChain(start Value, chain []ChainStep) (*ChainResult, error) {
	cur := start
	exact := start.Rat()
	accULP := int64(0)
	// fracBound tracks the sub-ulp remainder of accumulated error (in ulps).
	fracBound := big.NewRat(0, 1)
	steps := make([]StepResult, 0, len(chain))
	for i, st := range chain {
		idx := i + 1
		switch st.Op {
		case "mul":
			var (
				step StepResult
				err  error
			)
			cur, exact, accULP, step, err = mulStep(cur, exact, accULP, st.Factor, idx)
			if err != nil {
				return nil, err
			}
			// Fractional remainder scales by factor too.
			fracBound.Mul(fracBound, big.NewRat(st.Factor, 1))
			steps = append(steps, step)
		case "div":
			var (
				step StepResult
				err  error
			)
			cur, exact, accULP, step, err = divStep(cur, exact, accULP, st.Factor, idx)
			if err != nil {
				return nil, err
			}
			// fracBound/divisor plus the new 0.5 ulp from this division.
			fracBound.Quo(fracBound, big.NewRat(st.Factor, 1))
			fracBound.Add(fracBound, big.NewRat(1, 2))
			// Normalize whole ulps out of the fractional component.
			whole := new(big.Int).Quo(fracBound.Num(), fracBound.Denom())
			if whole.Sign() > 0 {
				accULP += whole.Int64()
				fracBound.Sub(fracBound, new(big.Rat).SetInt(whole))
			}
			steps = append(steps, step)
		default:
			return nil, NewError(ESpecInvalid, "unknown chain op %q", st.Op).WithStep(idx)
		}
	}
	ulp := big.NewRat(1, 1)
	if p, ok := Pow10(cur.Scale); ok {
		ulp = big.NewRat(1, p)
	}
	boundULPs := new(big.Rat).Add(big.NewRat(accULP, 1), fracBound)
	boundRat := new(big.Rat).Mul(boundULPs, ulp)
	return &ChainResult{
		Final:         cur,
		Steps:         steps,
		AccumErrorULP: accULP,
		BoundRat:      boundRat,
		ExactFinal:    exact,
	}, nil
}

// ActualAbsError returns the exact absolute difference between the fixed-point
// final value and the exact rational result.
func (c *ChainResult) ActualAbsError() *big.Rat {
	return new(big.Rat).Abs(new(big.Rat).Sub(c.Final.Rat(), c.ExactFinal))
}
