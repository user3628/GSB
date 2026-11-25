package rope

import (
	"fmt"
	"math"

	"ropechunk/internal/model"
)

// InvFrequency returns the angular frequency for a pair index. The pair uses
// dimensions (2*i, 2*i+1), so its exponent is 2*i/headDim.
//
// ntk_piecewise keeps high-frequency pairs (the first quarter of pairs) at the
// original base, and applies the NTK-adjusted base to the remaining pairs:
//
//	adjustedBase = base * scale^(headDim/(headDim-2))
//
// This is a deterministic two-segment NTK schedule and never expands scaling to
// an angle matrix.
func InvFrequency(pairIdx, headDim int, cfg model.Config) float64 {
	if pairIdx < 0 || pairIdx >= headDim/2 || headDim <= 0 {
		return 0
	}
	exponent := 2 * float64(pairIdx) / float64(headDim)
	base := cfg.BaseFrequency
	switch cfg.ScalingMode {
	case model.ScaleLinear:
		return math.Pow(base, -exponent) / cfg.ScaleFactor
	case model.ScaleNTKPart:
		split := headDim / 8
		if split < 1 {
			split = 1
		}
		if pairIdx >= split {
			exp := 1.0
			if headDim > 2 {
				exp = float64(headDim) / float64(headDim-2)
			}
			base *= math.Pow(cfg.ScaleFactor, exp)
		}
	default:
		// Unscaled mode.
	}
	return math.Pow(base, -exponent)
}

func ValidateFrequencyConfig(cfg model.Config, headDim int) error {
	for i := 0; i < headDim/2; i++ {
		f := InvFrequency(i, headDim, cfg)
		if math.IsNaN(f) || math.IsInf(f, 0) || f <= 0 {
			return fmt.Errorf("invalid frequency for pair %d: %v", i, f)
		}
	}
	return nil
}
