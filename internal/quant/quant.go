// Package quant 实现确定性的对称定点量化。
package quant

import (
	"fmt"
	"math"
)

type Scheme struct {
	Bits    int     `json:"bits"`
	MaxCode int64   `json:"max_code"`
	MaxAbs  float64 `json:"max_abs"`
	Step    float64 `json:"step"`
}

func NewScheme(bits int, values []float32) Scheme {
	maxAbs := 1.0
	for _, v := range values {
		if a := math.Abs(float64(v)); a > maxAbs && isFinite(a) {
			maxAbs = a
		}
	}
	if bits < 2 || bits > 64 {
		bits = 32
	}
	maxCode := int64(1)<<(bits-1) - 1
	if bits == 64 {
		maxCode = (int64(1)<<62 - 1)
	}
	return Scheme{Bits: bits, MaxCode: maxCode, MaxAbs: maxAbs, Step: maxAbs / float64(maxCode)}
}

func FixedStep(bits int, maxAbs float64) Scheme {
	if bits < 2 || bits > 64 {
		bits = 32
	}
	maxCode := int64(1)<<(bits-1) - 1
	if bits == 64 {
		maxCode = int64(1)<<62 - 1
	}
	if !(maxAbs > 0) {
		maxAbs = 1
	}
	return Scheme{Bits: bits, MaxCode: maxCode, MaxAbs: maxAbs, Step: maxAbs / float64(maxCode)}
}

func (s Scheme) Quantize(values []float32) ([]int64, int) {
	codes := make([]int64, len(values))
	clamps := 0
	for i, v := range values {
		x := math.Round(float64(v) / s.Step)
		if !isFinite(x) {
			x = 0
			clamps++
		}
		if x > float64(s.MaxCode) {
			x = float64(s.MaxCode)
			clamps++
		}
		minCode := -float64(s.MaxCode) - 1
		if x < minCode {
			x = minCode
			clamps++
		}
		codes[i] = int64(x)
	}
	return codes, clamps
}

func (s Scheme) Dequantize(codes []int64) []float64 {
	out := make([]float64, len(codes))
	for i, c := range codes {
		out[i] = float64(c) * s.Step
	}
	return out
}

func (s Scheme) String() string {
	return fmt.Sprintf("Q%d step=%.6g max_abs=%.6g code±%d", s.Bits, s.Step, s.MaxAbs, s.MaxCode)
}

func isFinite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }
