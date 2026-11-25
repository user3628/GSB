package pipeline

import (
	"fmt"
	"math"

	"headprune/internal/model"
)

// HeadQuant is the per-head quantization record. The derivation fields keep
// the full evidence chain behind the sensitivity error bound.
type HeadQuant struct {
	Head             model.HeadID `json:"head"`
	Bits             int          `json:"bits"`
	Scale            float64      `json:"scale"`
	ZeroPoint        int          `json:"zero_point"`
	Levels           int          `json:"levels"`
	MaxAbs           float64      `json:"max_abs"`
	TheoryAbsErr     float64      `json:"theory_abs_error_bound"`
	ObservedAbsErr   float64      `json:"observed_max_abs_error"`
	RelativeErrBound float64      `json:"relative_error_bound"`
	ENOB             float64      `json:"effective_bits"`
	Derivation       string       `json:"derivation"`
}

// QuantTable groups quantization results and exposes a content fingerprint.
type QuantTable struct {
	Bits  int         `json:"bits"`
	Heads []HeadQuant `json:"heads"`
}

func (t QuantTable) Fingerprint() string {
	d, _ := model.Digest(t)
	return d
}

// QuantizeHeads applies deterministic signed symmetric fixed-point
// quantization to every head activation vector.
func QuantizeHeads(heads []model.Head, bits int) (QuantTable, error) {
	if bits < 2 || bits > 64 {
		return QuantTable{}, &model.BizError{Code: model.CodePolicyInvalid, Message: fmt.Sprintf("quant bits out of range: %d", bits)}
	}
	levels := 1 << bits
	maxCode := levels/2 - 1
	table := QuantTable{Bits: bits, Heads: make([]HeadQuant, 0, len(heads))}
	for _, h := range heads {
		maxAbs := 0.0
		for _, a := range h.Activations {
			if math.IsNaN(a) || math.IsInf(a, 0) {
				return QuantTable{}, (&model.BizError{
					Code:    model.CodeQuantization,
					Message: "activation contains NaN/Inf, numeric processing refused",
				}).WithHead(h.Layer, h.Head)
			}
			if v := math.Abs(a); v > maxAbs {
				maxAbs = v
			}
		}
		if maxAbs == 0 {
			return QuantTable{}, (&model.BizError{
				Code:    model.CodeQuantization,
				Message: "scale factor evaluates to zero for a zero-range activation, no numeric truncation allowed",
				Context: map[string]string{"max_abs": "0"},
			}).WithHead(h.Layer, h.Head)
		}
		scale := maxAbs / float64(maxCode)
		if scale <= 0 || !model.Finite(scale) {
			return QuantTable{}, (&model.BizError{
				Code:    model.CodeQuantization,
				Message: "scale factor is zero or non-finite",
				Context: map[string]string{"scale": fmt.Sprintf("%g", scale)},
			}).WithHead(h.Layer, h.Head)
		}
		theory := scale / 2.0
		observed := 0.0
		signalEnergy := 0.0
		noiseEnergy := 0.0
		for _, a := range h.Activations {
			code := math.Round(a / scale)
			if code > float64(maxCode) {
				code = float64(maxCode)
			}
			if code < float64(-maxCode-1) {
				code = float64(-maxCode - 1)
			}
			deq := code * scale
			err := math.Abs(deq - a)
			if err > observed {
				observed = err
			}
			noiseEnergy += (deq - a) * (deq - a)
			signalEnergy += a * a
		}
		relative := theory / maxAbs
		enob := float64(bits)
		if signalEnergy > 0 {
			rmsSignal := math.Sqrt(signalEnergy / float64(len(h.Activations)))
			// Uniform quantization noise has RMS = scale/sqrt(12), the
			// standard fixed-point ENOB derivation.
			rmsQuantNoise := scale / math.Sqrt(12.0)
			enob = math.Log2(rmsSignal / rmsQuantNoise)
			if enob > float64(bits) {
				enob = float64(bits)
			}
			if enob < 0 {
				enob = 0
			}
		}
		table.Heads = append(table.Heads, HeadQuant{
			Head:             h.ID(),
			Bits:             bits,
			Scale:            scale,
			ZeroPoint:        0,
			Levels:           levels,
			MaxAbs:           maxAbs,
			TheoryAbsErr:     theory,
			ObservedAbsErr:   observed,
			RelativeErrBound: relative,
			ENOB:             enob,
			Derivation: fmt.Sprintf(
				"symmetric signed: scale=max_abs/max_code=%.12g/%d=%.12g; |q(x)-x|<=scale/2=%.12g; relative=%.12g; ENOB=log2(rms_signal/(scale/sqrt(12)))=%.6f",
				maxAbs, maxCode, scale, theory, relative, enob),
		})
	}
	return table, nil
}

// DequantActivation reconstructs a probe activation vector using the stored
// scale factor, keeping quantization explicit rather than truncating values.
func DequantActivation(h model.Head, q HeadQuant, probeIdx int, seed int64) []float64 {
	raw := model.ScaledActivation(h, probeIdx, seed)
	maxCode := (1<<q.Bits)/2 - 1
	minCode := -maxCode - 1
	out := make([]float64, len(raw))
	for i, a := range raw {
		code := math.Round(a / q.Scale)
		if code > float64(maxCode) {
			code = float64(maxCode)
		}
		if code < float64(minCode) {
			code = float64(minCode)
		}
		out[i] = code * q.Scale
	}
	return out
}

// ValidateQuantizedDistribution verifies that a softmax distribution produced
// from quantized activations still normalizes. A zero sum means quantization
// destroyed the signal and must surface as E_QUANTIZATION, never as silent
// truncation.
func ValidateQuantizedDistribution(dist []float64, layer, head int) error {
	sum := 0.0
	for _, p := range dist {
		if !model.Finite(p) || p < 0 {
			return (&model.BizError{
				Code:    model.CodeQuantization,
				Message: "quantized reconstruction contains non-finite probability",
				Context: map[string]string{"probability_sum": fmt.Sprintf("%g", sum)},
			}).WithHead(layer, head)
		}
		sum += p
	}
	if sum <= 0 {
		return (&model.BizError{
			Code:    model.CodeQuantization,
			Message: "quantized reconstruction probability sum is zero",
			Context: map[string]string{"probability_sum": "0"},
		}).WithHead(layer, head)
	}
	return nil
}
