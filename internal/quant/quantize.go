package quant

import (
	"math"

	"qaudit/internal/model"
)

// SupportedBitWidths is the fixed set accepted by the workbench.
var SupportedBitWidths = map[int]bool{4: true, 8: true}

// Params describes the integer domain of a quantized tensor.
type Params struct {
	Scale     float64
	ZeroPoint int
	QMin      int
	QMax      int
}

// Domain returns the signed/unsigned integer bounds for a bit width and mode.
func Domain(bits int, symmetric bool) (qmin, qmax int) {
	if symmetric {
		// Signed symmetric domain, reserving the extreme negative value is avoided
		// so the range is perfectly centered around zero.
		qmax = (1 << (bits - 1)) - 1
		qmin = -qmax
		return qmin, qmax
	}
	// Unsigned asymmetric domain.
	return 0, (1 << bits) - 1
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// ComputeParams derives scale and zero point so that [min, max] is fully covered.
// A non-finite range or a zero scale on a non-constant tensor is reported by the
// caller through E_QUANT_PARAM; this function returns ok=false in those cases.
func ComputeParams(min, max float64, bits int, symmetric bool) (Params, bool) {
	if math.IsNaN(min) || math.IsNaN(max) || math.IsInf(min, 0) || math.IsInf(max, 0) {
		return Params{}, false
	}
	qmin, qmax := Domain(bits, symmetric)
	if symmetric {
		r := math.Max(math.Abs(min), math.Abs(max))
		if r == 0 {
			// Constant-zero symmetric tensor: pick a unit scale instead of zero so
			// quantized weights never silently collapse to an unusable zero scale.
			return Params{Scale: 1, ZeroPoint: 0, QMin: qmin, QMax: qmax}, true
		}
		return Params{Scale: r / float64(qmax), ZeroPoint: 0, QMin: qmin, QMax: qmax}, true
	}
	if max < min {
		max = min
	}
	if max == min {
		// Constant tensor: widen the representable window around the constant while
		// keeping a positive, non-zero scale.
		span := math.Max(1, math.Abs(min))
		min, max = min-span, max+span
	}
	scale := (max - min) / float64(qmax-qmin)
	if scale <= 0 || math.IsNaN(scale) || math.IsInf(scale, 0) {
		return Params{}, false
	}
	zp := math.Round(-min/scale) - float64(qmin)
	z := clampInt(int(zp), qmin, qmax)
	return Params{Scale: scale, ZeroPoint: z, QMin: qmin, QMax: qmax}, true
}

// Quantize maps a real value into the integer domain and reports whether it was clamped.
func Quantize(p Params, x float64) (int, bool) {
	q := math.Round(x/p.Scale) + float64(p.ZeroPoint)
	qi := int(q)
	clamped := false
	if qi < p.QMin {
		qi = p.QMin
		clamped = true
	} else if qi > p.QMax {
		qi = p.QMax
		clamped = true
	}
	return qi, clamped
}

// Dequantize maps an integer value back to the real domain.
func Dequantize(p Params, q int) float64 {
	return (float64(q) - float64(p.ZeroPoint)) * p.Scale
}

// TensorResult aggregates a tensor calibration: parameters plus observed clamp count.
type TensorResult struct {
	Params  Params
	Clamps  int
	AllZero bool
	HasNaN  bool
}

func toModel(t TensorResult, channel int) model.TensorParams {
	return model.TensorParams{
		Scale:     t.Params.Scale,
		ZeroPoint: t.Params.ZeroPoint,
		QMin:      t.Params.QMin,
		QMax:      t.Params.QMax,
		Clamps:    t.Clamps,
		Step:      t.Params.Scale,
		Channel:   channel,
	}
}

// CalibrateTensor calibrates one flat tensor (layer-wise, channel == -1).
func CalibrateTensor(values []float64, bits int, symmetric bool) (TensorResult, bool) {
	return calibrate(values, bits, symmetric, -1)
}

func calibrate(values []float64, bits int, symmetric bool, channel int) (TensorResult, bool) {
	res := TensorResult{}
	if len(values) == 0 {
		return res, false
	}
	hasNaN := false
	allZero := true
	min, max := math.Inf(1), math.Inf(-1)
	for _, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			hasNaN = true
			continue
		}
		allZero = allZero && v == 0
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	res.HasNaN = hasNaN
	res.AllZero = allZero
	if hasNaN || math.IsInf(min, 0) || math.IsInf(max, 0) {
		return res, false
	}
	p, ok := ComputeParams(min, max, bits, symmetric)
	if !ok || p.Scale == 0 {
		return res, false
	}
	res.Params = p
	for _, v := range values {
		_, c := Quantize(p, v)
		if c {
			res.Clamps++
		}
	}
	return res, true
}

// CalibrateRows calibrates a matrix per output channel (one Params per row) or
// layer-wise (a single Params shared by every row).
func CalibrateRows(matrix [][]float64, bits int, symmetric, perChannel bool) ([]TensorResult, error) {
	if !perChannel {
		flat := make([]float64, 0, len(matrix)*len(matrix[0]))
		for _, row := range matrix {
			flat = append(flat, row...)
		}
		res, ok := calibrate(flat, bits, symmetric, -1)
		if !ok {
			return nil, model.NewBizError(model.CodeQuantParam, "weight calibration failed",
				map[string]string{"channel": "-1"})
		}
		return []TensorResult{res}, nil
	}
	out := make([]TensorResult, len(matrix))
	for ch, row := range matrix {
		res, ok := calibrate(row, bits, symmetric, ch)
		if !ok {
			ctx := map[string]string{"channel": intToStr(ch)}
			if res.HasNaN {
				ctx["reason"] = "non_finite_weight"
			} else if res.AllZero {
				ctx["reason"] = "all_zero_channel"
			}
			return nil, model.NewBizError(model.CodeQuantParam, "per-channel weight calibration failed", ctx)
		}
		out[ch] = res
	}
	return out, nil
}

// ToModelParams converts internal tensor results into the persisted model shape.
func ToModelParams(results []TensorResult) []model.TensorParams {
	out := make([]model.TensorParams, len(results))
	for i, r := range results {
		ch := -1
		if len(results) > 1 {
			ch = i
		}
		out[i] = toModel(r, ch)
	}
	return out
}

// ParamsFor returns the calibration parameters applicable to channel ch,
// honoring layer-wise vs per-channel layouts.
func ParamsFor(results []TensorResult, ch int) Params {
	if len(results) == 1 {
		return results[0].Params
	}
	return results[ch].Params
}

func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
