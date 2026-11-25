// Package quant implements deterministic affine quantization, dequantization,
// and quantized-domain linear operators for 4-bit and 8-bit tensors.
package quant

import (
	"errors"
	"fmt"
	"math"
)

type Mode string

const (
	Symmetric  Mode = "symmetric"
	Asymmetric Mode = "asymmetric"
)

type Granularity string

const (
	PerTensor  Granularity = "per_tensor"
	PerChannel Granularity = "per_channel"
)

// Config describes one affine quantization scheme. Per-channel granularity is
// anchored on axis 0: rows for activations and output channels for weights.
type Config struct {
	Bits        int         `json:"bits"`
	Mode        Mode        `json:"mode"`
	Granularity Granularity `json:"granularity"`
}

func (c Config) Validate() error {
	if c.Bits != 4 && c.Bits != 8 {
		return fmt.Errorf("unsupported bit width %d (want 4 or 8)", c.Bits)
	}
	if c.Mode != Symmetric && c.Mode != Asymmetric {
		return fmt.Errorf("unsupported quantization mode %q", c.Mode)
	}
	if c.Granularity != PerTensor && c.Granularity != PerChannel {
		return fmt.Errorf("unsupported granularity %q", c.Granularity)
	}
	return nil
}

// Range returns the inclusive integer range for the configured mode.
func (c Config) Range() (int32, int32) {
	if c.Mode == Symmetric {
		qmax := int32((1 << (c.Bits - 1)) - 1)
		return -qmax - 1, qmax
	}
	return 0, int32((1 << c.Bits) - 1)
}

// Params holds one affine quantization mapping: real = (q-zero)*scale.
type Params struct {
	Scale     float64 `json:"scale"`
	ZeroPoint int32   `json:"zero_point"`
}

type Tensor struct {
	Shape  []int     `json:"shape"`
	Values []float64 `json:"values"`
}

func NewTensor(shape []int, values []float64) Tensor {
	return Tensor{Shape: append([]int(nil), shape...), Values: append([]float64(nil), values...)}
}

func (t Tensor) Clone() Tensor { return NewTensor(t.Shape, t.Values) }

func (t Tensor) Validate() error {
	if len(t.Shape) != 2 {
		return fmt.Errorf("tensor must be rank 2, got shape %v", t.Shape)
	}
	if t.Shape[0] <= 0 || t.Shape[1] <= 0 {
		return fmt.Errorf("tensor dimensions must be positive, got shape %v", t.Shape)
	}
	if len(t.Values) != t.Shape[0]*t.Shape[1] {
		return fmt.Errorf("tensor shape %v requires %d values, got %d", t.Shape, t.Shape[0]*t.Shape[1], len(t.Values))
	}
	return nil
}

// NonFinite records a deterministic sanitization decision.
type NonFinite struct {
	Index int     `json:"index"`
	Kind  string  `json:"kind"`
	Value float64 `json:"value,omitempty"`
}

// Sanitize replaces NaN and infinities with zero and reports every changed
// position. The policy is deliberately explicit so malformed samples still
// produce deterministic outputs and auditable diagnostics.
func Sanitize(t Tensor) (Tensor, []NonFinite) {
	out := t.Clone()
	var events []NonFinite
	for i, v := range out.Values {
		switch {
		case math.IsNaN(v):
			events = append(events, NonFinite{Index: i, Kind: "nan"})
			out.Values[i] = 0
		case math.IsInf(v, 1):
			events = append(events, NonFinite{Index: i, Kind: "+inf"})
			out.Values[i] = 0
		case math.IsInf(v, -1):
			events = append(events, NonFinite{Index: i, Kind: "-inf"})
			out.Values[i] = 0
		}
	}
	return out, events
}

func validateFinite(t Tensor) error {
	for i, v := range t.Values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("non-finite value at flat position %d", i)
		}
	}
	return nil
}

// Calibrate derives affine parameters deterministically without clipping.
func Calibrate(t Tensor, cfg Config) ([]Params, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := t.Validate(); err != nil {
		return nil, err
	}
	if err := validateFinite(t); err != nil {
		return nil, err
	}
	channels, stride := channelPartition(t, cfg)
	params := make([]Params, channels)
	for ch := 0; ch < channels; ch++ {
		lo := ch * stride
		hi := lo + stride
		params[ch] = calibrateSlice(t.Values[lo:hi], cfg)
	}
	return params, nil
}

func channelPartition(t Tensor, cfg Config) (channels, stride int) {
	if cfg.Granularity == PerTensor {
		return 1, len(t.Values)
	}
	return t.Shape[0], t.Shape[1]
}

func calibrateSlice(values []float64, cfg Config) Params {
	qmin, qmax := cfg.Range()
	if cfg.Mode == Symmetric {
		maxAbs := 0.0
		for _, v := range values {
			a := math.Abs(v)
			if a > maxAbs {
				maxAbs = a
			}
		}
		scale := 1.0
		if maxAbs > 0 {
			scale = maxAbs / float64(qmax)
		}
		return Params{Scale: scale, ZeroPoint: 0}
	}

	minV, maxV := values[0], values[0]
	for _, v := range values[1:] {
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}
	if minV == maxV {
		switch {
		case minV == 0:
			return Params{Scale: 1, ZeroPoint: 0}
		case minV > 0:
			return Params{Scale: minV / float64(qmax), ZeroPoint: 0}
		default:
			return Params{Scale: -minV / float64(qmax), ZeroPoint: qmax}
		}
	}
	scale := (maxV - minV) / float64(qmax-qmin)
	zp := int64(math.Round(float64(qmin) - minV/scale))
	if zp < int64(qmin) {
		// The whole range is positive. Anchoring zero at qmin and
		// recomputing the scale avoids clipping the maximum value.
		zp = int64(qmin)
		scale = maxV / float64(qmax)
	} else if zp > int64(qmax) {
		// The whole range is negative. Anchor zero at qmax.
		zp = int64(qmax)
		scale = -minV / float64(qmax)
	}
	if scale <= 0 || math.IsNaN(scale) || math.IsInf(scale, 0) {
		scale = 1
	}
	return Params{Scale: scale, ZeroPoint: int32(zp)}
}

// ClampEvent records every value whose scaled integer representation was
// outside the configured range before clamping.
type ClampEvent struct {
	Index     int     `json:"index"`
	Channel   int     `json:"channel"`
	Value     float64 `json:"value"`
	Unclamped float64 `json:"unclamped"`
	Min       int32   `json:"min"`
	Max       int32   `json:"max"`
}

type QuantizedTensor struct {
	Shape      []int        `json:"shape"`
	Config     Config       `json:"config"`
	Params     []Params     `json:"params"`
	Values     []int64      `json:"values"`
	ClampCount int          `json:"clamp_count"`
	Clamps     []ClampEvent `json:"clamps,omitempty"`
}

func (q QuantizedTensor) ConfigForChannel(ch int) Params {
	if len(q.Params) == 1 {
		return q.Params[0]
	}
	return q.Params[ch]
}

// Quantize computes parameters from the tensor and then quantizes it.
func Quantize(t Tensor, cfg Config) (QuantizedTensor, error) {
	params, err := Calibrate(t, cfg)
	if err != nil {
		return QuantizedTensor{}, err
	}
	return QuantizeWithParams(t, cfg, params)
}

// QuantizeWithParams is useful for pre-calibrated or externally supplied
// scales. Values outside the representable range are clamped and recorded.
func QuantizeWithParams(t Tensor, cfg Config, params []Params) (QuantizedTensor, error) {
	if err := cfg.Validate(); err != nil {
		return QuantizedTensor{}, err
	}
	if err := t.Validate(); err != nil {
		return QuantizedTensor{}, err
	}
	if err := validateFinite(t); err != nil {
		return QuantizedTensor{}, err
	}
	channels, stride := channelPartition(t, cfg)
	if len(params) == 1 && channels > 1 {
		// Explicitly permit a per-tensor parameter slice for a per-channel
		// configuration, but expand it for stable channel lookup.
		expanded := make([]Params, channels)
		for i := range expanded {
			expanded[i] = params[0]
		}
		params = expanded
	}
	if len(params) != channels {
		return QuantizedTensor{}, fmt.Errorf("got %d parameter sets for %d channels", len(params), channels)
	}
	qmin, qmax := cfg.Range()
	out := QuantizedTensor{
		Shape:  append([]int(nil), t.Shape...),
		Config: cfg,
		Params: append([]Params(nil), params...),
		Values: make([]int64, len(t.Values)),
	}
	for i, v := range t.Values {
		p := params[0]
		ch := 0
		if channels > 1 {
			ch = i / stride
			p = params[ch]
		}
		if p.Scale <= 0 || math.IsNaN(p.Scale) || math.IsInf(p.Scale, 0) {
			return QuantizedTensor{}, fmt.Errorf("channel %d has invalid scale %v", ch, p.Scale)
		}
		raw := math.Round(v/p.Scale) + float64(p.ZeroPoint)
		q := int64(raw)
		if q < int64(qmin) || q > int64(qmax) {
			out.Clamps = append(out.Clamps, ClampEvent{
				Index: i, Channel: ch, Value: v, Unclamped: raw, Min: qmin, Max: qmax,
			})
			if q < int64(qmin) {
				q = int64(qmin)
			} else {
				q = int64(qmax)
			}
		}
		out.Values[i] = q
	}
	out.ClampCount = len(out.Clamps)
	return out, nil
}

// Dequantize restores the floating point representation of q.
func Dequantize(q QuantizedTensor) (Tensor, error) {
	if len(q.Shape) != 2 || len(q.Values) != q.Shape[0]*q.Shape[1] {
		return Tensor{}, errors.New("invalid quantized tensor shape")
	}
	channels, stride := channelPartition(Tensor{Shape: q.Shape, Values: make([]float64, len(q.Values))}, q.Config)
	if len(q.Params) != 1 && len(q.Params) != channels {
		return Tensor{}, fmt.Errorf("got %d parameter sets for %d channels", len(q.Params), channels)
	}
	out := Tensor{Shape: append([]int(nil), q.Shape...), Values: make([]float64, len(q.Values))}
	for i, iv := range q.Values {
		p := q.Params[0]
		if len(q.Params) > 1 {
			p = q.Params[i/stride]
		}
		out.Values[i] = float64(iv-int64(p.ZeroPoint)) * p.Scale
	}
	return out, nil
}

// LinearResult holds the integer-domain accumulator and the per-row/per-output
// scale chain required to reconstruct the float result.
type LinearResult struct {
	InputShape   []int        `json:"input_shape"`
	WeightShape  []int        `json:"weight_shape"`
	OutputShape  []int        `json:"output_shape"`
	Accum        []int64      `json:"accum"`
	AScales      []float64    `json:"a_scales"`
	AZero        []int32      `json:"a_zero_points"`
	WScales      []float64    `json:"w_scales"`
	WZero        []int32      `json:"w_zero_points"`
	InputParams  []Params     `json:"input_params"`
	WeightParams []Params     `json:"weight_params"`
	InputClamps  []ClampEvent `json:"input_clamps,omitempty"`
	WeightClamps []ClampEvent `json:"weight_clamps,omitempty"`
}

// Linear quantizes both operands, performs integer-domain multiply-accumulate
// with zero-point correction, and returns the scale chain needed for
// dequantization. The result is independent of evaluation order.
func Linear(a, w Tensor, cfgA, cfgW Config) (LinearResult, error) {
	qa, err := Quantize(a, cfgA)
	if err != nil {
		return LinearResult{}, fmt.Errorf("quantize activation: %w", err)
	}
	qw, err := Quantize(w, cfgW)
	if err != nil {
		return LinearResult{}, fmt.Errorf("quantize weight: %w", err)
	}
	res, err := linearQuantized(qa, qw)
	if err != nil {
		return LinearResult{}, err
	}
	res.InputParams = append([]Params(nil), qa.Params...)
	res.WeightParams = append([]Params(nil), qw.Params...)
	return res, nil
}

// LinearWithParams performs the same operation using pre-calibrated parameters.
func LinearWithParams(a, w Tensor, cfgA, cfgW Config, aParams, wParams []Params) (LinearResult, error) {
	qa, err := QuantizeWithParams(a, cfgA, aParams)
	if err != nil {
		return LinearResult{}, fmt.Errorf("quantize activation: %w", err)
	}
	qw, err := QuantizeWithParams(w, cfgW, wParams)
	if err != nil {
		return LinearResult{}, fmt.Errorf("quantize weight: %w", err)
	}
	res, err := linearQuantized(qa, qw)
	if err != nil {
		return LinearResult{}, err
	}
	res.InputParams = append([]Params(nil), qa.Params...)
	res.WeightParams = append([]Params(nil), qw.Params...)
	return res, nil
}

func linearQuantized(qa, qw QuantizedTensor) (LinearResult, error) {
	m, kA := qa.Shape[0], qa.Shape[1]
	n, kW := qw.Shape[0], qw.Shape[1]
	if kA != kW {
		return LinearResult{}, fmt.Errorf("inner dimensions differ: activation %v and weight %v", qa.Shape, qw.Shape)
	}
	res := LinearResult{
		InputShape:   append([]int(nil), qa.Shape...),
		WeightShape:  append([]int(nil), qw.Shape...),
		OutputShape:  []int{m, n},
		Accum:        make([]int64, m*n),
		AScales:      make([]float64, m),
		AZero:        make([]int32, m),
		WScales:      make([]float64, n),
		WZero:        make([]int32, n),
		InputParams:  append([]Params(nil), qa.Params...),
		WeightParams: append([]Params(nil), qw.Params...),
		InputClamps:  append([]ClampEvent(nil), qa.Clamps...),
		WeightClamps: append([]ClampEvent(nil), qw.Clamps...),
	}
	for row := 0; row < m; row++ {
		p := qa.ConfigForChannel(row)
		res.AScales[row], res.AZero[row] = p.Scale, p.ZeroPoint
	}
	for col := 0; col < n; col++ {
		p := qw.ConfigForChannel(col)
		res.WScales[col], res.WZero[col] = p.Scale, p.ZeroPoint
	}
	for row := 0; row < m; row++ {
		for col := 0; col < n; col++ {
			var sum int64
			for kk := 0; kk < kA; kk++ {
				av := qa.Values[row*kA+kk] - int64(qa.ConfigForChannel(row).ZeroPoint)
				wv := qw.Values[col*kW+kk] - int64(qw.ConfigForChannel(col).ZeroPoint)
				sum += av * wv
			}
			res.Accum[row*n+col] = sum
		}
	}
	return res, nil
}

// DequantizeLinear reconstructs the float output using the complete scale chain.
func DequantizeLinear(r LinearResult) (Tensor, error) {
	if len(r.OutputShape) != 2 {
		return Tensor{}, errors.New("invalid linear result")
	}
	m, n := r.OutputShape[0], r.OutputShape[1]
	if len(r.Accum) != m*n || len(r.AScales) != m || len(r.WScales) != n {
		return Tensor{}, errors.New("inconsistent linear result dimensions")
	}
	out := Tensor{Shape: append([]int(nil), r.OutputShape...), Values: make([]float64, len(r.Accum))}
	for row := 0; row < m; row++ {
		for col := 0; col < n; col++ {
			i := row*n + col
			out.Values[i] = float64(r.Accum[i]) * r.AScales[row] * r.WScales[col]
		}
	}
	return out, nil
}

// ReferenceLayer is the high-precision counterpart used by verification.
type ReferenceLayer struct {
	Index       int       `json:"index"`
	Name        string    `json:"name"`
	InputShape  []int     `json:"input_shape"`
	WeightShape []int     `json:"weight_shape"`
	OutputShape []int     `json:"output_shape"`
	Output      []float64 `json:"output"`
	WeightL1    []float64 `json:"weight_l1"`
}

func MatMulReference(a, w Tensor) (Tensor, []float64, error) {
	if err := a.Validate(); err != nil {
		return Tensor{}, nil, err
	}
	if err := w.Validate(); err != nil {
		return Tensor{}, nil, err
	}
	m, k := a.Shape[0], a.Shape[1]
	n, kw := w.Shape[0], w.Shape[1]
	if k != kw {
		return Tensor{}, nil, fmt.Errorf("inner dimensions differ: %v and %v", a.Shape, w.Shape)
	}
	out := Tensor{Shape: []int{m, n}, Values: make([]float64, m*n)}
	for row := 0; row < m; row++ {
		for col := 0; col < n; col++ {
			var sum, correction float64
			for kk := 0; kk < k; kk++ {
				term := a.Values[row*k+kk] * w.Values[col*k+kk]
				next := sum + term
				if math.Abs(sum) >= math.Abs(term) {
					correction += (sum - next) + term
				} else {
					correction += (term - next) + sum
				}
				sum = next
			}
			out.Values[row*n+col] = sum + correction
		}
	}
	weightL1 := make([]float64, m)
	for row := 0; row < m; row++ {
		var maxL1 float64
		for col := 0; col < n; col++ {
			var l1, correction float64
			for kk := 0; kk < k; kk++ {
				term := math.Abs(w.Values[col*k+kk])
				next := l1 + term
				if math.Abs(l1) >= math.Abs(term) {
					correction += (l1 - next) + term
				} else {
					correction += (term - next) + l1
				}
				l1 = next
			}
			l1 += correction
			if l1 > maxL1 {
				maxL1 = l1
			}
		}
		weightL1[row] = maxL1
	}
	return out, weightL1, nil
}

// ReferenceChain runs an exact float linear chain.
func ReferenceChain(input Tensor, weights []Tensor) ([]ReferenceLayer, error) {
	cur := input
	out := make([]ReferenceLayer, 0, len(weights))
	for i, w := range weights {
		next, l1, err := MatMulReference(cur, w)
		if err != nil {
			return nil, fmt.Errorf("reference layer %d: %w", i, err)
		}
		out = append(out, ReferenceLayer{
			Index: i, InputShape: append([]int(nil), cur.Shape...),
			WeightShape: append([]int(nil), w.Shape...), OutputShape: append([]int(nil), next.Shape...),
			Output: next.Values, WeightL1: l1,
		})
		cur = next
	}
	return out, nil
}
