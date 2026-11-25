package quant

import (
	"math"

	"qaudit/internal/model"
)

// LayerEngine is the executable fixed-point implementation of one calibrated layer.
type LayerEngine struct {
	Layer          model.Layer
	Policy         model.LayerPolicy
	WeightRes      []TensorResult
	InputRes       TensorResult
	OutputRes      TensorResult
	WeightQ        [][]int
	BiasQ          []int32
	AccumBits      int
	AccumClamps    int
	OutputClamps   int
	ArtifactDigest string
}

// LayerForwardResult captures one layer's quantized execution trace.
type LayerForwardResult struct {
	DequantOutput []float64
	OutputQ       []int
	AccumClamps   int
	OutputClamps  int
	AccumBits     int
}

func bitsFor(bits, inDim int) int {
	// Signed product needs 2*bits; summing inDim products adds ceil(log2(inDim));
	// one extra sign/guard bit is reserved for bias accumulation.
	extra := 0
	for (1 << extra) < inDim {
		extra++
	}
	need := bits + bits + extra + 1
	if need > 64 {
		need = 64
	}
	return need
}

func buildEngine(layer model.Layer, policy model.LayerPolicy, weightRes []TensorResult, inputRes TensorResult) *LayerEngine {
	wq := make([][]int, layer.OutDim)
	for o := 0; o < layer.OutDim; o++ {
		wp := ParamsFor(weightRes, o)
		wq[o] = make([]int, layer.InDim)
		for i := 0; i < layer.InDim; i++ {
			q, _ := Quantize(wp, layer.Weights[o][i])
			wq[o][i] = q
		}
	}
	return &LayerEngine{
		Layer:     layer,
		Policy:    policy,
		WeightRes: weightRes,
		InputRes:  inputRes,
		WeightQ:   wq,
		AccumBits: bitsFor(policy.BitWidth, layer.InDim),
	}
}

// SetOutputScale fixes the activation scale of the layer output and quantizes biases.
func (e *LayerEngine) SetOutputScale(outRes TensorResult) {
	e.OutputRes = outRes
	e.rebuildBias()
}

// rebuildBias quantizes biases in the accumulator domain. The bias zero point
// absorbs the input/weight zero-point offsets so integer accumulation maps back
// to the real domain exactly via (acc - biasZP) * inputScale * weightScale.
func (e *LayerEngine) rebuildBias() {
	inputScale := e.InputRes.Params.Scale
	inputZP := int64(e.InputRes.Params.ZeroPoint)
	e.BiasQ = make([]int32, e.Layer.OutDim)
	for o := 0; o < e.Layer.OutDim; o++ {
		wp := e.ParamsForWeight(o)
		weightScale := wp.Scale
		weightZP := int64(wp.ZeroPoint)
		var wSum int64
		for i := 0; i < e.Layer.InDim; i++ {
			wSum += int64(e.WeightQ[o][i])
		}
		biasReal := e.Layer.Bias[o]
		// acc = sum(xq*wq) + biasQ; real = (acc - xZP*sum(wq)
		//       - wZP*sum(xq) + n*xZP*wZP) * sx*sw + bias
		biasInAccum := math.Round(biasReal/(inputScale*weightScale)) +
			float64(inputZP*wSum) +
			float64(weightZP)*float64(e.sumInputZero()) -
			float64(int64(e.Layer.InDim)*inputZP*weightZP)
		e.BiasQ[o] = int32(clampRange(biasInAccum, -2147483648, 2147483647))
	}
}

// sumInputZero returns sum(xZP) over the input dimension, i.e. n * inputZP.
func (e *LayerEngine) sumInputZero() int64 {
	return int64(e.Layer.InDim) * int64(e.InputRes.Params.ZeroPoint)
}

// ParamsForWeight exposes the weight scale of output channel o.
func (e *LayerEngine) ParamsForWeight(o int) Params { return ParamsFor(e.WeightRes, o) }

func clampRange(v float64, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// QuantizeInput maps a real input vector into the input integer domain.
func (e *LayerEngine) QuantizeInput(x []float64) ([]int, int) {
	out := make([]int, len(x))
	clamps := 0
	for i, v := range x {
		q, c := Quantize(e.InputRes.Params, v)
		out[i] = q
		if c {
			clamps++
		}
	}
	return out, clamps
}

// Forward runs the integer linear operation followed by requantization to the
// output activation domain. ReLU, when declared, is applied in the real domain
// before output quantization so clamp accounting stays explicit.
func (e *LayerEngine) Forward(inputQ []int) LayerForwardResult {
	nOut := e.Layer.OutDim
	accum := make([]int64, nOut)
	accumClamps := 0
	maxAccum := int64(1)<<(e.AccumBits-1) - 1
	minAccum := -(int64(1) << (e.AccumBits - 1))
	for o := 0; o < nOut; o++ {
		var s int64
		wrow := e.WeightQ[o]
		for i, xq := range inputQ {
			s += int64(xq) * int64(wrow[i])
		}
		s += int64(e.BiasQ[o])
		if s > maxAccum {
			s = maxAccum
			accumClamps++
		} else if s < minAccum {
			s = minAccum
			accumClamps++
		}
		accum[o] = s
	}

	inputScale := e.InputRes.Params.Scale
	inputZP := int64(e.InputRes.Params.ZeroPoint)
	inputSum := int64(0)
	for _, q := range inputQ {
		inputSum += int64(q)
	}
	outClamps := 0
	outQ := make([]int, nOut)
	deq := make([]float64, nOut)
	for o := 0; o < nOut; o++ {
		weightScale := e.ParamsForWeight(o).Scale
		weightZP := int64(e.ParamsForWeight(o).ZeroPoint)
		n := int64(e.Layer.InDim)
		// Invert the zero-point offsets folded into the bias zero point so the
		// integer accumulator maps back to the real linear result.
		effective := accum[o] -
			inputZP*weightSum(e.WeightQ[o]) -
			weightZP*inputSum +
			n*inputZP*weightZP
		real := float64(effective) * inputScale * weightScale
		if e.Layer.Relu && real < 0 {
			real = 0
		}
		q, c := Quantize(e.OutputRes.Params, real)
		if c {
			outClamps++
		}
		outQ[o] = q
		deq[o] = Dequantize(e.OutputRes.Params, q)
	}
	e.AccumClamps += accumClamps
	e.OutputClamps += outClamps
	return LayerForwardResult{
		DequantOutput: deq,
		OutputQ:       outQ,
		AccumClamps:   accumClamps,
		OutputClamps:  outClamps,
		AccumBits:     e.AccumBits,
	}
}

// ResetCounters zeroes per-run clamp counters.
func (e *LayerEngine) ResetCounters() {
	e.AccumClamps = 0
	e.OutputClamps = 0
}

// ClampTotals reports accumulated clamp counts since the last reset.
func (e *LayerEngine) ClampTotals() (accum, output int) {
	return e.AccumClamps, e.OutputClamps
}

func weightSum(row []int) int64 {
	var s int64
	for _, q := range row {
		s += int64(q)
	}
	return s
}
