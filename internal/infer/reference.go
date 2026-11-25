// Package infer implements the reference forward pass, quantized inference,
// stage-3 numeric comparison and the end-to-end pipeline orchestration.
package infer

import (
	"math"

	"qaudit/internal/model"
)

// ReferenceForward runs the high-precision (float64) network and returns the
// logits plus every layer-boundary activation keyed by layer name.
func ReferenceForward(net model.Network, input []float64) (logits []float64, boundaries map[string][]float64) {
	x := append([]float64(nil), input...)
	boundaries = map[string][]float64{}
	for _, layer := range net.Layers {
		y := make([]float64, layer.OutDim)
		for o := 0; o < layer.OutDim; o++ {
			s := layer.Bias[o]
			row := layer.Weights[o]
			for i, v := range x {
				s += row[i] * v
			}
			if layer.Relu && s < 0 {
				s = 0
			}
			y[o] = s
		}
		boundaries[layer.Name] = y
		x = y
	}
	return x, boundaries
}

// Softmax converts logits into class probabilities.
func Softmax(logits []float64) []float64 {
	max := math.Inf(-1)
	for _, v := range logits {
		if v > max {
			max = v
		}
	}
	out := make([]float64, len(logits))
	sum := 0.0
	for i, v := range logits {
		out[i] = math.Exp(v - max)
		sum += out[i]
	}
	for i := range out {
		out[i] /= sum
	}
	return out
}

// Argmax returns the index of the largest value.
func Argmax(v []float64) int {
	best, idx := math.Inf(-1), 0
	for i, x := range v {
		if x > best {
			best, idx = x, i
		}
	}
	return idx
}

// LayerDeviation compares one layer boundary at an explicit sample position.
type LayerDeviation struct {
	Layer       string    `json:"layer"`
	Position    int       `json:"position"`
	MaxAbsDev   float64   `json:"max_abs_dev"`
	RelDev      float64   `json:"rel_dev"`
	QuantOutput []float64 `json:"quant_output"`
	RefOutput   []float64 `json:"ref_output"`
}

// RelativeError computes the end-to-end relative error on logits.
func RelativeError(quant, ref []float64) float64 {
	var num, den float64
	for i := range ref {
		num += (quant[i] - ref[i]) * (quant[i] - ref[i])
		den += ref[i] * ref[i]
	}
	if den == 0 {
		return math.Sqrt(num)
	}
	return math.Sqrt(num / den)
}
