package model

import (
	"math"
	"sort"

	"regworkbench/internal/preprocess"
)

type MatrixBundle struct {
	TrainX [][]float64
	TrainY []float64
	EvalX  [][]float64
	EvalY  []float64
	Names  []string
}

func BuildBundle(p *preprocess.Prepared, trainIdx, evalIdx []int, smoothing float64) MatrixBundle {
	targetEncoders := make([]map[string]float64, len(p.TargetRaw))
	globalPrior := meanY(p.Y, trainIdx)
	for t, rawCol := range p.TargetRaw {
		sum := map[string]float64{}
		count := map[string]float64{}
		var pos, n float64
		for _, idx := range trainIdx {
			key := rawCol[idx]
			sum[key] += p.Y[idx]
			count[key]++
			pos += p.Y[idx]
			n++
		}
		prior := globalPrior
		if n > 0 {
			prior = pos / n
		}
		enc := map[string]float64{}
		keys := make([]string, 0, len(count))
		for k := range count {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			enc[k] = (sum[k] + smoothing*prior) / (count[k] + smoothing)
		}
		enc["__prior__"] = prior
		targetEncoders[t] = enc
	}
	return MatrixBundle{
		TrainX: buildRows(p, trainIdx, targetEncoders),
		TrainY: subsetY(p.Y, trainIdx),
		EvalX:  buildRows(p, evalIdx, targetEncoders),
		EvalY:  subsetY(p.Y, evalIdx),
		Names:  append([]string(nil), p.FeatureNames...),
	}
}

func BuildFullBundle(p *preprocess.Prepared, smoothing float64) MatrixBundle {
	all := make([]int, p.RowCount)
	for i := range all {
		all[i] = i
	}
	b := BuildBundle(p, all, nil, smoothing)
	b.EvalX = nil
	b.EvalY = nil
	return b
}

func buildRows(p *preprocess.Prepared, indices []int, encoders []map[string]float64) [][]float64 {
	out := make([][]float64, len(indices))
	numCount := len(p.Numeric)
	oneHotWidth := 0
	if len(p.OneHot) > 0 {
		oneHotWidth = len(p.OneHot[0])
	}
	for r, idx := range indices {
		row := make([]float64, 0, numCount+oneHotWidth+len(encoders))
		if idx < len(p.Numeric) {
			row = append(row, p.Numeric[idx]...)
		} else {
			row = append(row, make([]float64, numCount)...)
		}
		if len(p.OneHot) > 0 && idx < len(p.OneHot) {
			row = append(row, p.OneHot[idx]...)
		}
		for t, enc := range encoders {
			v := enc["__prior__"]
			if t < len(p.TargetRaw) && idx < len(p.TargetRaw[t]) {
				if encoded, ok := enc[p.TargetRaw[t][idx]]; ok {
					v = encoded
				}
			}
			row = append(row, v)
		}
		out[r] = row
	}
	return out
}

func subsetY(y []float64, idx []int) []float64 {
	out := make([]float64, len(idx))
	for i, j := range idx {
		out[i] = y[j]
	}
	return out
}

func meanY(y []float64, idx []int) float64 {
	if len(idx) == 0 {
		return 0.5
	}
	s := 0.0
	for _, i := range idx {
		s += y[i]
	}
	return s / float64(len(idx))
}

func sigmoid(z float64) float64 {
	if z >= 0 {
		e := math.Exp(-z)
		return 1 / (1 + e)
	}
	e := math.Exp(z)
	return e / (1 + e)
}

func softplus(z float64) float64 {
	if z > 0 {
		return z + math.Log1p(math.Exp(-z))
	}
	return math.Log1p(math.Exp(z))
}
