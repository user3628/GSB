// Package reference 生成仅用于本地真值的 float64 朴素注意力与定点量化码。
package reference

import (
	"math"

	"flashattention/internal/quant"
	"flashattention/internal/tensor"
)

type Output struct {
	Float          *tensor.Tensor `json:"-"`
	Codes          []int64        `json:"codes"`
	Scheme         quant.Scheme   `json:"scheme"`
	Step           float64        `json:"step"`
	Bits           int            `json:"bits"`
	DequantizedBuf []float64      `json:"-"`
}

func Compute(q, k, v *tensor.Tensor, causal bool, scale float64, bits, keyLen int) *Output {
	sh := q.Shape
	out := tensor.New(sh)
	if keyLen <= 0 || keyLen > sh.S {
		keyLen = sh.S
	}
	weights := make([]float64, sh.S)

	for b := 0; b < sh.B; b++ {
		for h := 0; h < sh.H; h++ {
			for i := 0; i < sh.S; i++ {
				m := math.Inf(-1)
				for j := 0; j < keyLen; j++ {
					if causal && j > i {
						weights[j] = math.Inf(-1)
						continue
					}
					var dot float64
					qr := q.Row(b, h, i)
					kr := k.Row(b, h, j)
					for d := 0; d < sh.D; d++ {
						dot += float64(qr[d]) * float64(kr[d])
					}
					w := dot * scale
					weights[j] = w
					if w > m {
						m = w
					}
				}
				sum := 0.0
				for j := 0; j < keyLen; j++ {
					if causal && j > i || math.IsInf(weights[j], -1) {
						weights[j] = 0
						continue
					}
					weights[j] = math.Exp(weights[j] - m)
					sum += weights[j]
				}
				for d := 0; d < sh.D; d++ {
					acc := 0.0
					for j := 0; j < keyLen; j++ {
						if weights[j] == 0 {
							continue
						}
						acc += (weights[j] / sum) * float64(v.At(b, h, j, d))
					}
					out.Set(b, h, i, d, float32(acc))
				}
			}
		}
	}

	scheme := quant.NewScheme(bits, out.Data)
	codes, _ := scheme.Quantize(out.Data)
	return &Output{Float: out, Codes: codes, Scheme: scheme, Step: scheme.Step, Bits: bits}
}

func FromCodes(sh tensor.Shape, codes []int64, scheme quant.Scheme) *Output {
	buf := scheme.Dequantize(codes)
	return &Output{
		Float:          &tensor.Tensor{Shape: sh, Data: float64To32(buf)},
		Codes:          codes,
		Scheme:         scheme,
		Step:           scheme.Step,
		Bits:           scheme.Bits,
		DequantizedBuf: buf,
	}
}

func (r *Output) Dequantized() []float64 {
	if r.DequantizedBuf != nil {
		return r.DequantizedBuf
	}
	r.DequantizedBuf = r.Scheme.Dequantize(r.Codes)
	return r.DequantizedBuf
}

func float64To32(xs []float64) []float32 {
	out := make([]float32, len(xs))
	for i, x := range xs {
		out[i] = float32(x)
	}
	return out
}
