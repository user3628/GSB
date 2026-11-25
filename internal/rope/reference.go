package rope

import (
	"fmt"
	"math/big"

	"ropechunk/internal/model"
)

const referencePrecision = 256

var bigPi = func() *big.Float {
	pi, _, err := big.ParseFloat("3.141592653589793238462643383279502884197169399375105820974944592307816406286", 10, referencePrecision, big.ToNearestEven)
	if err != nil {
		panic(err)
	}
	return pi
}()

// ReferenceRotate computes the fixed-point reference with 256-bit Taylor
// trigonometry. The angular frequency itself is derived by the public library
// formula, while all subsequent rotation arithmetic is high precision.
func ReferenceRotate(src model.QuantTensor, cfg model.Config) (model.QuantTensor, error) {
	if err := src.Validate("reference source tensor"); err != nil {
		return model.QuantTensor{}, err
	}
	if err := ValidateFrequencyConfig(cfg, src.Shape.HeadDim()); err != nil {
		return model.QuantTensor{}, err
	}
	d := src.Shape.HeadDim()
	out := model.QuantTensor{Shape: src.Shape, Scale: src.Scale, Data: make([]int64, len(src.Data))}
	scale := new(big.Float).SetPrec(referencePrecision).SetInt64(src.Scale)
	for b := 0; b < src.Shape.Batch(); b++ {
		for h := 0; h < src.Shape.Heads(); h++ {
			for row := 0; row < src.Shape.Seq(); row++ {
				base := src.Index(b, h, row, 0)
				copy(out.Data[base:base+d], src.Data[base:base+d])
				for dim := 0; dim+1 < d; dim += 2 {
					invFreq := InvFrequency(dim/2, d, cfg)
					angle := new(big.Float).SetPrec(referencePrecision).SetFloat64(float64(row) * invFreq)
					sine, cosine := bigSinCos(angle)
					x0 := new(big.Float).SetPrec(referencePrecision).SetInt64(src.Data[base+dim])
					x0.Quo(x0, scale)
					x1 := new(big.Float).SetPrec(referencePrecision).SetInt64(src.Data[base+dim+1])
					x1.Quo(x1, scale)
					y0 := new(big.Float).SetPrec(referencePrecision).Mul(x0, cosine)
					y0.Sub(y0, new(big.Float).SetPrec(referencePrecision).Mul(x1, sine))
					y1 := new(big.Float).SetPrec(referencePrecision).Mul(x0, sine)
					y1.Add(y1, new(big.Float).SetPrec(referencePrecision).Mul(x1, cosine))
					out.Data[base+dim] = quantizeBig(y0, src.Scale)
					out.Data[base+dim+1] = quantizeBig(y1, src.Scale)
				}
			}
		}
	}
	return out, nil
}

func bigSinCos(x *big.Float) (sin, cos *big.Float) {
	prec := uint(referencePrecision)
	twoPi := new(big.Float).SetPrec(prec).Mul(bigPi, new(big.Float).SetPrec(prec).SetInt64(2))
	q := new(big.Float).SetPrec(prec).Quo(x, twoPi)
	qInt, _ := q.Int(nil)
	r := new(big.Float).SetPrec(prec).Sub(x, new(big.Float).SetPrec(prec).Mul(new(big.Float).SetPrec(prec).SetInt(qInt), twoPi))
	if r.Cmp(bigPi) > 0 {
		r.Sub(r, twoPi)
	}

	sq := new(big.Float).SetPrec(prec).Mul(r, r)
	sin = new(big.Float).SetPrec(prec).Set(r)
	sinTerm := new(big.Float).SetPrec(prec).Set(r)
	cos = new(big.Float).SetPrec(prec).SetInt64(1)
	cosTerm := new(big.Float).SetPrec(prec).SetInt64(1)
	limit := new(big.Float).SetPrec(prec).SetMantExp(big.NewFloat(1), -int(prec)+8)
	for n := 1; n < 100; n++ {
		denSin := new(big.Float).SetPrec(prec).SetInt64(int64((2 * n) * (2*n + 1)))
		sinTerm.Mul(sinTerm, sq)
		sinTerm.Quo(sinTerm, denSin)
		sinTerm.Neg(sinTerm)
		sin.Add(sin, sinTerm)

		denCos := new(big.Float).SetPrec(prec).SetInt64(int64((2*n - 1) * (2 * n)))
		cosTerm.Mul(cosTerm, sq)
		cosTerm.Quo(cosTerm, denCos)
		cosTerm.Neg(cosTerm)
		cos.Add(cos, cosTerm)

		if new(big.Float).SetPrec(prec).Abs(sinTerm).Cmp(limit) < 0 &&
			new(big.Float).SetPrec(prec).Abs(cosTerm).Cmp(limit) < 0 {
			break
		}
	}
	return sin, cos
}

func quantizeBig(v *big.Float, scale int64) int64 {
	scaled := new(big.Float).SetPrec(referencePrecision).Mul(v, new(big.Float).SetPrec(referencePrecision).SetInt64(scale))
	half := new(big.Float).SetPrec(referencePrecision).SetFloat64(0.5)
	if scaled.Sign() >= 0 {
		scaled.Add(scaled, half)
	} else {
		scaled.Sub(scaled, half)
	}
	i, accuracy := scaled.Int(nil)
	if accuracy == big.Below || accuracy == big.Above {
		// Int always returns the truncated integer; accuracy only describes it.
	}
	if !i.IsInt64() {
		panic(fmt.Sprintf("reference quantization overflow: %s", i.String()))
	}
	return i.Int64()
}
