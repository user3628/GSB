package hp

import (
	"fmt"
	"math"
	"math/big"
)

const Precision = 320

func New() *big.Float { return new(big.Float).SetPrec(Precision) }

func FromInt(v int64) *big.Float {
	return new(big.Float).SetPrec(Precision).SetInt64(v)
}

func FromFloat(v float64) *big.Float {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return new(big.Float).SetPrec(Precision).SetFloat64(v)
}

func FromRatio(numerator, denominator int64) *big.Float {
	return new(big.Float).SetPrec(Precision).Quo(FromInt(numerator), FromInt(denominator))
}

func Log(x *big.Float) (*big.Float, error) {
	if x == nil || x.Sign() <= 0 {
		return nil, fmt.Errorf("log requires a finite positive value")
	}
	work := new(big.Float).SetPrec(Precision + 64).Set(x)
	mant := new(big.Float).SetPrec(Precision + 64)
	exp := work.MantExp(mant)
	// MantExp returns a mantissa in [0.5,1). Shift to [1,2) and compensate.
	m := new(big.Float).SetPrec(Precision+64).Mul(mant, FromInt(2))
	exp--
	one := FromInt(1)
	z := new(big.Float).SetPrec(Precision+64).Quo(
		new(big.Float).SetPrec(Precision+64).Sub(m, one),
		new(big.Float).SetPrec(Precision+64).Add(m, one),
	)
	z2 := new(big.Float).SetPrec(Precision+64).Mul(z, z)
	sum := new(big.Float).SetPrec(Precision + 64).Set(z)
	power := new(big.Float).SetPrec(Precision + 64).Set(z)
	threshold := new(big.Float).SetPrec(Precision + 64)
	threshold.SetString("1e-95")
	for k := int64(1); k < 10000; k++ {
		power.Mul(power, z2)
		delta := new(big.Float).SetPrec(Precision+64).Quo(power, FromInt(2*k+1))
		sum.Add(sum, delta)
		if new(big.Float).Abs(delta).Cmp(threshold) < 0 {
			break
		}
	}
	result := new(big.Float).SetPrec(Precision+64).Mul(sum, FromInt(2))
	if exp != 0 {
		ln2 := new(big.Float).SetPrec(Precision + 64)
		ln2.SetString("0.693147180559945309417232121458176568075500134360255254120680009493393621969694715605863326996418687542")
		part := new(big.Float).SetPrec(Precision+64).Mul(FromInt(int64(exp)), ln2)
		result.Add(result, part)
	}
	return new(big.Float).SetPrec(Precision).Set(result), nil
}

func IDF(docCount, docFreq int64) (*big.Float, error) {
	if docCount <= 0 || docFreq <= 0 || docFreq > docCount {
		return nil, fmt.Errorf("invalid BM25 document frequencies: n=%d df=%d", docCount, docFreq)
	}
	// ln(1 + (N - df + 0.5)/(df + 0.5))
	numerator := new(big.Float).SetPrec(Precision).Add(
		new(big.Float).SetPrec(Precision).Sub(FromInt(docCount), FromInt(docFreq)),
		FromFloat(0.5),
	)
	denominator := new(big.Float).SetPrec(Precision).Add(FromInt(docFreq), FromFloat(0.5))
	ratio := new(big.Float).SetPrec(Precision).Quo(numerator, denominator)
	ratio.Add(ratio, FromInt(1))
	return Log(ratio)
}

func BM25Weight(idf *big.Float, tf, docLen int64, avgDocLen, k1, b *big.Float) (*big.Float, error) {
	if idf == nil || avgDocLen == nil || k1 == nil || b == nil {
		return nil, fmt.Errorf("nil BM25 operand")
	}
	if tf <= 0 || docLen < 0 || avgDocLen.Sign() <= 0 {
		return nil, fmt.Errorf("invalid BM25 term frequency or length: tf=%d dl=%d", tf, docLen)
	}
	tfF := FromInt(tf)
	dlF := FromInt(docLen)
	numerator := new(big.Float).SetPrec(Precision).Mul(tfF, new(big.Float).SetPrec(Precision).Add(k1, FromInt(1)))
	lengthRatio := new(big.Float).SetPrec(Precision).Quo(dlF, avgDocLen)
	lengthPart := new(big.Float).SetPrec(Precision).Add(
		new(big.Float).SetPrec(Precision).Sub(FromInt(1), b),
		new(big.Float).SetPrec(Precision).Mul(b, lengthRatio),
	)
	denominator := new(big.Float).SetPrec(Precision).Add(tfF, new(big.Float).SetPrec(Precision).Mul(k1, lengthPart))
	if denominator.Sign() <= 0 {
		return nil, fmt.Errorf("non-positive BM25 denominator")
	}
	factor := new(big.Float).SetPrec(Precision).Quo(numerator, denominator)
	return new(big.Float).SetPrec(Precision).Mul(idf, factor), nil
}

func Add(a, b *big.Float) *big.Float {
	return new(big.Float).SetPrec(Precision).Add(a, b)
}

func Decimal(v *big.Float, digits int) string {
	if v == nil {
		return "NaN"
	}
	return v.Text('g', digits)
}

func Float64(v *big.Float) (float64, error) {
	f, _ := v.Float64()
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("high precision value is not finite as float64")
	}
	return f, nil
}
