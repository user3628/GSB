// Package reference contains the straightforward float64 reference path used
// to quantify fixed-point error. It intentionally uses Kahan summation so its
// rounding noise is much smaller than 8/4-bit quantization error.
package reference

import "math"

const Implementation = "float64-kahan-direct-v1"

func Dot(a, b []float64) float64 {
	var sum, compensation float64
	for i := range a {
		term := a[i]*b[i] - compensation
		next := sum + term
		compensation = (next - sum) - term
		sum = next
	}
	return sum
}

func Norm(v []float64) float64 {
	squares := make([]float64, len(v))
	for i, x := range v {
		squares[i] = x * x
	}
	return math.Sqrt(Dot(squares, ones(len(squares))))
}

func ones(n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = 1
	}
	return out
}

func Cosine(a, b []float64) float64 {
	if len(a) != len(b) {
		return 0
	}
	dot := Dot(a, b)
	na, nb := Norm(a), Norm(b)
	if na == 0 || nb == 0 {
		return 0
	}
	c := dot / (na * nb)
	return math.Max(-1, math.Min(1, c))
}

func MaxAbsDifference(a, b []float64) (maxAbs, sumSq float64) {
	for i := range a {
		d := a[i] - b[i]
		maxAbs = math.Max(maxAbs, math.Abs(d))
		sumSq += d * d
	}
	return maxAbs, math.Sqrt(sumSq)
}
