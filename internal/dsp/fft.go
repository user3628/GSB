package dsp

import "math"

func nextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// FFT performs an in-place radix-2 Cooley-Tukey transform.
func FFT(real, imag []float64) {
	fft(real, imag, false)
}

// IFFT performs the inverse radix-2 transform and applies 1/N scaling.
func IFFT(real, imag []float64) {
	fft(real, imag, true)
	n := float64(len(real))
	for i := range real {
		real[i] /= n
		imag[i] /= n
	}
}

func fft(real, imag []float64, inverse bool) {
	n := len(real)
	if n == 0 || n&(n-1) != 0 {
		panic("fft length must be a non-zero power of two")
	}

	j := 0
	for i := 1; i < n; i++ {
		bit := n >> 1
		for j&bit != 0 {
			j ^= bit
			bit >>= 1
		}
		j ^= bit
		if i < j {
			real[i], real[j] = real[j], real[i]
			imag[i], imag[j] = imag[j], imag[i]
		}
	}

	sign := -1.0
	if inverse {
		sign = 1.0
	}
	for length := 2; length <= n; length <<= 1 {
		angle := sign * 2 * math.Pi / float64(length)
		wr, wi := math.Cos(angle), math.Sin(angle)
		half := length >> 1
		for start := 0; start < n; start += length {
			curR, curI := 1.0, 0.0
			for k := 0; k < half; k++ {
				a := start + k
				b := start + k + half
				tr := real[b]*curR - imag[b]*curI
				ti := real[b]*curI + imag[b]*curR
				real[b] = real[a] - tr
				imag[b] = imag[a] - ti
				real[a] += tr
				imag[a] += ti
				nextR := curR*wr - curI*wi
				curI = curR*wi + curI*wr
				curR = nextR
			}
		}
	}
}
