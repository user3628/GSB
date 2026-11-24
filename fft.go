package cockpitaudio

import "math"

func fftRadix2(re, im []float64) {
	n := len(re)
	if n <= 1 {
		return
	}
	for i, j := 1, 0; i < n; i++ {
		bit := n >> 1
		for ; j&bit != 0; bit >>= 1 {
			j ^= bit
		}
		j ^= bit
		if i < j {
			re[i], re[j] = re[j], re[i]
			im[i], im[j] = im[j], im[i]
		}
	}

	for length := 2; length <= n; length <<= 1 {
		half := length >> 1
		angle := -2 * math.Pi / float64(length)
		wRe := math.Cos(angle)
		wIm := math.Sin(angle)
		for start := 0; start < n; start += length {
			curRe, curIm := 1.0, 0.0
			for j := 0; j < half; j++ {
				a := start + j
				b := start + j + half
				tr := curRe*re[b] - curIm*im[b]
				ti := curRe*im[b] + curIm*re[b]
				re[b] = re[a] - tr
				im[b] = im[a] - ti
				re[a] += tr
				im[a] += ti
				nextRe := curRe*wRe - curIm*wIm
				curIm = curRe*wIm + curIm*wRe
				curRe = nextRe
			}
		}
	}
}
