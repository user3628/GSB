package dsp

import "math"

type MelFilterBank struct {
	SampleRate int
	FFTSize    int
	Bands      int
	LowHz      float64
	HighHz     float64
	Weights    [][]float64
	RowSum     []float64
	BinCount   int
}

func hzToMel(hz float64) float64 {
	return 1127 * math.Log(1+hz/700)
}

func melToHz(mel float64) float64 {
	return 700 * (math.Exp(mel/1127) - 1)
}

func NewMelFilterBank(sampleRate, fftSize, bands int) *MelFilterBank {
	low := 80.0
	high := math.Min(7600, float64(sampleRate)/2*0.95)
	if high <= low {
		high = float64(sampleRate) / 2
	}
	binCount := fftSize/2 + 1
	fb := &MelFilterBank{
		SampleRate: sampleRate,
		FFTSize:    fftSize,
		Bands:      bands,
		LowHz:      low,
		HighHz:     high,
		Weights:    make([][]float64, bands),
		RowSum:     make([]float64, bands),
		BinCount:   binCount,
	}

	points := make([]float64, bands+2)
	lowMel, highMel := hzToMel(low), hzToMel(high)
	for i := range points {
		mel := lowMel + (highMel-lowMel)*float64(i)/float64(bands+1)
		points[i] = melToHz(mel)
	}

	for band := 0; band < bands; band++ {
		fb.Weights[band] = make([]float64, binCount)
		left := points[band]
		center := points[band+1]
		right := points[band+2]
		for bin := 0; bin < binCount; bin++ {
			freq := float64(bin) * float64(sampleRate) / float64(fftSize)
			weight := 0.0
			if freq > left && freq < center && center > left {
				weight = (freq - left) / (center - left)
			} else if freq >= center && freq < right && right > center {
				weight = (right - freq) / (right - center)
			}
			if weight > 0 {
				fb.Weights[band][bin] = weight
				fb.RowSum[band] += weight
			}
		}
		if fb.RowSum[band] == 0 {
			fb.RowSum[band] = 1
		}
	}
	return fb
}

func (fb *MelFilterBank) Energies(power []float64) []float64 {
	out := make([]float64, fb.Bands)
	for band := 0; band < fb.Bands; band++ {
		energy := 0.0
		for bin, weight := range fb.Weights[band] {
			if weight != 0 && bin < len(power) {
				energy += power[bin] * weight
			}
		}
		out[band] = roundFloat(math.Log1p(energy), 6)
	}
	return out
}

func (fb *MelFilterBank) InversePower(logEnergies []float64) []float64 {
	power := make([]float64, fb.BinCount)
	for band, logged := range logEnergies {
		linear := math.Max(0, math.Exp(logged)-1)
		rowSum := fb.RowSum[band]
		for bin, weight := range fb.Weights[band] {
			if weight > 0 {
				power[bin] += linear * weight / rowSum
			}
		}
	}
	for bin := range power {
		if power[bin] < 1e-10 {
			power[bin] = 1e-10
		}
	}
	return power
}

func roundFloat(v float64, digits int) float64 {
	pow := math.Pow10(digits)
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*pow) / pow
}
