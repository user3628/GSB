package speakerdiar

import (
	"math"
	"math/bits"
)

// ComplexBin is a JSON-safe complex spectrum bin.
type ComplexBin struct {
	Real float64 `json:"re"`
	Imag float64 `json:"im"`
}

func (b ComplexBin) magnitude() float64 {
	return math.Hypot(b.Real, b.Imag)
}

// FrameSpectrum contains one padded, windowed STFT frame and scalar diagnostics.
type FrameSpectrum struct {
	Index          int          `json:"index"`
	StartSample    int          `json:"start_sample"`
	FrameSamples   int          `json:"frame_samples"`
	HopSamples     int          `json:"hop_samples"`
	Padded         bool         `json:"padded"`
	PaddingSamples int          `json:"padding_samples"`
	StartMS        int          `json:"start_ms"`
	CenterMS       int          `json:"center_ms"`
	Bins           []ComplexBin `json:"bins"`
	Magnitudes     []float64    `json:"magnitudes"`
	LogEnergy      float64      `json:"log_energy"`
	Energy         float64      `json:"energy"`
	RMS            float64      `json:"rms"`
	Flatness       float64      `json:"flatness"`
}

// SpectralResult is the first pipeline stage product.
type SpectralResult struct {
	ChunkID      string          `json:"chunk_id"`
	SampleCount  int             `json:"sample_count"`
	SampleRate   int             `json:"sample_rate"`
	FrameLength  int             `json:"frame_length"`
	HopLength    int             `json:"hop_length"`
	FFTSize      int             `json:"fft_size"`
	FrameCount   int             `json:"frame_count"`
	PaddedFrames []int           `json:"padded_frames"`
	Frames       []FrameSpectrum `json:"frames"`
	Digest       string          `json:"digest"`
}

type spectralDigestFrame struct {
	Index     int          `json:"index"`
	Padded    bool         `json:"padded"`
	LogEnergy float64      `json:"log_energy"`
	Flatness  float64      `json:"flatness"`
	Bins      []ComplexBin `json:"bins"`
}

type spectralDigestData struct {
	ChunkID     string                `json:"chunk_id"`
	SampleCount int                   `json:"sample_count"`
	SampleRate  int                   `json:"sample_rate"`
	FrameLength int                   `json:"frame_length"`
	HopLength   int                   `json:"hop_length"`
	FFTSize     int                   `json:"fft_size"`
	Frames      []spectralDigestFrame `json:"frames"`
}

// NextPow2 returns the smallest power of two at least n.
func NextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// HannWindow returns the symmetric Hann window used for every frame.
func HannWindow(n int) []float64 {
	w := make([]float64, n)
	if n == 1 {
		w[0] = 1
		return w
	}
	for i := 0; i < n; i++ {
		w[i] = 0.5 * (1 - math.Cos(2*math.Pi*float64(i)/float64(n-1)))
	}
	return w
}

// FFT performs an in-place radix-two complex FFT. n must be a power of two.
func FFT(real, imag []float64, inverse bool) {
	n := len(real)
	if n != len(imag) || n == 0 || n&(n-1) != 0 {
		panic("FFT requires equal non-empty power-of-two slices")
	}
	for i, j := 1, 0; i < n; i++ {
		bit := n >> 1
		for ; j&bit != 0; bit >>= 1 {
			j ^= bit
		}
		j ^= bit
		if i < j {
			real[i], real[j] = real[j], real[i]
			imag[i], imag[j] = imag[j], imag[i]
		}
	}

	for length := 2; length <= n; length <<= 1 {
		angle := 2 * math.Pi / float64(length)
		if inverse {
			angle = -angle
		}
		wr := math.Cos(angle)
		wi := math.Sin(angle)
		for start := 0; start < n; start += length {
			cr, ci := 1.0, 0.0
			half := length / 2
			for j := 0; j < half; j++ {
				a := start + j
				b := a + half
				tr := real[b]*cr - imag[b]*ci
				ti := real[b]*ci + imag[b]*cr
				real[b] = real[a] - tr
				imag[b] = imag[a] - ti
				real[a] += tr
				imag[a] += ti
				newCR := cr*wr - ci*wi
				ci = cr*wi + ci*wr
				cr = newCR
			}
		}
	}
	if inverse {
		for i := range real {
			real[i] /= float64(n)
			imag[i] /= float64(n)
		}
	}
}

// AnalyzeSpectrum frames float PCM and computes STFT complex bins and descriptors.
func AnalyzeSpectrum(chunkID string, samples []float64, cfg Config) (*SpectralResult, error) {
	cfg = cfg.withDefaults()
	if err := validateBasicConfig(cfg); err != nil {
		return nil, err
	}
	frameLen := cfg.FrameSamples()
	hop := cfg.HopSamples()
	fftSize := cfg.FFTSize
	if fftSize < frameLen {
		fftSize = NextPow2(frameLen)
	}
	window := HannWindow(frameLen)
	binCount := fftSize/2 + 1
	frameCount := 0
	if len(samples) > 0 {
		frameCount = (len(samples) + hop - 1) / hop
	}

	out := &SpectralResult{
		ChunkID: chunkID, SampleCount: len(samples), SampleRate: cfg.SampleRate,
		FrameLength: frameLen, HopLength: hop, FFTSize: fftSize, FrameCount: frameCount,
		Frames: make([]FrameSpectrum, 0, frameCount),
	}
	digestData := spectralDigestData{
		ChunkID: chunkID, SampleCount: len(samples), SampleRate: cfg.SampleRate,
		FrameLength: frameLen, HopLength: hop, FFTSize: fftSize,
		Frames: make([]spectralDigestFrame, 0, frameCount),
	}

	for frameIndex := 0; frameIndex < frameCount; frameIndex++ {
		start := frameIndex * hop
		end := start + frameLen
		padded := end > len(samples)
		padding := 0
		if padded {
			padding = end - len(samples)
		}
		real := make([]float64, fftSize)
		imag := make([]float64, fftSize)
		energy := 0.0
		validCount := 0
		for j := 0; j < frameLen; j++ {
			idx := start + j
			v := 0.0
			if idx < len(samples) {
				v = samples[idx]
				validCount++
			}
			wv := v * window[j]
			if math.IsNaN(wv) || math.IsInf(wv, 0) {
				return nil, numericError(chunkID, frameIndex, "windowed sample is non-finite")
			}
			real[j] = wv
			energy += wv * wv
		}
		FFT(real, imag, false)

		bins := make([]ComplexBin, binCount)
		mags := make([]float64, binCount)
		arithmetic := 0.0
		logSum := 0.0
		zeroMagnitude := false
		for b := 0; b < binCount; b++ {
			if !isFinite(real[b]) || !isFinite(imag[b]) {
				return nil, numericError(chunkID, frameIndex, "STFT bin is NaN or Inf")
			}
			bins[b] = ComplexBin{Real: real[b], Imag: imag[b]}
			m := bins[b].magnitude()
			if !isFinite(m) {
				return nil, numericError(chunkID, frameIndex, "spectral magnitude is NaN or Inf")
			}
			mags[b] = m
			arithmetic += m
			if m == 0 {
				zeroMagnitude = true
			} else {
				logSum += math.Log(m)
			}
		}
		energy /= float64(maxInt(validCount, 1))
		logEnergy := math.Log(energy + cfg.FrameEnergyFloor)
		rms := math.Sqrt(energy)
		flatness := 0.0
		if arithmetic > 0 && !zeroMagnitude {
			geometric := math.Exp(logSum / float64(binCount))
			flatness = geometric / (arithmetic / float64(binCount))
		}
		for name, v := range map[string]float64{"log_energy": logEnergy, "flatness": flatness, "energy": energy, "rms": rms} {
			if !isFinite(v) {
				_ = name
				return nil, numericError(chunkID, frameIndex, "spectral descriptor is NaN or Inf")
			}
		}

		startMS := cfg.SamplesToMS(start)
		center := start + frameLen/2
		frame := FrameSpectrum{
			Index: frameIndex, StartSample: start, FrameSamples: frameLen, HopSamples: hop,
			Padded: padded, PaddingSamples: padding, StartMS: startMS,
			CenterMS: cfg.SamplesToMS(center), Bins: bins, Magnitudes: mags,
			LogEnergy: logEnergy, Energy: energy, RMS: rms, Flatness: flatness,
		}
		out.Frames = append(out.Frames, frame)
		digestData.Frames = append(digestData.Frames, spectralDigestFrame{
			Index: frameIndex, Padded: padded, LogEnergy: logEnergy,
			Flatness: flatness, Bins: bins,
		})
		if padded {
			out.PaddedFrames = append(out.PaddedFrames, frameIndex)
		}
	}
	d, raw, err := DigestValue(digestData)
	if err != nil {
		return nil, err
	}
	if err := assertCanonicalFinite(raw, chunkID); err != nil {
		return nil, err
	}
	out.Digest = d
	return out, nil
}

func numericError(chunkID string, frame int, msg string) *Error {
	de := newError(CodeNumeric, msg)
	de.ChunkID = chunkID
	de.Stage = StageFrame
	de.FrameIndex = frame
	return de
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func assertCanonicalFinite(raw []byte, chunkID string) error {
	if len(raw) == 0 {
		return numericError(chunkID, 0, "empty spectral digest")
	}
	return nil
}
