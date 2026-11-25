package dsp

import (
	"math"

	"voiceline/internal/canonical"
	"voiceline/internal/model"
)

const MelBandCount = 24

type FrameParams struct {
	Index               int       `json:"index"`
	StartSample         int       `json:"start_sample"`
	CenterSample        int       `json:"center_sample"`
	FrameLengthSamples  int       `json:"frame_length_samples"`
	F0Hz                float64   `json:"f0_hz"`
	Voiced              bool      `json:"voiced"`
	AutocorrelationPeak float64   `json:"autocorrelation_peak"`
	RMSEnergy           float64   `json:"rms_energy"`
	MelLogEnergies      []float64 `json:"mel_log_energies"`
	MelDigest           string    `json:"mel_digest"`
	ParamDigest         string    `json:"param_digest"`
}

type TableSummary struct {
	FrameCount       int     `json:"frame_count"`
	VoicedFrameCount int     `json:"voiced_frame_count"`
	MeanF0Hz         float64 `json:"mean_f0_hz"`
	MelEnergyDigest  string  `json:"mel_energy_digest"`
	ParamTableDigest string  `json:"param_table_digest"`
}

type FrameTable struct {
	SchemaVersion      string         `json:"schema_version"`
	SegmentID          string         `json:"segment_id"`
	SampleRate         int            `json:"sample_rate"`
	ManifestSampleRate int            `json:"manifest_sample_rate"`
	FrameShiftMS       float64        `json:"frame_shift_ms"`
	FrameShiftSamples  int            `json:"frame_shift_samples"`
	FrameLengthMS      float64        `json:"frame_length_ms"`
	FrameLengthSamples int            `json:"frame_length_samples"`
	FFTSize            int            `json:"fft_size"`
	MelBands           int            `json:"mel_bands"`
	Window             string         `json:"window"`
	ExpectedSamples    int            `json:"expected_samples"`
	InputFingerprint   string         `json:"input_fingerprint"`
	Frames             []FrameParams  `json:"frames"`
	Summary            TableSummary   `json:"summary"`
	FilterBank         *MelFilterBank `json:"-"`
	windowCoefficients []float64      `json:"-"`
}

func HopSamples(frameShiftMS float64, sampleRate int) int {
	return int(math.Round(frameShiftMS * float64(sampleRate) / 1000))
}

func Analyze(segment model.SegmentManifest, manifestSampleRate, fileSampleRate int, pcm []int16, inputFingerprint string) (*FrameTable, error) {
	if fileSampleRate != manifestSampleRate {
		return nil, &model.BusinessError{
			Code:      model.CodeSampleRateConflict,
			SegmentID: segment.ID,
			Message:   "WAV sample rate conflicts with manifest",
			Details: map[string]any{
				"manifest_sample_rate": manifestSampleRate,
				"wav_sample_rate":      fileSampleRate,
			},
		}
	}
	if segment.FrameShiftMS == 0 {
		return nil, model.NewError(model.CodeFrameShiftZero, segment.ID, "frame_shift_ms must not be zero")
	}

	hop := HopSamples(segment.FrameShiftMS, manifestSampleRate)
	if hop <= 0 {
		return nil, model.NewError(model.CodeFrameShiftZero, segment.ID,
			"frame shift %g ms converts to %d samples", segment.FrameShiftMS, hop)
	}
	frameLengthMS := segment.EffectiveFrameLengthMS()
	frameLength := HopSamples(frameLengthMS, manifestSampleRate)
	if frameLength < 3 {
		frameLength = 3
	}
	fftSize := nextPow2(frameLength)
	if fftSize < 256 {
		fftSize = 256
	}
	expectedSamples := segment.EffectiveExpectedSamples()
	frameCount := (expectedSamples + hop - 1) / hop
	window := periodicHann(frameLength)
	filters := NewMelFilterBank(manifestSampleRate, fftSize, MelBandCount)

	table := &FrameTable{
		SchemaVersion:      "frame-table.v1",
		SegmentID:          segment.ID,
		SampleRate:         manifestSampleRate,
		ManifestSampleRate: manifestSampleRate,
		FrameShiftMS:       segment.FrameShiftMS,
		FrameShiftSamples:  hop,
		FrameLengthMS:      frameLengthMS,
		FrameLengthSamples: frameLength,
		FFTSize:            fftSize,
		MelBands:           MelBandCount,
		Window:             "periodic-hann",
		ExpectedSamples:    expectedSamples,
		InputFingerprint:   inputFingerprint,
		Frames:             make([]FrameParams, 0, frameCount),
		FilterBank:         filters,
		windowCoefficients: window,
	}

	floatPCM := make([]float64, len(pcm))
	for i, sample := range pcm {
		floatPCM[i] = float64(sample) / 32768.0
	}

	voicedCount := 0
	f0Sum := 0.0
	melDigests := make([]string, 0, frameCount)
	for index := 0; index < frameCount; index++ {
		start := index * hop
		samples := readPadded(floatPCM, start, frameLength)
		rms := rmsEnergy(samples)
		windowed := make([]float64, frameLength)
		for i := range samples {
			windowed[i] = samples[i] * window[i]
		}

		f0, voiced, peak := autocorrelationF0(windowed, manifestSampleRate)
		if voiced {
			voicedCount++
			f0Sum += f0
		}

		real := make([]float64, fftSize)
		imag := make([]float64, fftSize)
		copy(real, windowed)
		FFT(real, imag)
		power := make([]float64, fftSize/2+1)
		for bin := range power {
			power[bin] = (real[bin]*real[bin] + imag[bin]*imag[bin]) / float64(fftSize)
		}
		mel := filters.Energies(power)
		_, melDigest, err := canonical.JSON(mel)
		if err != nil {
			return nil, err
		}

		frame := FrameParams{
			Index:               index,
			StartSample:         start,
			CenterSample:        start + frameLength/2,
			FrameLengthSamples:  frameLength,
			F0Hz:                f0,
			Voiced:              voiced,
			AutocorrelationPeak: roundFloat(peak, 6),
			RMSEnergy:           roundFloat(rms, 8),
			MelLogEnergies:      mel,
			MelDigest:           melDigest,
		}
		frame.ParamDigest = hashFrameWithoutDigest(frame)
		table.Frames = append(table.Frames, frame)
		melDigests = append(melDigests, melDigest)
	}

	meanF0 := 0.0
	if voicedCount > 0 {
		meanF0 = roundFloat(f0Sum/float64(voicedCount), 3)
	}
	_, melSummaryDigest, _ := canonical.JSON(melDigests)
	table.Summary = TableSummary{
		FrameCount:       frameCount,
		VoicedFrameCount: voicedCount,
		MeanF0Hz:         meanF0,
		MelEnergyDigest:  melSummaryDigest,
	}

	// Hash the complete deterministic JSON table after summary fields are populated.
	tableJSON, digest, err := canonical.JSON(table)
	if err != nil {
		return nil, err
	}
	_ = tableJSON
	table.Summary.ParamTableDigest = digest
	return table, nil
}

func hashFrameWithoutDigest(frame FrameParams) string {
	frame.ParamDigest = ""
	_, digest, _ := canonical.JSON(frame)
	return digest
}

func periodicHann(n int) []float64 {
	w := make([]float64, n)
	for i := 0; i < n; i++ {
		w[i] = 0.5 * (1 - math.Cos(2*math.Pi*float64(i)/float64(n)))
	}
	return w
}

func readPadded(pcm []float64, start, n int) []float64 {
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		idx := start + i
		if idx >= 0 && idx < len(pcm) {
			out[i] = pcm[idx]
		}
	}
	return out
}

func rmsEnergy(samples []float64) float64 {
	sum := 0.0
	for _, v := range samples {
		sum += v * v
	}
	return math.Sqrt(sum / float64(len(samples)))
}

func autocorrelationF0(x []float64, sampleRate int) (float64, bool, float64) {
	n := len(x)
	minLag := sampleRate / 400
	maxLag := sampleRate / 70
	if maxLag >= n-2 {
		maxLag = n - 2
	}
	if minLag < 2 {
		minLag = 2
	}
	if maxLag <= minLag+1 {
		return 0, false, 0
	}

	energy0 := 0.0
	for _, v := range x {
		energy0 += v * v
	}
	if energy0 < 1e-7 {
		return 0, false, 0
	}

	corr := make([]float64, maxLag+2)
	bestLag := 0
	best := -1.0
	for lag := minLag; lag <= maxLag; lag++ {
		sum := 0.0
		energyLag := 0.0
		for i := 0; i+lag < n; i++ {
			sum += x[i] * x[i+lag]
			energyLag += x[i+lag] * x[i+lag]
		}
		denom := math.Sqrt(energy0 * energyLag)
		if denom > 0 {
			corr[lag] = sum / denom
		}
		if corr[lag] > best {
			best = corr[lag]
			bestLag = lag
		}
	}

	if bestLag <= minLag || bestLag >= maxLag || best < 0.22 {
		return roundFloat(float64(sampleRate)/float64(bestLag), 3), best >= 0.22, roundFloat(best, 6)
	}

	alpha := corr[bestLag-1]
	beta := corr[bestLag]
	gamma := corr[bestLag+1]
	denom := alpha - 2*beta + gamma
	delta := 0.0
	if math.Abs(denom) > 1e-12 {
		delta = 0.5 * (alpha - gamma) / denom
		if delta > 1 {
			delta = 1
		} else if delta < -1 {
			delta = -1
		}
	}
	lagFloat := float64(bestLag) + delta
	if lagFloat <= 0 {
		return 0, false, roundFloat(best, 6)
	}
	return roundFloat(float64(sampleRate)/lagFloat, 3), true, roundFloat(best, 6)
}
