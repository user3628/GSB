package dsp

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"voiceline/internal/canonical"
	"voiceline/internal/model"
)

type SpeakerProfile struct {
	Number     int     `json:"number"`
	F0Scale    float64 `json:"f0_scale"`
	Brightness float64 `json:"brightness"`
}

func TargetSpeakerProfile(speakerID string) SpeakerProfile {
	digits := strings.Builder{}
	for _, r := range speakerID {
		if r >= '0' && r <= '9' {
			digits.WriteRune(r)
		}
	}
	number := 7
	if digits.Len() > 0 {
		if parsed, err := strconv.Atoi(digits.String()); err == nil {
			number = parsed
		}
	}
	return SpeakerProfile{
		Number:     number,
		F0Scale:    roundFloat(1+float64((number%5)-2)*0.012, 6),
		Brightness: roundFloat(0.92+0.008*float64(number%17), 6),
	}
}

type MappedFrame struct {
	Index         int       `json:"index"`
	SourceF0Hz    float64   `json:"source_f0_hz"`
	MappedF0Hz    float64   `json:"mapped_f0_hz"`
	TargetScale   float64   `json:"target_scale"`
	WarpedMel     []float64 `json:"warped_mel_log_energies"`
	MappingDigest string    `json:"mapping_digest"`
}

func MapParameters(segment model.SegmentManifest, table *FrameTable, f0Shift, envelopeWarp float64) ([]MappedFrame, error) {
	if f0Shift == 0 || math.IsNaN(f0Shift) || math.IsInf(f0Shift, 0) || f0Shift < 0 {
		frame := 0
		return nil, &model.BusinessError{
			Code:       model.CodeParamInvalid,
			SegmentID:  segment.ID,
			FrameIndex: &frame,
			Message:    "f0_shift must be a positive non-zero coefficient",
			Details:    map[string]any{"f0_shift": f0Shift},
		}
	}
	if envelopeWarp < model.MinEnvelopeWarp || envelopeWarp > model.MaxEnvelopeWarp ||
		math.IsNaN(envelopeWarp) || math.IsInf(envelopeWarp, 0) {
		frame := 0
		return nil, &model.BusinessError{
			Code:       model.CodeParamInvalid,
			SegmentID:  segment.ID,
			FrameIndex: &frame,
			Message:    fmt.Sprintf("envelope_warp must be within [%.1f, %.1f]", model.MinEnvelopeWarp, model.MaxEnvelopeWarp),
			Details:    map[string]any{"envelope_warp": envelopeWarp},
		}
	}

	profile := TargetSpeakerProfile(segment.TargetSpeaker)
	mapped := make([]MappedFrame, 0, len(table.Frames))
	for _, source := range table.Frames {
		warped := warpMel(source.MelLogEnergies, envelopeWarp)
		for band := range warped {
			targetGain := 1 + 0.045*math.Sin(float64(profile.Number)*0.31+float64(band)*0.47)
			warped[band] = roundFloat(warped[band]*profile.Brightness*targetGain, 6)
		}
		item := MappedFrame{
			Index:       source.Index,
			SourceF0Hz:  source.F0Hz,
			MappedF0Hz:  roundFloat(source.F0Hz*f0Shift*profile.F0Scale, 3),
			TargetScale: profile.F0Scale,
			WarpedMel:   warped,
		}
		_, digest, _ := canonical.JSON(item)
		item.MappingDigest = digest
		mapped = append(mapped, item)
	}
	return mapped, nil
}

func warpMel(values []float64, warp float64) []float64 {
	bands := len(values)
	out := make([]float64, bands)
	if bands == 0 {
		return out
	}
	for band := 0; band < bands; band++ {
		u := float64(band) / float64(bands-1)
		sourcePosition := math.Pow(u, warp) * float64(bands-1)
		left := int(math.Floor(sourcePosition))
		right := left + 1
		frac := sourcePosition - float64(left)
		if left < 0 {
			left = 0
		}
		if left >= bands-1 {
			out[band] = values[bands-1]
			continue
		}
		out[band] = roundFloat(values[left]*(1-frac)+values[right]*frac, 6)
	}
	return out
}

type RenderedBlock struct {
	Index       int     `json:"index"`
	FrameStart  int     `json:"frame_start"`
	FrameEnd    int     `json:"frame_end"`
	SampleStart int     `json:"sample_start"`
	SampleEnd   int     `json:"sample_end"`
	Samples     []int16 `json:"-"`
	ClampCount  int     `json:"clamp_count"`
	PCMDigest   string  `json:"pcm_digest"`
}

func BlockCount(table *FrameTable, blockFrames int) int {
	if blockFrames <= 0 {
		blockFrames = 1
	}
	return (len(table.Frames) + blockFrames - 1) / blockFrames
}

func RenderBlock(table *FrameTable, mapped []MappedFrame, blockIndex, blockFrames int, outputGain float64) (*RenderedBlock, error) {
	if blockFrames <= 0 {
		return nil, fmt.Errorf("block frames must be positive")
	}
	frameStart := blockIndex * blockFrames
	if frameStart >= len(table.Frames) {
		return nil, fmt.Errorf("block %d is outside frame table", blockIndex)
	}
	frameEnd := minInt(frameStart+blockFrames, len(table.Frames))
	hop := table.FrameShiftSamples
	sampleStart := frameStart * hop
	sampleEnd := minInt(frameEnd*hop, table.ExpectedSamples)
	if sampleEnd <= sampleStart {
		return nil, fmt.Errorf("block %d has empty sample range", blockIndex)
	}

	accum := make([]float64, sampleEnd-sampleStart)
	window := periodicHann(table.FrameLengthSamples)
	for frameIndex := maxInt(0, frameStart-1); frameIndex < frameEnd; frameIndex++ {
		grain := renderGrain(table, mapped[frameIndex], outputGain)
		for n, value := range grain {
			globalSample := frameIndex*hop + n
			if globalSample < sampleStart || globalSample >= sampleEnd {
				continue
			}
			accum[globalSample-sampleStart] += value * window[n]
		}
	}

	samples := make([]int16, len(accum))
	clamps := 0
	for i, value := range accum {
		if value > 1 {
			value = 1
			clamps++
		} else if value < -1 {
			value = -1
			clamps++
		}
		samples[i] = int16(math.Round(value * 32767))
	}
	pcm := int16ToBytes(samples)
	return &RenderedBlock{
		Index:       blockIndex,
		FrameStart:  frameStart,
		FrameEnd:    frameEnd,
		SampleStart: sampleStart,
		SampleEnd:   sampleEnd,
		Samples:     samples,
		ClampCount:  clamps,
		PCMDigest:   canonical.Bytes(pcm),
	}, nil
}

func renderGrain(table *FrameTable, mapped MappedFrame, outputGain float64) []float64 {
	nfft := table.FFTSize
	fb := table.FilterBank
	if fb == nil {
		fb = NewMelFilterBank(table.SampleRate, nfft, table.MelBands)
	}
	power := fb.InversePower(mapped.WarpedMel)
	binHz := float64(table.SampleRate) / float64(nfft)
	magnitude := make([]float64, nfft/2+1)
	for bin := 1; bin < nfft/2; bin++ {
		freq := float64(bin) * binHz
		envelope := math.Sqrt(power[bin])
		f0 := mapped.MappedF0Hz
		comb := 0.25
		if f0 > 1 {
			harmonic := math.Max(1, math.Round(freq/f0))
			harmonicBin := harmonic * f0 / binHz
			distance := math.Abs(float64(bin) - harmonicBin)
			comb += 1.75 * math.Exp(-(distance*distance)/(2*0.42*0.42))
		}
		magnitude[bin] = envelope * comb
	}

	real := make([]float64, nfft)
	imag := make([]float64, nfft)
	center := float64(table.FrameLengthSamples) / 2
	for bin := 1; bin < nfft/2; bin++ {
		phase := -2 * math.Pi * float64(bin) * center / float64(nfft)
		real[bin] = magnitude[bin] * math.Cos(phase)
		imag[bin] = magnitude[bin] * math.Sin(phase)
		real[nfft-bin] = real[bin]
		imag[nfft-bin] = -imag[bin]
	}
	IFFT(real, imag)

	length := table.FrameLengthSamples
	grain := make([]float64, length)
	copy(grain, real[:length])
	rms := rmsEnergy(grain)
	targetRMS := table.Frames[mapped.Index].RMSEnergy * 0.92
	if outputGain > 0 {
		targetRMS *= outputGain
	}
	if targetRMS < 0.015 {
		targetRMS = 0.015
	}
	if rms > 1e-9 {
		scale := targetRMS / rms
		for i := range grain {
			grain[i] *= scale
		}
	}
	return grain
}

func int16ToBytes(samples []int16) []byte {
	b := make([]byte, len(samples)*2)
	for i, sample := range samples {
		b[2*i] = byte(sample)
		b[2*i+1] = byte(uint16(sample) >> 8)
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
