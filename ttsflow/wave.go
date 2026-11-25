package ttsflow

import (
	"encoding/binary"
	"math"
)

// ValidatePCMArtifact enforces the frame-shift to sample-count contract. The
// declared and actual values are sample counts per channel, not byte counts.
func ValidatePCMArtifact(uttID string, segmentSeq, frameCount, frameShiftSamples, channels int, pcm []byte) error {
	if channels <= 0 {
		return &BizError{Code: CodeConfigInvalid, Message: "channels must be positive", UTTID: uttID, SegmentSeq: segmentSeq}
	}
	declared := frameCount * frameShiftSamples
	bytesPerSample := channels * 2
	actual := len(pcm) / bytesPerSample
	if len(pcm)%bytesPerSample != 0 || actual != declared {
		return &BizError{
			Code: CodeLengthMismatch, Message: "PCM sample count does not match frames times frame shift",
			UTTID: uttID, SegmentSeq: segmentSeq, Stage: StageWave.String(),
			DeclaredValue: declared, ActualValue: actual,
		}
	}
	return nil
}

func synthesizePCM(sample TextSample, unit Unit, frames []AcousticFrame, frameStart int, cfg Config) ([]byte, int, error) {
	hop := cfg.Acoustic.FrameShiftSamples
	declaredSamples := len(frames) * hop
	channels := cfg.Wave.Channels
	pcm := make([]byte, declaredSamples*channels*2)
	sampleRate := float64(cfg.Wave.SampleRate)
	clamps := 0
	offset := 0
	for fi, frame := range frames {
		bands := len(frame.Energies)
		if bands == 0 {
			continue
		}
		for i := 0; i < hop; i++ {
			pos := (float64(i) + 0.5) / float64(hop)
			envelope := math.Pow(math.Sin(math.Pi*pos), 0.7)
			envelope *= 0.55 + 0.45*math.Exp(-float64(i)/float64(hop*2))
			globalSample := (frameStart+fi)*hop + i
			t := float64(globalSample) / sampleRate
			var value float64
			for b, energy := range frame.Energies {
				freq := 150.0*float64(b+1) + frame.F0*0.22
				phase := 2.0 * math.Pi * float64(b+1) / 7.0
				value += energy * math.Sin(2.0*math.Pi*freq*t+phase)
			}
			value = value / float64(bands) * 11500.0 * envelope
			iv := int32(math.Round(value))
			if iv > math.MaxInt16 {
				iv = math.MaxInt16
				clamps++
			} else if iv < math.MinInt16 {
				iv = math.MinInt16
				clamps++
			}
			for ch := 0; ch < channels; ch++ {
				binary.LittleEndian.PutUint16(pcm[offset:offset+2], uint16(int16(iv)))
				offset += 2
			}
		}
	}
	if err := ValidatePCMArtifact(sample.UTTID, unit.Index, len(frames), hop, channels, pcm); err != nil {
		return nil, clamps, err
	}
	return pcm, clamps, nil
}

// SynthesizeWave concatenates deterministic per-unit PCM artifacts.
func SynthesizeWave(sample TextSample, segments []AcousticSegment, cfg Config) ([]WaveSegment, []byte, error) {
	waves := make([]WaveSegment, 0, len(segments))
	var all []byte
	for _, seg := range segments {
		pcm, clamps, err := synthesizePCM(sample, seg.Unit, seg.Frames, seg.FrameStart, cfg)
		if err != nil {
			return nil, nil, err
		}
		hop := cfg.Acoustic.FrameShiftSamples
		wave := WaveSegment{
			SegmentSeq:  seg.SegmentSeq,
			Unit:        seg.Unit,
			ChunkIndex:  seg.Unit.ChunkIndex,
			FrameStart:  seg.FrameStart,
			FrameCount:  len(seg.Frames),
			SampleStart: seg.FrameStart * hop,
			SampleEnd:   (seg.FrameStart + len(seg.Frames)) * hop,
			Channels:    cfg.Wave.Channels,
			Bytes:       pcm,
			Digest:      digestBytes(pcm),
			ClampCount:  clamps,
		}
		waves = append(waves, wave)
		all = append(all, pcm...)
	}
	return waves, all, nil
}

func buildChunkAlignments(frontend FrontendResult, waves []WaveSegment) []ChunkAlignment {
	alignments := make([]ChunkAlignment, 0, len(frontend.Chunks))
	for _, chunk := range frontend.Chunks {
		alignment := ChunkAlignment{ChunkIndex: chunk.Index, UnitStart: chunk.UnitStart, UnitEnd: chunk.UnitEnd}
		for _, wave := range waves {
			if wave.SegmentSeq < chunk.UnitStart || wave.SegmentSeq >= chunk.UnitEnd {
				continue
			}
			if len(alignment.UnitIndices) == 0 {
				alignment.SampleStart = wave.SampleStart
			}
			alignment.SampleEnd = wave.SampleEnd
			alignment.Frames += wave.FrameCount
			alignment.UnitIndices = append(alignment.UnitIndices, wave.SegmentSeq)
		}
		if len(alignment.UnitIndices) > 0 {
			alignments = append(alignments, alignment)
		}
	}
	return alignments
}

func statsForSegment(seg AcousticSegment, wave WaveSegment, fingerprints stageFingerprints) SegmentStats {
	stats := SegmentStats{
		SegmentSeq:          seg.SegmentSeq,
		UnitID:              seg.Unit.ID,
		ChunkIndex:          seg.Unit.ChunkIndex,
		FrameStart:          seg.FrameStart,
		FrameCount:          len(seg.Frames),
		SampleStart:         wave.SampleStart,
		SampleEnd:           wave.SampleEnd,
		ClampCount:          wave.ClampCount,
		AcousticDigest:      seg.Digest,
		PCMDigest:           wave.Digest,
		FrontendFingerprint: fingerprints.Frontend,
		AcousticFingerprint: fingerprints.Acoustic,
		WaveFingerprint:     fingerprints.Wave,
	}
	stats.EnergyMin = math.Inf(1)
	stats.EnergyMax = math.Inf(-1)
	stats.F0Min = math.Inf(1)
	stats.F0Max = math.Inf(-1)
	energySum := 0.0
	energyCount := 0
	for _, frame := range seg.Frames {
		if frame.F0 < stats.F0Min {
			stats.F0Min = frame.F0
		}
		if frame.F0 > stats.F0Max {
			stats.F0Max = frame.F0
		}
		for _, e := range frame.Energies {
			if e < stats.EnergyMin {
				stats.EnergyMin = e
			}
			if e > stats.EnergyMax {
				stats.EnergyMax = e
			}
			energySum += e
			energyCount++
		}
	}
	if energyCount > 0 {
		stats.EnergyMean = energySum / float64(energyCount)
	} else {
		stats.EnergyMin, stats.EnergyMax = 0, 0
	}
	if len(seg.Frames) == 0 {
		stats.F0Min, stats.F0Max = 0, 0
	}
	var sumSquares float64
	for i := 0; i+1 < len(wave.Bytes); i += 2 {
		v := int(binary.LittleEndian.Uint16(wave.Bytes[i : i+2]))
		sv := int(int16(v))
		if abs := int(math.Abs(float64(sv))); abs > stats.PCMPeak {
			stats.PCMPeak = abs
		}
		sumSquares += float64(sv) * float64(sv)
	}
	samples := len(wave.Bytes) / 2
	if samples > 0 {
		stats.RMS = math.Sqrt(sumSquares / float64(samples))
	}
	return stats
}
