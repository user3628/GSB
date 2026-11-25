package speakerdiar

import (
	"math"
)

// MaskRound records mask movement after one deterministic update round.
type MaskRound struct {
	Round        int       `json:"round"`
	FrameChanges []float64 `json:"frame_changes"`
	MeanChange   float64   `json:"mean_change"`
	MaxChange    float64   `json:"max_change"`
	Converged    bool      `json:"converged"`
}

// SourceTrack is one separated waveform and per-hop energy.
type SourceTrack struct {
	SourceIndex int       `json:"source_index"`
	PCM         []int16   `json:"pcm"`
	FrameRMS    []float64 `json:"frame_rms"`
	Digest      string    `json:"digest"`
	Samples     []float64 `json:"-"`
}

// FloatTrack returns float samples, restoring them from unmarshalled PCM if needed.
func (t *SourceTrack) FloatTrack() []float64 {
	if len(t.Samples) == len(t.PCM) {
		return t.Samples
	}
	t.Samples = PCMToFloat(t.PCM)
	return t.Samples
}

// SeparationResult is the second pipeline stage product.
type SeparationResult struct {
	ChunkID        string         `json:"chunk_id"`
	SpeakerCount   int            `json:"speaker_count"`
	Iterations     int            `json:"iterations"`
	Converged      bool           `json:"converged"`
	ConvergedRound int            `json:"converged_round"`
	Rounds         []MaskRound    `json:"rounds"`
	Tracks         []SourceTrack  `json:"tracks"`
	Diagnostics    map[string]any `json:"diagnostics,omitempty"`
	Digest         string         `json:"digest"`
	Masks          [][][]float64  `json:"-"`
}

type separationDigestData struct {
	ChunkID      string        `json:"chunk_id"`
	SpeakerCount int           `json:"speaker_count"`
	Iterations   int           `json:"iterations"`
	Rounds       []MaskRound   `json:"rounds"`
	Tracks       []SourceTrack `json:"tracks"`
}

// SeparateSources performs deterministic iterative amplitude-mask estimation.
func SeparateSources(spec *SpectralResult, speakerCount int, cfg Config, injection Injection) (*SeparationResult, error) {
	cfg = cfg.withDefaults()
	if spec == nil {
		return nil, newError(CodeSeparation, "missing spectral result")
	}
	if speakerCount <= 0 {
		de := newError(CodeInvalidParam, "speaker count must be positive")
		de.ChunkID = spec.ChunkID
		de.Stage = StageSeparate
		de.Details = map[string]any{"speaker_count": speakerCount}
		return nil, de
	}
	if speakerCount > cfg.MaxSpeakers || injection == InjectTooManySpeakers {
		count := speakerCount
		if injection == InjectTooManySpeakers {
			count = cfg.MaxSpeakers + 1
		}
		de := newError(CodeSeparation, "speaker count exceeds configured upper limit")
		de.ChunkID = spec.ChunkID
		de.Stage = StageSeparate
		de.SourceIndex = -1
		de.Details = map[string]any{"speaker_count": count, "max_speakers": cfg.MaxSpeakers}
		return &SeparationResult{ChunkID: spec.ChunkID, SpeakerCount: count}, de
	}
	if cfg.MaskMaxIterations < 0 {
		de := newError(CodeInvalidParam, "mask iterations must not be negative")
		de.ChunkID = spec.ChunkID
		de.Stage = StageSeparate
		return nil, de
	}

	frames := len(spec.Frames)
	bins := spec.FFTSize/2 + 1
	magnitudes := make([][]float64, frames)
	for f := range spec.Frames {
		magnitudes[f] = spec.Frames[f].Magnitudes
	}

	profiles := initializeProfiles(speakerCount, bins, cfg.SampleRate, spec.FFTSize)
	activations := make([][]float64, speakerCount)
	masks := make([][][]float64, speakerCount)
	for k := 0; k < speakerCount; k++ {
		activations[k] = make([]float64, frames)
		masks[k] = make([][]float64, frames)
		for f := 0; f < frames; f++ {
			activations[k][f] = math.Max(spec.Frames[f].RMS, cfg.FrameEnergyFloor) * (0.85 + 0.15*float64(k+1)/float64(speakerCount))
			masks[k][f] = make([]float64, bins)
		}
	}

	result := &SeparationResult{ChunkID: spec.ChunkID, SpeakerCount: speakerCount, Diagnostics: map[string]any{}}
	convergenceStreak := 0
	previous := zeroMasks(speakerCount, frames, bins)

	for round := 1; round <= cfg.MaskMaxIterations; round++ {
		raw := make([][][]float64, speakerCount)
		for k := range raw {
			raw[k] = make([][]float64, frames)
			for f := range raw[k] {
				raw[k][f] = make([]float64, bins)
			}
		}
		for k := 0; k < speakerCount; k++ {
			for f := 0; f < frames; f++ {
				for b := 0; b < bins; b++ {
					raw[k][f][b] = activations[k][f]*profiles[k][b] + 1e-12
				}
			}
		}
		newMasks := zeroMasks(speakerCount, frames, bins)
		// Explicit normalization avoids the awkward bIndex placeholder above.
		for f := 0; f < frames; f++ {
			for b := 0; b < bins; b++ {
				sum := 0.0
				for k := 0; k < speakerCount; k++ {
					sum += raw[k][f][b]
				}
				powered := make([]float64, speakerCount)
				powerSum := 0.0
				for k := 0; k < speakerCount; k++ {
					m := 0.0
					if sum > 0 {
						m = raw[k][f][b] / sum
					}
					m = math.Pow(m, cfg.MaskSharpness)
					powered[k] = m
					powerSum += m
				}
				for k := 0; k < speakerCount; k++ {
					m := 0.0
					if powerSum > 0 {
						m = powered[k] / powerSum
					}
					newMasks[k][f][b] = (1-cfg.MaskFloor)*m + cfg.MaskFloor/float64(speakerCount)
				}
			}
		}
		if injection == InjectZeroMaskedSource && round == 1 {
			for f := 0; f < frames; f++ {
				for b := 0; b < bins; b++ {
					newMasks[0][f][b] = 0
				}
			}
		}

		record := MaskRound{Round: round, FrameChanges: make([]float64, frames)}
		totalChange := 0.0
		maxChange := 0.0
		cells := float64(speakerCount * bins)
		for f := 0; f < frames; f++ {
			change := 0.0
			for k := 0; k < speakerCount; k++ {
				for b := 0; b < bins; b++ {
					d := math.Abs(newMasks[k][f][b] - previous[k][f][b])
					change += d
				}
			}
			change /= cells
			record.FrameChanges[f] = change
			totalChange += change
			if change > maxChange {
				maxChange = change
			}
		}
		if frames > 0 {
			record.MeanChange = totalChange / float64(frames)
		}
		record.MaxChange = maxChange
		record.Converged = record.MeanChange < cfg.MaskEpsilon
		result.Rounds = append(result.Rounds, record)
		if injection == InjectZeroMaskedSource {
			de := newError(CodeSeparation, "mask multiplication zeroed the complete source")
			de.ChunkID = spec.ChunkID
			de.Stage = StageSeparate
			de.SourceIndex = 0
			de.Round = round
			de.Details = map[string]any{
				"reason": "injected_zero_mask", "mean_change": record.MeanChange,
				"frame_count": frames, "bin_count": bins,
			}
			result.Diagnostics["zero_source"] = 0
			return result, de
		}

		updateFactors(newMasks, magnitudes, profiles, activations, cfg)
		previous = newMasks
		masks = newMasks

		if record.Converged {
			convergenceStreak++
			if convergenceStreak >= cfg.ConvergenceRounds {
				result.Converged = true
				result.ConvergedRound = round
				break
			}
		} else {
			convergenceStreak = 0
		}
	}
	if result.ConvergedRound == 0 && len(result.Rounds) > 0 {
		result.ConvergedRound = len(result.Rounds)
	}
	result.Iterations = len(result.Rounds)
	result.Masks = masks

	tracks, err := reconstructTracks(spec, masks, cfg)
	if err != nil {
		return result, err
	}
	for k := range tracks {
		totalEnergy := 0.0
		for _, v := range tracks[k].FloatTrack() {
			totalEnergy += v * v
		}
		if totalEnergy == 0 {
			round := result.Iterations
			de := newError(CodeSeparation, "separated source has zero energy in every frame")
			de.ChunkID = spec.ChunkID
			de.Stage = StageSeparate
			de.SourceIndex = k
			de.Round = round
			de.Details = map[string]any{
				"reason":      "mask_product_zero_or_silent_source",
				"frame_count": frames,
				"bin_count":   bins,
				"diagnostics": result.Diagnostics,
			}
			result.Diagnostics["zero_source"] = k
			return result, de
		}
	}
	result.Tracks = tracks

	digestData := separationDigestData{
		ChunkID: result.ChunkID, SpeakerCount: result.SpeakerCount,
		Iterations: result.Iterations, Rounds: result.Rounds, Tracks: result.Tracks,
	}
	d := mustDigestValue(digestData)
	for i := range result.Tracks {
		result.Tracks[i].Digest = mustDigestValue(result.Tracks[i])
	}
	result.Digest = d
	return result, nil
}

func zeroMasks(k, f, b int) [][][]float64 {
	m := make([][][]float64, k)
	for i := range m {
		m[i] = make([][]float64, f)
		for j := range m[i] {
			m[i][j] = make([]float64, b)
		}
	}
	return m
}

func initializeProfiles(k, bins, rate, fftSize int) [][]float64 {
	profiles := make([][]float64, k)
	candidates := []float64{112, 155, 198, 246}
	for source := 0; source < k; source++ {
		f0 := candidates[0]
		if k == 3 {
			f0 = candidates[1+source]
		} else if k == 4 {
			f0 = candidates[source]
		} else if k == 2 {
			f0 = []float64{118, 172}[source]
		} else {
			f0 = 112 * math.Pow(2.1, float64(source)/math.Max(1, float64(k-1)))
		}
		p := make([]float64, bins)
		for b := 0; b < bins; b++ {
			freq := float64(b) * float64(rate) / float64(fftSize)
			v := 0.0
			for h := 1; h <= 16; h++ {
				center := f0 * float64(h)
				width := 18 + 5*float64(h)
				d := (freq - center) / width
				v += math.Pow(float64(h), -0.9) * math.Exp(-0.5*d*d)
			}
			for _, formant := range []float64{620, 1750, 2850} {
				d := (freq - formant) / 180
				v += 0.08 * math.Exp(-0.5*d*d)
			}
			p[b] = 0.05 + v
		}
		normalizeVector(p)
		profiles[source] = p
	}
	return profiles
}

func normalizeVector(v []float64) {
	sum := 0.0
	for _, x := range v {
		sum += x
	}
	if sum > 0 {
		for i := range v {
			v[i] /= sum
		}
	}
}

func updateFactors(masks [][][]float64, mag [][]float64, profiles [][]float64, activations [][]float64, cfg Config) {
	k := len(masks)
	frames := len(mag)
	bins := len(profiles[0])
	for source := 0; source < k; source++ {
		newProfile := make([]float64, bins)
		for f := 0; f < frames; f++ {
			act := 0.0
			for b := 0; b < bins; b++ {
				w := masks[source][f][b] * mag[f][b]
				newProfile[b] += w
				act += w
			}
			activations[source][f] = 0.65*activations[source][f] + 0.35*(act/float64(bins))
		}
		normalizeVector(newProfile)
		for b := 0; b < bins; b++ {
			if newProfile[b] > 0 {
				profiles[source][b] = 0.9*profiles[source][b] + 0.1*newProfile[b]
			}
		}
		normalizeVector(profiles[source])
	}
}

func reconstructTracks(spec *SpectralResult, masks [][][]float64, cfg Config) ([]SourceTrack, error) {
	k := len(masks)
	frames := len(spec.Frames)
	n := spec.SampleCount
	frameLen := spec.FrameLength
	fftSize := spec.FFTSize
	hop := spec.HopLength
	window := HannWindow(frameLen)
	tracks := make([]SourceTrack, k)

	for source := 0; source < k; source++ {
		ola := make([]float64, n+frameLen+hop)
		windowSum := make([]float64, len(ola))
		for f, frame := range spec.Frames {
			start := f * hop
			real := make([]float64, fftSize)
			imag := make([]float64, fftSize)
			for b := 0; b < len(frame.Bins); b++ {
				mv := masks[source][f][b]
				br := frame.Bins[b].Real * mv
				bi := frame.Bins[b].Imag * mv
				real[b] = br
				imag[b] = bi
				if b > 0 && b < fftSize/2 {
					real[fftSize-b] = br
					imag[fftSize-b] = -bi
				}
			}
			FFT(real, imag, true)
			for j := 0; j < frameLen && start+j < len(ola); j++ {
				ola[start+j] += real[j]
				windowSum[start+j] += window[j]
			}
		}
		raw := make([]float64, n)
		for i := 0; i < n; i++ {
			if windowSum[i] > 1e-12 {
				raw[i] = ola[i] / windowSum[i]
			}
			if !isFinite(raw[i]) {
				de := newError(CodeNumeric, "non-finite reconstructed sample")
				de.ChunkID = spec.ChunkID
				de.Stage = StageSeparate
				return nil, de
			}
		}
		pcm := floatToPCM(raw)
		floatSamples := PCMToFloat(pcm)
		frameRMS := make([]float64, frames)
		for f := 0; f < frames; f++ {
			start := f * hop
			end := start + hop
			if end > n {
				end = n
			}
			sum := 0.0
			count := 0
			for i := start; i < end; i++ {
				sum += floatSamples[i] * floatSamples[i]
				count++
			}
			if count > 0 {
				frameRMS[f] = math.Sqrt(sum / float64(count))
			}
		}
		tracks[source] = SourceTrack{SourceIndex: source, PCM: pcm, FrameRMS: frameRMS, Samples: floatSamples}
	}
	return tracks, nil
}

func floatToPCM(samples []float64) []int16 {
	out := make([]int16, len(samples))
	for i, v := range samples {
		if v > 1 {
			v = 1
		} else if v < -1 {
			v = -1
		}
		out[i] = int16(math.Round(v * 32767))
	}
	return out
}
