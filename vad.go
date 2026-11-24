package cockpitaudio

import (
	"fmt"
	"math"
	"sort"
)

type FrameAnalysis struct {
	Index       int     `json:"index"`
	StartMS     int     `json:"start_ms"`
	EndMS       int     `json:"end_ms"`
	LogEnergyDB float64 `json:"log_energy_db"`
	Flatness    float64 `json:"spectral_flatness"`
	EnergyScore float64 `json:"energy_score"`
	RawScore    float64 `json:"raw_score"`
	Score       float64 `json:"score"`
	Active      bool    `json:"active"`
}

type ZoneVAD struct {
	ShardID            string          `json:"shard_id"`
	ZoneID             string          `json:"zone_id"`
	SampleStart        int             `json:"sample_start"`
	SampleEnd          int             `json:"sample_end"`
	FrameLengthMS      int             `json:"frame_length_ms"`
	FrameShiftMS       int             `json:"frame_shift_ms"`
	FrameLengthSamples int             `json:"frame_length_samples"`
	FrameShiftSamples  int             `json:"frame_shift_samples"`
	FFTSamples         int             `json:"fft_samples"`
	FrameCount         int             `json:"frame_count"`
	Intervals          []Interval      `json:"intervals"`
	Frames             []FrameAnalysis `json:"frames,omitempty"`
}

type ShardVAD struct {
	ShardID           string    `json:"shard_id"`
	ShardFingerprint  string    `json:"shard_fingerprint"`
	ConfigFingerprint string    `json:"config_fingerprint"`
	ConfigVersion     string    `json:"config_version"`
	Zones             []ZoneVAD `json:"zones"`
}

func ValidateShardAudio(shard *ShardManifest, wav *WAV) *Error {
	if wav == nil {
		return NewError(CodeInvalidParam, StageIngest, shard.ShardID, "wav is nil")
	}
	if shard.DeclaredSampleCount != len(wav.Samples) {
		return &Error{
			Code: CodeSampleCountMismatch, Stage: StageIngest, ShardID: shard.ShardID,
			Message: fmt.Sprintf("declared sample count %d does not match actual %d", shard.DeclaredSampleCount, len(wav.Samples)),
			Diagnostics: map[string]string{
				"declared_sample_count": fmt.Sprint(shard.DeclaredSampleCount),
				"actual_sample_count":   fmt.Sprint(len(wav.Samples)),
			},
		}
	}
	if shard.SampleRateHz != wav.SampleRateHz {
		return &Error{
			Code: CodeRateConflict, Stage: StageIngest, ShardID: shard.ShardID,
			Message: fmt.Sprintf("manifest rate %d conflicts with WAV rate %d", shard.SampleRateHz, wav.SampleRateHz),
			Diagnostics: map[string]string{
				"manifest_sample_rate_hz": fmt.Sprint(shard.SampleRateHz),
				"wav_sample_rate_hz":      fmt.Sprint(wav.SampleRateHz),
			},
		}
	}

	offsets := append([]ZoneOffset(nil), shard.Zones...)
	sort.Slice(offsets, func(i, j int) bool { return offsets[i].StartSample < offsets[j].StartSample })
	for i, z := range offsets {
		if z.StartSample < 0 || z.EndSample > len(wav.Samples) || z.StartSample >= z.EndSample {
			return &Error{
				Code: CodeInvalidParam, Stage: StageIngest, ShardID: shard.ShardID,
				Message: "zone offset is outside WAV bounds or empty",
				Diagnostics: map[string]string{
					"reason": "invalid_zone_bounds", "zone_id": z.ZoneID,
					"start_sample": fmt.Sprint(z.StartSample), "end_sample": fmt.Sprint(z.EndSample),
					"actual_samples": fmt.Sprint(len(wav.Samples)),
				},
			}
		}
		if i > 0 && z.StartSample < offsets[i-1].EndSample {
			return &Error{
				Code: CodeInvalidParam, Stage: StageIngest, ShardID: shard.ShardID,
				Message: "zone sample offsets overlap",
				Diagnostics: map[string]string{
					"reason": "offset_overlap", "zone_id": z.ZoneID,
					"start_sample":        fmt.Sprint(z.StartSample),
					"previous_end_sample": fmt.Sprint(offsets[i-1].EndSample),
				},
			}
		}
	}
	return nil
}

func RunVAD(shard *ShardManifest, wav *WAV, cfg Config) (*ShardVAD, *Error) {
	if err := ValidateShardAudio(shard, wav); err != nil {
		return nil, err
	}
	resolved, err := cfg.ResolveVAD(shard.ShardID)
	if err != nil {
		return nil, err
	}
	out := &ShardVAD{
		ShardID: shard.ShardID, ConfigFingerprint: VADConfigFingerprint(cfg),
		ConfigVersion: VADConfigFingerprint(cfg)[:16],
	}
	for _, zoneID := range ZoneOrder {
		var offset ZoneOffset
		found := false
		for _, z := range shard.Zones {
			if z.ZoneID == zoneID {
				offset = z
				found = true
				break
			}
		}
		if !found {
			return nil, NewError(CodeInvalidParam, StageVAD, shard.ShardID, "missing zone offset for %s", zoneID)
		}
		zone, zerr := analyzeZone(shard, offset, wav.Samples[offset.StartSample:offset.EndSample], resolved)
		if zerr != nil {
			return nil, zerr
		}
		out.Zones = append(out.Zones, *zone)
	}
	return out, nil
}

func samplesToMS(samples, rate int) int {
	return samples * 1000 / rate
}

func analyzeZone(shard *ShardManifest, offset ZoneOffset, pcm []int16, cfg ResolvedConfig) (*ZoneVAD, *Error) {
	nFrame := cfg.FrameLengthSamples
	hop := cfg.FrameShiftSamples
	if len(pcm) < nFrame {
		return nil, NewError(CodeInvalidParam, StageVAD, shard.ShardID, "zone %s is shorter than one frame", offset.ZoneID)
	}
	frameCount := (len(pcm)-nFrame)/hop + 1
	window := make([]float64, nFrame)
	for i := range window {
		window[i] = 0.5 - 0.5*math.Cos(2*math.Pi*float64(i)/float64(nFrame))
	}

	scores := make([]float64, frameCount)
	frames := make([]FrameAnalysis, frameCount)
	for i := 0; i < frameCount; i++ {
		start := i * hop
		meanSquare := 0.0
		re := make([]float64, cfg.FFTSamples)
		im := make([]float64, cfg.FFTSamples)
		for j := 0; j < nFrame; j++ {
			x := float64(pcm[start+j]) / 32768.0
			wx := x * window[j]
			meanSquare += x * x
			re[j] = wx
		}
		meanSquare /= float64(nFrame)
		logEnergy := 10 * math.Log10(meanSquare+1e-12)
		energyScore := clamp((logEnergy+52.0)/34.0, 0, 1)

		fftRadix2(re, im)
		count := cfg.FFTSamples/2 - 1
		logSum, magSum := 0.0, 0.0
		for bin := 1; bin < cfg.FFTSamples/2; bin++ {
			mag := math.Hypot(re[bin], im[bin]) + 1e-10
			logSum += math.Log(mag)
			magSum += mag
		}
		flatness := math.Exp(logSum/float64(count)) / (magSum / float64(count))
		flatness = clamp(flatness, 0, 1)
		rawScore := 0.72*energyScore + 0.28*(1-flatness)
		scores[i] = rawScore
		frames[i] = FrameAnalysis{
			Index:       i,
			StartMS:     samplesToMS(start, cfg.SampleRateHz),
			EndMS:       samplesToMS(start+nFrame, cfg.SampleRateHz),
			LogEnergyDB: Round(logEnergy, 6), Flatness: Round(flatness, 6),
			EnergyScore: Round(energyScore, 6), RawScore: Round(rawScore, 6),
			Score: Round(rawScore, 6),
		}
	}

	mask := append([]float64(nil), scores...)
	for iter := 0; iter < cfg.MaskIterations; iter++ {
		next := append([]float64(nil), mask...)
		for i := range mask {
			left := mask[maxInt(0, i-1)]
			right := mask[minInt(len(mask)-1, i+1)]
			next[i] = (left + 2*mask[i] + right) / 4
		}
		mask = next
	}

	active := false
	activeFrames := make([]bool, frameCount)
	for i, score := range mask {
		if !active && score >= cfg.HighThreshold {
			active = true
		} else if active && score < cfg.LowThreshold {
			active = false
		}
		activeFrames[i] = active
		frames[i].Score = Round(score, 6)
		frames[i].Active = active
	}

	rawIntervals := runsToIntervals(shard, offset.ZoneID, activeFrames, hop, nFrame, cfg, frames)
	merged := mergeShortSilences(rawIntervals, cfg.MinSilenceMS)
	kept := make([]Interval, 0, len(merged))
	for _, in := range merged {
		if in.EndMS-in.StartMS >= cfg.MinSpeechMS {
			kept = append(kept, in)
		}
	}

	return &ZoneVAD{
		ShardID: shard.ShardID, ZoneID: offset.ZoneID,
		SampleStart: offset.StartSample, SampleEnd: offset.EndSample,
		FrameLengthMS: cfg.FrameLengthMS, FrameShiftMS: cfg.FrameShiftMS,
		FrameLengthSamples: cfg.FrameLengthSamples, FrameShiftSamples: cfg.FrameShiftSamples,
		FFTSamples: cfg.FFTSamples, FrameCount: frameCount,
		Intervals: kept, Frames: frames,
	}, nil
}

func runsToIntervals(shard *ShardManifest, zoneID string, active []bool, hop, frameN int, cfg ResolvedConfig, frames []FrameAnalysis) []Interval {
	out := []Interval{}
	for i := 0; i < len(active); {
		if !active[i] {
			i++
			continue
		}
		startIdx := i
		for i < len(active) && active[i] {
			i++
		}
		endIdx := i - 1
		startSample := startIdx * hop
		endSample := endIdx*hop + frameN
		startMS := shard.TimelineStartMS + samplesToMS(startSample, cfg.SampleRateHz)
		endMS := shard.TimelineStartMS + samplesToMS(endSample, cfg.SampleRateHz)
		scoreSum, scoreN := 0.0, 0.0
		for j := startIdx; j <= endIdx; j++ {
			scoreSum += frames[j].Score
			scoreN++
		}
		out = append(out, Interval{
			ZoneID: zoneID, StartMS: startMS, EndMS: endMS,
			Kind: "speech", Source: "vad", Score: Round(scoreSum/scoreN, 6),
		})
	}
	return out
}

func mergeShortSilences(in []Interval, minSilenceMS int) []Interval {
	if len(in) == 0 {
		return in
	}
	out := []Interval{in[0]}
	for _, cur := range in[1:] {
		last := &out[len(out)-1]
		if cur.ZoneID == last.ZoneID && cur.StartMS-last.EndMS < minSilenceMS {
			last.EndMS = cur.EndMS
			last.Score = Round((last.Score+cur.Score)/2, 6)
		} else {
			out = append(out, cur)
		}
	}
	return out
}

func clamp(v, lo, hi float64) float64 {
	return math.Max(lo, math.Min(hi, v))
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var _ = fmt.Sprintf
