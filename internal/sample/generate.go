package sample

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"voiceline/internal/model"
	"voiceline/internal/wav"
)

const (
	DefaultSeed         int64 = 0x51A7E11
	DefaultSampleRate         = 16000
	DefaultSamples            = 7200 // 45 x 10 ms hops at 16 kHz
	DefaultSegmentCount       = 4
)

type Options struct {
	OutDir       string
	Seed         int64
	SegmentCount int
	SampleRate   int
	Samples      int
}

type splitMix64 struct {
	state uint64
}

func (r *splitMix64) nextUint64() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *splitMix64) unit() float64 {
	return float64(r.nextUint64()>>11) / float64(1<<53)
}

func Generate(opts Options) (*model.Manifest, error) {
	if opts.OutDir == "" {
		return nil, fmt.Errorf("sample output directory is required")
	}
	if opts.Seed == 0 {
		opts.Seed = DefaultSeed
	}
	if opts.SegmentCount <= 0 {
		opts.SegmentCount = DefaultSegmentCount
	}
	if opts.SampleRate <= 0 {
		opts.SampleRate = DefaultSampleRate
	}
	if opts.Samples <= 0 {
		opts.Samples = DefaultSamples
	}

	wavDir := filepath.Join(opts.OutDir, "wav")
	if err := os.MkdirAll(wavDir, 0o755); err != nil {
		return nil, err
	}

	baseF0 := []float64{112, 126, 140, 154, 118, 134}
	targets := []string{"tgt-21", "tgt-34", "tgt-47", "tgt-52", "tgt-63", "tgt-78"}
	shifts := []float64{1.04, 0.97, 1.08, 0.94, 1.02, 1.11}
	warps := []float64{0.92, 1.07, 1.13, 0.88, 1.01, 1.18}

	manifest := &model.Manifest{
		SchemaVersion: "voiceline.v1",
		SampleRate:    opts.SampleRate,
		Channels:      1,
		BitsPerSample: 16,
		Seed:          opts.Seed,
		Segments:      make([]model.SegmentManifest, 0, opts.SegmentCount),
	}

	for i := 0; i < opts.SegmentCount; i++ {
		segmentID := fmt.Sprintf("seg-%03d", i)
		base := baseF0[i%len(baseF0)]
		rng := &splitMix64{state: uint64(opts.Seed) + uint64(i+1)*0xD1B54A32D192ED03}
		samples := generateSegment(opts.Samples, opts.SampleRate, base, i, rng)

		wavName := filepath.Join("wav", segmentID+".wav")
		if err := wav.WriteFile(filepath.Join(opts.OutDir, wavName), samples, opts.SampleRate); err != nil {
			return nil, err
		}

		low := math.Round((base-12)*10) / 10
		high := math.Round((base+12)*10) / 10
		segment := model.SegmentManifest{
			ID:                        segmentID,
			WAVPath:                   wavName,
			SourceSpeaker:             fmt.Sprintf("src-%02d", 10+i),
			TargetSpeaker:             targets[i%len(targets)],
			F0Shift:                   shifts[i%len(shifts)],
			EnvelopeWarp:              warps[i%len(warps)],
			FrameShiftMS:              10,
			FrameLengthMS:             20,
			BlockLength:               4,
			BlockFrames:               4,
			ExpectedSamplesUpperBound: opts.Samples,
			ExpectedSamples:           opts.Samples,
			ReferenceF0MeanRange:      [2]float64{low, high},
			F0ToleranceHz:             18,
			MinInRangeRatio:           0.9,
			MaxGroups:                 2,
			FramesPerGroup:            8,
			RoundBudgetNanos:          int64(20 * timeMillisecond),
		}
		manifest.Segments = append(manifest.Segments, segment)
	}

	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	manifestBytes = append(manifestBytes, '\n')
	if err := os.WriteFile(filepath.Join(opts.OutDir, "manifest.json"), manifestBytes, 0o644); err != nil {
		return nil, err
	}
	return manifest, nil
}

const timeMillisecond = 1_000_000 // nanoseconds

func generateSegment(n, sampleRate int, baseF0 float64, segmentIndex int, rng *splitMix64) []int16 {
	out := make([]int16, n)
	phase := 0.0
	for i := 0; i < n; i++ {
		t := float64(i) / float64(sampleRate)
		f0 := baseF0 +
			3.2*math.Sin(2*math.Pi*0.73*t+float64(segmentIndex)*0.9) +
			1.8*math.Sin(2*math.Pi*5.1*t+float64(segmentIndex))
		phase += 2 * math.Pi * f0 / float64(sampleRate)

		value := 0.0
		for harmonic := 1; harmonic <= 12; harmonic++ {
			amplitude := 0.42 / math.Pow(float64(harmonic), 1.18)
			if harmonic%3 == 0 {
				amplitude *= 0.72
			}
			value += amplitude * math.Sin(float64(harmonic)*phase+0.17*float64(harmonic))
		}
		value += (rng.unit() - 0.5) * 0.012

		fade := 1.0
		fadeSamples := sampleRate / 100 // 10 ms edge fade
		if i < fadeSamples {
			fade = 0.5 * (1 - math.Cos(math.Pi*float64(i)/float64(fadeSamples)))
		} else if i > n-fadeSamples {
			remain := n - i
			fade = 0.5 * (1 - math.Cos(math.Pi*float64(remain)/float64(fadeSamples)))
		}
		value *= 0.62 * fade

		clamped := math.Max(-1, math.Min(1, value))
		out[i] = int16(math.Round(clamped * 32767))
	}
	return out
}
