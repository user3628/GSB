package cockpitaudio

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
)

const (
	DefaultSeed       int64 = 0x434F434B50495401
	DefaultShardCount       = 3
	ZoneBlockSamples        = 16000
	ShardTimelineMS         = 1000
)

type Interval struct {
	ZoneID  string  `json:"zone_id"`
	StartMS int     `json:"start_ms"`
	EndMS   int     `json:"end_ms"`
	Kind    string  `json:"kind,omitempty"`
	Score   float64 `json:"score,omitempty"`
	Source  string  `json:"source,omitempty"`
}

type ZoneOffset struct {
	ZoneID          string `json:"zone_id"`
	StartSample     int    `json:"start_sample"`
	EndSample       int    `json:"end_sample"`
	StorageOffsetMS int    `json:"storage_offset_ms"`
}

type CandidateHit struct {
	ZoneID   string  `json:"zone_id"`
	Word     string  `json:"word"`
	Strength float64 `json:"strength_log_evidence"`
}

type Command struct {
	ID         string         `json:"id"`
	ShardID    string         `json:"shard_id"`
	Word       string         `json:"word"`
	StartMS    int            `json:"start_ms"`
	EndMS      int            `json:"end_ms"`
	TrueZoneID string         `json:"true_zone_id"`
	Hits       []CandidateHit `json:"reference_hits"`
}

type ShardManifest struct {
	SessionID           string       `json:"session_id"`
	ShardID             string       `json:"shard_id"`
	ShardIndex          int          `json:"shard_index"`
	WAVFile             string       `json:"wav_file"`
	SampleRateHz        int          `json:"sample_rate_hz"`
	DeclaredSampleCount int          `json:"declared_sample_count"`
	TimelineStartMS     int          `json:"timeline_start_ms"`
	TimelineEndMS       int          `json:"timeline_end_ms"`
	ZoneBlockSamples    int          `json:"zone_block_samples"`
	Zones               []ZoneOffset `json:"zones"`
	ReferenceIntervals  []Interval   `json:"reference_intervals"`
	Commands            []Command    `json:"commands"`
}

type SessionManifest struct {
	SessionID    string          `json:"session_id"`
	Seed         int64           `json:"seed"`
	SampleRateHz int             `json:"sample_rate_hz"`
	ShardCount   int             `json:"shard_count"`
	Shards       []ShardManifest `json:"shards"`
}

type Injection struct {
	Kind                 string `json:"kind,omitempty"`
	SampleCountDelta     int    `json:"sample_count_delta,omitempty"`
	ManifestSampleRateHz int    `json:"manifest_sample_rate_hz,omitempty"`
	OverlapZoneOffsets   bool   `json:"overlap_zone_offsets,omitempty"`
}

type GenerateOptions struct {
	Root       string
	Seed       int64
	ShardCount int
	Injection  Injection
}

type Dataset struct {
	Root         string
	ManifestPath string
	Manifest     *SessionManifest
	WAVPaths     []string
}

func GenerateDataset(opts GenerateOptions) (*Dataset, error) {
	if opts.Seed == 0 {
		opts.Seed = DefaultSeed
	}
	if opts.ShardCount <= 0 {
		opts.ShardCount = DefaultShardCount
	}
	if err := os.MkdirAll(opts.Root, 0o755); err != nil {
		return nil, err
	}

	session := &SessionManifest{
		SessionID:    "cabin-session-fixed-seed",
		Seed:         opts.Seed,
		SampleRateHz: 16000,
		ShardCount:   opts.ShardCount,
	}
	dataset := &Dataset{Root: opts.Root, Manifest: session}

	for shardIndex := 0; shardIndex < opts.ShardCount; shardIndex++ {
		shardID := fmt.Sprintf("shard-%03d", shardIndex+1)
		wavName := shardID + ".wav"
		wavPath := filepath.Join(opts.Root, wavName)
		dataset.WAVPaths = append(dataset.WAVPaths, wavPath)

		shardStart := shardIndex * ShardTimelineMS
		patterns := referencePatterns()
		intervals := make([]Interval, 0)
		for zi, zoneID := range ZoneOrder {
			for _, pair := range patterns[zoneID] {
				intervals = append(intervals, Interval{
					ZoneID: zoneID, StartMS: shardStart + pair[0], EndMS: shardStart + pair[1],
					Kind: "speech", Source: "reference", Score: 1,
				})
			}
			_ = zi
		}

		totalSamples := len(ZoneOrder) * ZoneBlockSamples
		samples := make([]int16, totalSamples)
		rng := NewDeterministicRNG(opts.Seed + int64(shardIndex)*1009)
		for zi, zoneID := range ZoneOrder {
			block := synthesizeZone(shardIndex, zi, zoneID, patterns[zoneID], rng)
			copy(samples[zi*ZoneBlockSamples:(zi+1)*ZoneBlockSamples], block)
		}
		if err := WriteWAV(wavPath, &WAV{
			SampleRateHz: 16000, Channels: 1, BitsPerSample: 16, Samples: samples,
		}); err != nil {
			return nil, err
		}

		zones := make([]ZoneOffset, 0, len(ZoneOrder))
		for zi, zoneID := range ZoneOrder {
			start := zi * ZoneBlockSamples
			end := (zi + 1) * ZoneBlockSamples
			if opts.Injection.OverlapZoneOffsets && zoneID == ZoneRearLeft {
				start = len(ZoneOrder[0:1])*ZoneBlockSamples + ZoneBlockSamples/2
			}
			zones = append(zones, ZoneOffset{
				ZoneID: zoneID, StartSample: start, EndSample: end,
				StorageOffsetMS: start * 1000 / 16000,
			})
		}

		declaredCount := totalSamples
		if opts.Injection.Kind == "sample_count" {
			delta := opts.Injection.SampleCountDelta
			if delta == 0 {
				delta = 37
			}
			declaredCount += delta
		}
		manifestRate := 16000
		if opts.Injection.Kind == "rate_conflict" {
			manifestRate = opts.Injection.ManifestSampleRateHz
			if manifestRate == 0 {
				manifestRate = 8000
			}
		}

		commands := referenceCommands(shardID, shardStart)
		shard := ShardManifest{
			SessionID: session.SessionID, ShardID: shardID, ShardIndex: shardIndex,
			WAVFile: wavName, SampleRateHz: manifestRate,
			DeclaredSampleCount: declaredCount, TimelineStartMS: shardStart,
			TimelineEndMS: shardStart + ShardTimelineMS, ZoneBlockSamples: ZoneBlockSamples,
			Zones: zones, ReferenceIntervals: intervals, Commands: commands,
		}
		session.Shards = append(session.Shards, shard)
	}

	manifestPath := filepath.Join(opts.Root, "session_manifest.json")
	b, err := CanonicalJSON(session)
	if err != nil {
		return nil, err
	}
	b = append(b, '\n')
	if err := os.WriteFile(manifestPath, b, 0o644); err != nil {
		return nil, err
	}
	dataset.ManifestPath = manifestPath
	return dataset, nil
}

func referencePatterns() map[string][][2]int {
	return map[string][][2]int{
		ZoneDriver: {
			{110, 390}, {620, 900},
		},
		ZonePassenger: {
			{120, 400}, {630, 890},
		},
		ZoneRearLeft: {
			{130, 410}, {610, 880},
		},
		ZoneRearRight: {
			{140, 420}, {618, 895},
		},
	}
}

func referenceCommands(shardID string, startMS int) []Command {
	return []Command{
		{
			ID: shardID + "-cmd-open-window", ShardID: shardID, Word: "打开主驾车窗",
			StartMS: startMS + 120, EndMS: startMS + 470, TrueZoneID: ZoneDriver,
			Hits: []CandidateHit{
				{ZoneID: ZoneDriver, Word: "打开主驾车窗", Strength: 4.10},
				{ZoneID: ZonePassenger, Word: "打开车窗", Strength: 2.20},
				{ZoneID: ZoneRearLeft, Word: "打开", Strength: 1.45},
				{ZoneID: ZoneRearRight, Word: "打开", Strength: 1.15},
			},
		},
		{
			ID: shardID + "-cmd-temperature", ShardID: shardID, Word: "把温度调高两度",
			StartMS: startMS + 610, EndMS: startMS + 880, TrueZoneID: ZoneRearLeft,
			Hits: []CandidateHit{
				{ZoneID: ZoneRearLeft, Word: "温度调高", Strength: 3.22},
				{ZoneID: ZoneRearRight, Word: "温度调高", Strength: 3.16},
				{ZoneID: ZoneDriver, Word: "温度", Strength: 2.42},
				{ZoneID: ZonePassenger, Word: "温度", Strength: 2.35},
			},
		},
	}
}

func synthesizeZone(shardIndex, zoneIndex int, zoneID string, patterns [][2]int, rng *DeterministicRNG) []int16 {
	out := make([]int16, ZoneBlockSamples)
	baseFreq := map[string]float64{
		ZoneDriver: 132, ZonePassenger: 188, ZoneRearLeft: 118, ZoneRearRight: 166,
	}[zoneID] + float64((shardIndex%3)-1)*2.0

	for i := range out {
		ms := float64(i) * 1000.0 / 16000.0
		envelope := 0.0
		for _, span := range patterns {
			start := float64(span[0])
			end := float64(span[1])
			if ms < start-8 || ms > end+8 {
				continue
			}
			a := 1.0
			fade := 8.0
			if ms < start+fade {
				a = smoothStep((ms - (start - fade)) / (2 * fade))
			} else if ms > end-fade {
				a = smoothStep(((end + fade) - ms) / (2 * fade))
			}
			if a > envelope {
				envelope = a
			}
		}

		noise := (rng.Float64()*2 - 1)
		var v float64
		if envelope > 0 {
			t := float64(i) / 16000.0
			phase := 2 * math.Pi * baseFreq * t
			harmonic := 0.66*math.Sin(phase) + 0.22*math.Sin(2*phase) + 0.10*math.Sin(3*phase)
			amplitude := 10500 + float64(zoneIndex)*350
			v = envelope * amplitude * harmonic
			v += envelope * noise * 420
		} else {
			v = noise * 150
		}
		if v > 32767 {
			v = 32767
		} else if v < -32768 {
			v = -32768
		}
		out[i] = int16(math.Round(v))
	}
	return out
}

func smoothStep(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x * x * (3 - 2*x)
}

func LoadManifest(path string) (*SessionManifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m SessionManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
