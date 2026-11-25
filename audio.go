package speakerdiar

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
)

type genSegment struct {
	startMS    int
	endMS      int
	speaker    string
	trusted    bool
	confidence float64
}

type genChunk struct {
	id         string
	durationMS int
	segments   []genSegment
}

func deterministicPlan() []genChunk {
	return []genChunk{
		{
			id: "chunk-000", durationMS: 6000,
			segments: []genSegment{
				{400, 1800, "S1", true, 0.99},
				{1300, 2700, "S2", true, 0.98},
				{3000, 4300, "S1", true, 0.97},
				{4700, 5700, "S2", false, 0.82},
			},
		},
		{
			id: "chunk-001", durationMS: 6400,
			segments: []genSegment{
				{300, 1600, "S2", true, 0.99},
				{1200, 2600, "S3", true, 0.98},
				{2800, 4000, "S4", true, 0.96},
				{3600, 4900, "S2", true, 0.97},
				{5200, 6100, "S3", false, 0.84},
			},
		},
		{
			id: "chunk-002", durationMS: 7000,
			segments: []genSegment{
				{300, 1500, "S1", true, 0.99},
				{1100, 2400, "S3", true, 0.98},
				{2100, 3400, "S4", true, 0.97},
				{3100, 4500, "S2", true, 0.98},
				{4200, 5400, "S1", false, 0.86},
				{5600, 6600, "S3", true, 0.95},
			},
		},
	}
}

type voiceProfile struct {
	f0       float64
	f0Spread float64
	formants [3]float64
	widths   [3]float64
	amps     [3]float64
}

func speakerProfile(speaker string) voiceProfile {
	switch speaker {
	case "S1":
		return voiceProfile{116, 5.5, [3]float64{520, 1500, 2500}, [3]float64{55, 90, 130}, [3]float64{1.0, 0.55, 0.28}}
	case "S2":
		return voiceProfile{158, 7.0, [3]float64{650, 1720, 2750}, [3]float64{65, 100, 140}, [3]float64{0.95, 0.62, 0.32}}
	case "S3":
		return voiceProfile{198, 8.0, [3]float64{760, 1950, 3000}, [3]float64{75, 110, 150}, [3]float64{0.9, 0.58, 0.34}}
	default:
		return voiceProfile{246, 9.0, [3]float64{880, 2150, 3250}, [3]float64{85, 120, 160}, [3]float64{0.82, 0.62, 0.38}}
	}
}

// GenerateSession writes deterministic WAV shards and manifest under dir.
// Existing generated files are treated as immutable and are not regenerated.
func GenerateSession(dir string, cfg Config) (*SessionManifest, error) {
	cfg = cfg.withDefaults()
	if err := os.MkdirAll(filepath.Join(dir, "samples"), 0o755); err != nil {
		return nil, err
	}
	plan := deterministicPlan()
	manifest := SessionManifest{
		SchemaVersion: SchemaVersion,
		SessionID:     "deterministic-local-session",
		SampleRate:    cfg.SampleRate,
		Config:        cfg,
		Chunks:        make([]ChunkManifest, 0, len(plan)),
	}

	for chunkIndex, pc := range plan {
		sampleCount := pc.durationMS * cfg.SampleRate / 1000
		mix := make([]float64, sampleCount)
		speakerSet := map[string]struct{}{}
		for _, gs := range pc.segments {
			speakerSet[gs.speaker] = struct{}{}
			start := gs.startMS * cfg.SampleRate / 1000
			end := gs.endMS * cfg.SampleRate / 1000
			if end > sampleCount {
				end = sampleCount
			}
			renderVoice(mix, gs.speaker, start, end, cfg.SampleRate, cfg.Seed+int64(chunkIndex*17)+int64(gs.startMS))
		}

		peak := 1.0
		for _, v := range mix {
			peak = math.Max(peak, math.Abs(v))
		}
		gain := 0.89 / peak
		pcm := make([]int16, sampleCount)
		for i, v := range mix {
			scaled := v * gain
			if scaled > 1 {
				scaled = 1
			} else if scaled < -1 {
				scaled = -1
			}
			pcm[i] = int16(math.Round(scaled * 32767))
		}

		wavRel := filepath.ToSlash(filepath.Join("samples", pc.id+".wav"))
		wavPath := filepath.Join(dir, wavRel)
		if err := WriteWAV(wavPath, pcm, cfg.SampleRate, cfg.Channels); err != nil {
			return nil, err
		}

		speakers := make([]string, 0, len(speakerSet))
		for sp := range speakerSet {
			speakers = append(speakers, sp)
		}
		// Fixed acoustic order S1..S4, not Go map order.
		speakerOrder := map[string]int{"S1": 1, "S2": 2, "S3": 3, "S4": 4}
		for i := 0; i < len(speakers); i++ {
			for j := i + 1; j < len(speakers); j++ {
				if speakerOrder[speakers[j]] < speakerOrder[speakers[i]] {
					speakers[i], speakers[j] = speakers[j], speakers[i]
				}
			}
		}

		refs := make([]ReferenceSegment, 0, len(pc.segments))
		for _, gs := range pc.segments {
			refs = append(refs, ReferenceSegment{
				StartMS: gs.startMS, EndMS: gs.endMS, SpeakerID: gs.speaker,
				Confidence: gs.confidence, Trusted: gs.trusted,
			})
		}
		manifest.Chunks = append(manifest.Chunks, ChunkManifest{
			ChunkID: pc.id, Index: chunkIndex, WAVPath: wavRel,
			DurationMS: pc.durationMS, DeclaredSampleCount: sampleCount,
			SampleCountUpperBound: sampleCount, SpeakerCount: len(speakers),
			ReferenceSegments: refs,
		})
	}

	manifestPath := filepath.Join(dir, "manifest.json")
	b, err := PrettyJSON(manifest)
	if err != nil {
		return nil, err
	}
	if err := AtomicWriteFile(manifestPath, b); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func renderVoice(mix []float64, speaker string, start, end, rate int, seed int64) {
	if start < 0 {
		start = 0
	}
	if end > len(mix) {
		end = len(mix)
	}
	if end <= start {
		return
	}
	p := speakerProfile(speaker)
	rng := rand.New(rand.NewSource(seed))
	phase := rng.Float64() * 2 * math.Pi
	noisePhase := rng.Float64() * 2 * math.Pi
	attack := rate / 40  // 25 ms
	release := rate / 30 // 33 ms
	for i := start; i < end; i++ {
		local := i - start
		t := float64(local) / float64(rate)
		vibrato := 1.0 + 0.018*math.Sin(2*math.Pi*5.2*t+float64(len(speaker)))
		jitter := (rng.Float64() - 0.5) * 0.004
		f0 := (p.f0 + p.f0Spread*math.Sin(2*math.Pi*0.7*t)) * vibrato * (1 + jitter)
		phase += 2 * math.Pi * f0 / float64(rate)
		sample := 0.0
		for h := 1; h <= 14; h++ {
			freq := f0 * float64(h)
			weight := 0.0
			for fi, center := range p.formants {
				d := (freq - center) / p.widths[fi]
				weight += p.amps[fi] * math.Exp(-0.5*d*d)
			}
			weight *= math.Pow(float64(h), -1.15)
			sample += weight * math.Sin(float64(h)*phase)
		}
		noisePhase += 2 * math.Pi * (f0 * 7.3) / float64(rate)
		sample += 0.025 * math.Sin(noisePhase) * math.Sin(2*math.Pi*0.31*t)

		env := 1.0
		if local < attack {
			env = 0.5 - 0.5*math.Cos(math.Pi*float64(local)/float64(attack))
		} else if end-start-local < release {
			remain := end - start - local
			env = 0.5 - 0.5*math.Cos(math.Pi*float64(remain)/float64(release))
		}
		tremolo := 0.92 + 0.08*math.Sin(2*math.Pi*4.7*t)
		mix[i] += 0.24 * sample * env * tremolo
	}
}

// WriteWAV writes mono or multi-channel PCM16 WAV data.
func WriteWAV(path string, samples []int16, sampleRate, channels int) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	dataBytes := len(samples) * 2
	riffSize := 36 + dataBytes
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	header := make([]byte, 44)
	copy(header[0:4], "RIFF")
	binary.LittleEndian.PutUint32(header[4:8], uint32(riffSize))
	copy(header[8:12], "WAVE")
	copy(header[12:16], "fmt ")
	binary.LittleEndian.PutUint32(header[16:20], 16)
	binary.LittleEndian.PutUint16(header[20:22], 1)
	binary.LittleEndian.PutUint16(header[22:24], uint16(channels))
	binary.LittleEndian.PutUint32(header[24:28], uint32(sampleRate))
	byteRate := sampleRate * channels * 16 / 8
	binary.LittleEndian.PutUint32(header[28:32], uint32(byteRate))
	blockAlign := channels * 16 / 8
	binary.LittleEndian.PutUint16(header[32:34], uint16(blockAlign))
	binary.LittleEndian.PutUint16(header[34:36], 16)
	copy(header[36:40], "data")
	binary.LittleEndian.PutUint32(header[40:44], uint32(dataBytes))
	if _, err := f.Write(header); err != nil {
		return err
	}
	buf := make([]byte, 2)
	for _, s := range samples {
		binary.LittleEndian.PutUint16(buf, uint16(s))
		if _, err := f.Write(buf); err != nil {
			return err
		}
	}
	return f.Close()
}

// ReadWAV reads PCM16 RIFF data generated or supplied locally.
func ReadWAV(path string) ([]int16, int, int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, 0, err
	}
	if len(b) < 44 || string(b[0:4]) != "RIFF" || string(b[8:12]) != "WAVE" {
		return nil, 0, 0, fmt.Errorf("invalid WAV header: %s", path)
	}
	pos := 12
	sampleRate := 0
	channels := 0
	bits := 0
	data := []byte{}
	for pos+8 <= len(b) {
		id := string(b[pos : pos+4])
		size := int(binary.LittleEndian.Uint32(b[pos+4 : pos+8]))
		body := pos + 8
		if body+size > len(b) {
			return nil, 0, 0, fmt.Errorf("truncated WAV chunk %s", id)
		}
		if id == "fmt " && size >= 16 {
			channels = int(binary.LittleEndian.Uint16(b[body+2 : body+4]))
			sampleRate = int(binary.LittleEndian.Uint32(b[body+4 : body+8]))
			bits = int(binary.LittleEndian.Uint16(b[body+14 : body+16]))
		}
		if id == "data" {
			data = b[body : body+size]
		}
		pos = body + size + size%2
	}
	if bits != 16 {
		return nil, sampleRate, channels, fmt.Errorf("only PCM16 is supported, got %d bits", bits)
	}
	samples := make([]int16, len(data)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(data[i*2 : i*2+2]))
	}
	return samples, sampleRate, channels, nil
}

// PCMToFloat converts deterministic int16 samples to [-1,1).
func PCMToFloat(samples []int16) []float64 {
	out := make([]float64, len(samples))
	for i, s := range samples {
		out[i] = float64(s) / 32768.0
	}
	return out
}
