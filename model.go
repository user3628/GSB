package speakerdiar

import "fmt"

const (
	StageFrame     = "frame_spectrum"
	StageSeparate  = "mask_separation"
	StageEmbed     = "embedding_arbitration"
	StageSummary   = "summary_timeline"
	SchemaVersion  = 1
	DefaultSeed    = int64(0x5EED239)
	DefaultWorkDir = "diar-workspace"
)

// StageOrder maps the four required phases to contiguous checkpoint ordinals.
var StageOrder = map[string]int{
	StageFrame:    1,
	StageSeparate: 2,
	StageEmbed:    3,
	StageSummary:  4,
}

// OrderedStages returns stages in pipeline order.
func OrderedStages() []string {
	return []string{StageFrame, StageSeparate, StageEmbed, StageSummary}
}

// Config is the deterministic parameter set for a session.
type Config struct {
	Seed int64 `json:"seed"`

	SampleRate    int `json:"sample_rate"`
	Channels      int `json:"channels"`
	BitsPerSample int `json:"bits_per_sample"`

	FrameDurationMS int `json:"frame_duration_ms"`
	HopDurationMS   int `json:"hop_duration_ms"`
	FFTSize         int `json:"fft_size"`

	MaxSpeakers int `json:"max_speakers"`

	MaskMaxIterations int     `json:"mask_max_iterations"`
	MaskEpsilon       float64 `json:"mask_epsilon"`
	MaskFloor         float64 `json:"mask_floor"`
	MaskSharpness     float64 `json:"mask_sharpness"`
	ConvergenceRounds int     `json:"convergence_rounds"`

	MelBands           int     `json:"mel_bands"`
	EmbeddingDim       int     `json:"embedding_dim"`
	ProjectionInputDim int     `json:"projection_input_dim"`
	ProjectionSeed     uint64  `json:"projection_seed"`
	MarginThreshold    float64 `json:"margin_threshold"`

	VoiceEnergyThreshold   float64 `json:"voice_energy_threshold"`
	OverlapEnergyThreshold float64 `json:"overlap_energy_threshold"`
	FrameEnergyFloor       float64 `json:"frame_energy_floor"`
	ShortGapMS             int     `json:"short_gap_ms"`
	MinSegmentMS           int     `json:"min_segment_ms"`
	MaxSegmentMS           int     `json:"max_segment_ms"`
}

// DefaultConfig returns the required 16 kHz mono PCM16 configuration.
func DefaultConfig() Config {
	return Config{
		Seed:                   DefaultSeed,
		SampleRate:             16000,
		Channels:               1,
		BitsPerSample:          16,
		FrameDurationMS:        25,
		HopDurationMS:          10,
		FFTSize:                512,
		MaxSpeakers:            4,
		MaskMaxIterations:      8,
		MaskEpsilon:            1e-3,
		MaskFloor:              0.002,
		MaskSharpness:          2.4,
		ConvergenceRounds:      1,
		MelBands:               24,
		EmbeddingDim:           64,
		ProjectionInputDim:     0,
		ProjectionSeed:         0xD1A9,
		MarginThreshold:        0.035,
		VoiceEnergyThreshold:   0.010,
		OverlapEnergyThreshold: 0.014,
		FrameEnergyFloor:       1e-10,
		ShortGapMS:             120,
		MinSegmentMS:           120,
		MaxSegmentMS:           5000,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.Seed == 0 {
		c.Seed = d.Seed
	}
	if c.SampleRate == 0 {
		c.SampleRate = d.SampleRate
	}
	if c.Channels == 0 {
		c.Channels = d.Channels
	}
	if c.BitsPerSample == 0 {
		c.BitsPerSample = d.BitsPerSample
	}
	if c.FrameDurationMS == 0 {
		c.FrameDurationMS = d.FrameDurationMS
	}
	if c.HopDurationMS == 0 {
		c.HopDurationMS = d.HopDurationMS
	}
	if c.FFTSize == 0 {
		c.FFTSize = d.FFTSize
	}
	if c.MaxSpeakers == 0 {
		c.MaxSpeakers = d.MaxSpeakers
	}
	if c.MaskMaxIterations == 0 {
		c.MaskMaxIterations = d.MaskMaxIterations
	}
	if c.MaskEpsilon == 0 {
		c.MaskEpsilon = d.MaskEpsilon
	}
	if c.MaskFloor == 0 {
		c.MaskFloor = d.MaskFloor
	}
	if c.MaskSharpness == 0 {
		c.MaskSharpness = d.MaskSharpness
	}
	if c.ConvergenceRounds == 0 {
		c.ConvergenceRounds = d.ConvergenceRounds
	}
	if c.MelBands == 0 {
		c.MelBands = d.MelBands
	}
	if c.EmbeddingDim == 0 {
		c.EmbeddingDim = d.EmbeddingDim
	}
	if c.ProjectionSeed == 0 {
		c.ProjectionSeed = d.ProjectionSeed
	}
	if c.MarginThreshold == 0 {
		c.MarginThreshold = d.MarginThreshold
	}
	if c.VoiceEnergyThreshold == 0 {
		c.VoiceEnergyThreshold = d.VoiceEnergyThreshold
	}
	if c.OverlapEnergyThreshold == 0 {
		c.OverlapEnergyThreshold = d.OverlapEnergyThreshold
	}
	if c.FrameEnergyFloor == 0 {
		c.FrameEnergyFloor = d.FrameEnergyFloor
	}
	if c.ShortGapMS == 0 {
		c.ShortGapMS = d.ShortGapMS
	}
	if c.MinSegmentMS == 0 {
		c.MinSegmentMS = d.MinSegmentMS
	}
	if c.MaxSegmentMS == 0 {
		c.MaxSegmentMS = d.MaxSegmentMS
	}
	return c
}

// FrameSamples converts declared frame milliseconds to an integer sample count.
func (c Config) FrameSamples() int {
	return c.FrameDurationMS * c.SampleRate / 1000
}

// HopSamples converts declared frame shift milliseconds to integer samples.
func (c Config) HopSamples() int {
	return c.HopDurationMS * c.SampleRate / 1000
}

// SamplesToMS converts a sample offset to integer milliseconds.
func (c Config) SamplesToMS(samples int) int {
	return samples * 1000 / c.SampleRate
}

// ReferenceSegment is one truth annotation from the session manifest.
type ReferenceSegment struct {
	StartMS    int     `json:"start_ms"`
	EndMS      int     `json:"end_ms"`
	SpeakerID  string  `json:"speaker_id"`
	Confidence float64 `json:"confidence"`
	Trusted    bool    `json:"trusted"`
}

// ChunkManifest describes one deterministic WAV shard and its reference labels.
type ChunkManifest struct {
	ChunkID               string             `json:"chunk_id"`
	Index                 int                `json:"index"`
	WAVPath               string             `json:"wav_path"`
	DurationMS            int                `json:"duration_ms"`
	DeclaredSampleCount   int                `json:"declared_sample_count"`
	SampleCountUpperBound int                `json:"sample_count_upper_bound"`
	SpeakerCount          int                `json:"speaker_count"`
	ReferenceSegments     []ReferenceSegment `json:"reference_segments"`
}

// SessionManifest is the sole declared truth and chunk ordering source.
type SessionManifest struct {
	SchemaVersion int             `json:"schema_version"`
	SessionID     string          `json:"session_id"`
	SampleRate    int             `json:"sample_rate"`
	Config        Config          `json:"config"`
	Chunks        []ChunkManifest `json:"chunks"`
}

// Injection selects one deterministic external-parameter failure link.
type Injection string

const (
	InjectNone                  Injection = ""
	InjectLengthMismatch        Injection = "length_mismatch"
	InjectRateMismatch          Injection = "rate_mismatch"
	InjectZeroSpeakers          Injection = "zero_speakers"
	InjectNegativeMaskRounds    Injection = "negative_mask_rounds"
	InjectProjectionDimMismatch Injection = "projection_dim_mismatch"
	InjectEmbeddingTie          Injection = "embedding_tie"
	InjectNumericSpectrum       Injection = "numeric_spectrum"
	InjectZeroMaskedSource      Injection = "zero_masked_source"
	InjectTooManySpeakers       Injection = "too_many_speakers"
	InjectIncrementalMismatch   Injection = "incremental_mismatch"
)

func validateBasicConfig(c Config) error {
	if c.SampleRate <= 0 {
		return newError(CodeInvalidParam, "sample_rate must be positive")
	}
	if c.FrameDurationMS <= 0 || c.HopDurationMS <= 0 {
		return newError(CodeInvalidParam, "frame and hop durations must be positive")
	}
	if c.FrameSamples() <= 0 || c.HopSamples() <= 0 {
		return newError(CodeInvalidParam, "millisecond conversion produced zero samples")
	}
	if c.FFTSize < c.FrameSamples() {
		return newError(CodeInvalidParam, fmt.Sprintf("fft_size %d is smaller than frame samples %d", c.FFTSize, c.FrameSamples()))
	}
	if c.EmbeddingDim <= 0 || c.MelBands <= 0 {
		return newError(CodeInvalidParam, "mel and embedding dimensions must be positive")
	}
	return nil
}
