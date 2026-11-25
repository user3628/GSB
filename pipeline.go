package speakerdiar

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
)

// FingerprintSet contains content-addressed stage versions for one chunk.
type FingerprintSet struct {
	ChunkContent  string `json:"chunk_content"`
	ConfigVersion string `json:"config_version"`
	Frame         string `json:"frame"`
	Separation    string `json:"separation"`
	Embedding     string `json:"embedding"`
	Summary       string `json:"summary"`
}

// ChunkProducts holds all in-memory stage products for one chunk.
type ChunkProducts struct {
	ChunkID      string            `json:"chunk_id"`
	ChunkIndex   int               `json:"chunk_index"`
	Fingerprints FingerprintSet    `json:"fingerprints"`
	Spectral     *SpectralResult   `json:"spectral,omitempty"`
	Separation   *SeparationResult `json:"separation,omitempty"`
	Embedding    *EmbeddingResult  `json:"embedding,omitempty"`
	Summary      *ChunkSummary     `json:"summary,omitempty"`
}

// LoadManifest reads the session JSON manifest.
func LoadManifest(workDir string) (*SessionManifest, error) {
	path := filepath.Join(workDir, "manifest.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m SessionManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	cfg := m.Config.withDefaults()
	m.Config = cfg
	return &m, nil
}

// SaveManifest writes the manifest atomically and deterministically.
func SaveManifest(workDir string, m *SessionManifest) error {
	b, err := PrettyJSON(m)
	if err != nil {
		return err
	}
	return AtomicWriteFile(filepath.Join(workDir, "manifest.json"), b)
}

// ReadAndValidateChunk reads WAV bytes and enforces manifest length/rate facts.
func ReadAndValidateChunk(workDir string, session *SessionManifest, chunk ChunkManifest) ([]byte, []int16, error) {
	path := filepath.Join(workDir, chunk.WAVPath)
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	pcm, wavRate, channels, err := ReadWAV(path)
	if err != nil {
		return raw, nil, err
	}
	if wavRate != session.SampleRate || wavRate != session.Config.SampleRate {
		de := newError(CodeRateMismatch, "WAV sample rate conflicts with JSON manifest")
		de.ChunkID = chunk.ChunkID
		de.Stage = StageFrame
		de.Details = map[string]any{"wav_rate": wavRate, "manifest_rate": session.SampleRate, "config_rate": session.Config.SampleRate}
		return raw, pcm, de
	}
	if channels != session.Config.Channels {
		de := newError(CodeRateMismatch, "WAV channel count conflicts with manifest configuration")
		de.ChunkID = chunk.ChunkID
		de.Stage = StageFrame
		de.Details = map[string]any{"wav_channels": channels, "config_channels": session.Config.Channels}
		return raw, pcm, de
	}
	actual := len(pcm)
	if chunk.DeclaredSampleCount != actual || actual > chunk.SampleCountUpperBound {
		de := newError(CodeInputLength, "manifest declared sample total does not match WAV data")
		de.ChunkID = chunk.ChunkID
		de.Stage = StageFrame
		de.Details = map[string]any{
			"actual_samples": actual, "declared_samples": chunk.DeclaredSampleCount,
			"upper_bound": chunk.SampleCountUpperBound,
		}
		return raw, pcm, de
	}
	return raw, pcm, nil
}

// ComputeFingerprints derives content and parameter fingerprints for all stages.
func ComputeFingerprints(chunk ChunkManifest, wavBytes []byte, cfg Config) FingerprintSet {
	content := DigestBytes(wavBytes)
	configVersion := mustDigestValue(cfg)
	frameFP := mustDigestValue(struct {
		Content         string `json:"content"`
		SampleRate      int    `json:"sample_rate"`
		FrameDurationMS int    `json:"frame_duration_ms"`
		HopDurationMS   int    `json:"hop_duration_ms"`
		FrameSamples    int    `json:"frame_samples"`
		HopSamples      int    `json:"hop_samples"`
		FFTSize         int    `json:"fft_size"`
	}{content, cfg.SampleRate, cfg.FrameDurationMS, cfg.HopDurationMS, cfg.FrameSamples(), cfg.HopSamples(), cfg.FFTSize})

	separationFP := mustDigestValue(struct {
		FrameFP           string  `json:"frame_fp"`
		SpeakerCount      int     `json:"speaker_count"`
		MaxSpeakers       int     `json:"max_speakers"`
		MaskMaxIterations int     `json:"mask_max_iterations"`
		MaskEpsilon       float64 `json:"mask_epsilon"`
		MaskFloor         float64 `json:"mask_floor"`
		ConvergenceRounds int     `json:"convergence_rounds"`
	}{frameFP, chunk.SpeakerCount, cfg.MaxSpeakers, cfg.MaskMaxIterations, cfg.MaskEpsilon, cfg.MaskFloor, cfg.ConvergenceRounds})

	embeddingFP := mustDigestValue(struct {
		SeparationFP       string             `json:"separation_fp"`
		ReferenceSegments  []ReferenceSegment `json:"reference_segments"`
		MelBands           int                `json:"mel_bands"`
		EmbeddingDim       int                `json:"embedding_dim"`
		ProjectionInputDim int                `json:"projection_input_dim"`
		ProjectionSeed     uint64             `json:"projection_seed"`
		MarginThreshold    float64            `json:"margin_threshold"`
	}{separationFP, chunk.ReferenceSegments, cfg.MelBands, cfg.EmbeddingDim, cfg.ProjectionInputDim, cfg.ProjectionSeed, cfg.MarginThreshold})

	summaryFP := mustDigestValue(struct {
		EmbeddingFP            string  `json:"embedding_fp"`
		VoiceEnergyThreshold   float64 `json:"voice_energy_threshold"`
		OverlapEnergyThreshold float64 `json:"overlap_energy_threshold"`
		ShortGapMS             int     `json:"short_gap_ms"`
		MinSegmentMS           int     `json:"min_segment_ms"`
		MaxSegmentMS           int     `json:"max_segment_ms"`
	}{embeddingFP, cfg.VoiceEnergyThreshold, cfg.OverlapEnergyThreshold, cfg.ShortGapMS, cfg.MinSegmentMS, cfg.MaxSegmentMS})

	return FingerprintSet{
		ChunkContent: content, ConfigVersion: configVersion, Frame: frameFP,
		Separation: separationFP, Embedding: embeddingFP, Summary: summaryFP,
	}
}

// RunFrameStage performs input validation already done by caller and STFT analysis.
func RunFrameStage(chunk ChunkManifest, pcm []int16, cfg Config, injection Injection) (*SpectralResult, error) {
	spec, err := AnalyzeSpectrum(chunk.ChunkID, PCMToFloat(pcm), cfg)
	if err != nil {
		return nil, err
	}
	if injection == InjectNumericSpectrum && len(spec.Frames) > 0 {
		spec.Frames[0].Bins[1].Real = math.NaN()
		spec.Frames[0].Bins[1].Imag = math.NaN()
		if err := ValidateSpectralResult(spec); err != nil {
			return nil, err
		}
	}
	return spec, nil
}

// ValidateSpectralResult scans all complex bins and descriptors for non-finite values.
func ValidateSpectralResult(spec *SpectralResult) error {
	for _, frame := range spec.Frames {
		for _, bin := range frame.Bins {
			if math.IsNaN(bin.Real) || math.IsInf(bin.Real, 0) || math.IsNaN(bin.Imag) || math.IsInf(bin.Imag, 0) {
				return numericError(spec.ChunkID, frame.Index, "spectral bin is NaN or Inf")
			}
		}
		for name, v := range map[string]float64{"log_energy": frame.LogEnergy, "flatness": frame.Flatness, "energy": frame.Energy, "rms": frame.RMS} {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				de := numericError(spec.ChunkID, frame.Index, "spectral descriptor is NaN or Inf")
				_ = name
				return de
			}
		}
	}
	return nil
}

// RunAllChunkStages executes all four stages without persistence.
func RunAllChunkStages(session *SessionManifest, chunkIndex int, injection Injection) (*ChunkProducts, []byte, []int16, error) {
	chunk := session.Chunks[chunkIndex]
	cfg := session.Config
	raw, pcm, err := ReadAndValidateChunk(".", session, chunk)
	if err != nil {
		return nil, nil, nil, err
	}
	return runChunkWithAudio(session, chunk, raw, pcm, cfg, injection)
}

func runChunkWithAudio(session *SessionManifest, chunk ChunkManifest, raw []byte, pcm []int16, cfg Config, injection Injection) (*ChunkProducts, []byte, []int16, error) {
	fps := ComputeFingerprints(chunk, raw, cfg)
	spec, err := RunFrameStage(chunk, pcm, cfg, injection)
	if err != nil {
		return nil, raw, pcm, err
	}
	sep, err := SeparateSources(spec, chunk.SpeakerCount, cfg, injection)
	if err != nil {
		return nil, raw, pcm, err
	}
	emb, err := RunEmbeddingStage(chunk, spec, sep, cfg, injection)
	if err != nil {
		return nil, raw, pcm, err
	}
	summary, err := RunSummaryStage(chunk, emb, sep, cfg)
	if err != nil {
		return nil, raw, pcm, err
	}
	return &ChunkProducts{
		ChunkID: chunk.ChunkID, ChunkIndex: chunk.Index, Fingerprints: fps,
		Spectral: spec, Separation: sep, Embedding: emb, Summary: summary,
	}, raw, pcm, nil
}
