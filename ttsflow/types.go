package ttsflow

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Stage identifies one of the four deterministic synthesis stages.
type Stage uint8

const (
	StageFrontend Stage = iota
	StageAcoustic
	StageWave
	StageConsistency
)

const stageCount = 4

func (s Stage) String() string {
	switch s {
	case StageFrontend:
		return "frontend"
	case StageAcoustic:
		return "acoustic"
	case StageWave:
		return "wave"
	case StageConsistency:
		return "consistency"
	default:
		return fmt.Sprintf("stage(%d)", s)
	}
}

func (s Stage) bit() int { return 1 << uint(s) }

// Checkpoint stages are stored as a bit mask on the single active row for a
// frame segment. A segment is published only after the consistency bit is set.
const completeStageMask = 0b1111

// Prosody contains deterministic, caller-visible control values.
type Prosody struct {
	Rate       float64 `json:"rate" yaml:"rate"`
	Pitch      float64 `json:"pitch" yaml:"pitch"`
	Energy     float64 `json:"energy" yaml:"energy"`
	PauseMS    int     `json:"pause_ms" yaml:"pause_ms"`
	Emphasis   float64 `json:"emphasis" yaml:"emphasis"`
	PitchCurve float64 `json:"pitch_curve" yaml:"pitch_curve"`
}

// TextSample is one line of the generated JSONL corpus.
type TextSample struct {
	UTTID      string  `json:"utt_id" yaml:"utt_id"`
	Text       string  `json:"text" yaml:"text"`
	Language   string  `json:"language" yaml:"language"`
	SpeakerID  int     `json:"speaker_id" yaml:"speaker_id"`
	Prosody    Prosody `json:"prosody" yaml:"prosody"`
	SourceRank int     `json:"source_rank" yaml:"source_rank"`
}

func (t TextSample) Validate() error {
	if strings.TrimSpace(t.UTTID) == "" {
		return fmt.Errorf("utt_id must not be empty")
	}
	if strings.TrimSpace(t.Text) == "" {
		return fmt.Errorf("text must not be empty")
	}
	if t.Language != "zh" && t.Language != "en" {
		return fmt.Errorf("language must be zh or en, got %q", t.Language)
	}
	if t.SpeakerID < 0 {
		return fmt.Errorf("speaker_id must be non-negative")
	}
	return nil
}

// Unit is the smallest indivisible pronunciation segment. A checkpoint is
// written after the frames for each Unit have been produced.
type Unit struct {
	Index        int    `json:"index"`
	ID           string `json:"id"`
	ChunkIndex   int    `json:"chunk_index"`
	Grapheme     string `json:"grapheme"`
	Language     string `json:"language"`
	Phoneme      string `json:"phoneme"`
	Symbol       string `json:"symbol"`
	PhonemeIndex int    `json:"phoneme_index"`
}

func (u Unit) Summary() string {
	return fmt.Sprintf("%06d:%s/%s>%s", u.Index, u.Grapheme, u.Phoneme, u.Symbol)
}

// Chunk is a text-frontend block. Rune and byte offsets refer to the normalized
// text, and chunks partition that text in order.
type Chunk struct {
	Index     int      `json:"index"`
	Text      string   `json:"text"`
	StartRune int      `json:"start_rune"`
	EndRune   int      `json:"end_rune"`
	StartByte int      `json:"start_byte"`
	EndByte   int      `json:"end_byte"`
	Phonemes  []string `json:"phonemes"`
	UnitStart int      `json:"unit_start"`
	UnitEnd   int      `json:"unit_end"`
}

// FrontendResult is deterministic for a sample, config, and split policy.
type FrontendResult struct {
	OriginalText     string  `json:"original_text"`
	NormalizedText   string  `json:"normalized_text"`
	NormalizedDigest string  `json:"normalized_digest"`
	TextDigest       string  `json:"text_digest"`
	SplitDigest      string  `json:"split_digest"`
	Chunks           []Chunk `json:"chunks"`
	Units            []Unit  `json:"units"`
	Digest           string  `json:"digest"`
}

// AcousticFrame is one deterministic frame of band energies and F0.
type AcousticFrame struct {
	Energies []float64 `json:"energies"`
	F0       float64   `json:"f0"`
}

// AcousticSegment is the acoustic artifact for one Unit.
type AcousticSegment struct {
	SegmentSeq int             `json:"segment_seq"`
	Unit       Unit            `json:"unit"`
	FrameStart int             `json:"frame_start"`
	Frames     []AcousticFrame `json:"frames"`
	Bytes      []byte          `json:"-"`
	Digest     string          `json:"digest"`
}

// WaveSegment is the 16-bit PCM artifact for one Unit.
type WaveSegment struct {
	SegmentSeq  int    `json:"segment_seq"`
	Unit        Unit   `json:"unit"`
	ChunkIndex  int    `json:"chunk_index"`
	FrameStart  int    `json:"frame_start"`
	FrameCount  int    `json:"frame_count"`
	SampleStart int    `json:"sample_start"`
	SampleEnd   int    `json:"sample_end"`
	Channels    int    `json:"channels"`
	Bytes       []byte `json:"-"`
	Digest      string `json:"digest"`
	ClampCount  int    `json:"clamp_count"`
}

// ChunkAlignment records the sample interval covered by a text chunk.
type ChunkAlignment struct {
	ChunkIndex  int   `json:"chunk_index"`
	UnitStart   int   `json:"unit_start"`
	UnitEnd     int   `json:"unit_end"`
	SampleStart int   `json:"sample_start"`
	SampleEnd   int   `json:"sample_end"`
	Frames      int   `json:"frames"`
	UnitIndices []int `json:"unit_indices"`
}

// SegmentStats are persisted by the consistency stage.
type SegmentStats struct {
	SegmentSeq          int     `json:"segment_seq"`
	UnitID              string  `json:"unit_id"`
	ChunkIndex          int     `json:"chunk_index"`
	FrameStart          int     `json:"frame_start"`
	FrameCount          int     `json:"frame_count"`
	SampleStart         int     `json:"sample_start"`
	SampleEnd           int     `json:"sample_end"`
	EnergyMin           float64 `json:"energy_min"`
	EnergyMax           float64 `json:"energy_max"`
	EnergyMean          float64 `json:"energy_mean"`
	F0Min               float64 `json:"f0_min"`
	F0Max               float64 `json:"f0_max"`
	PCMPeak             int     `json:"pcm_peak"`
	RMS                 float64 `json:"rms"`
	ClampCount          int     `json:"clamp_count"`
	AcousticDigest      string  `json:"acoustic_digest"`
	PCMDigest           string  `json:"pcm_digest"`
	FrontendFingerprint string  `json:"frontend_fingerprint"`
	AcousticFingerprint string  `json:"acoustic_fingerprint"`
	WaveFingerprint     string  `json:"wave_fingerprint"`
}

// VersionFingerprint exposes the four stage content fingerprints used for
// incremental invalidation.
type VersionFingerprint struct {
	UTTID          string `json:"utt_id"`
	TextDigest     string `json:"text_digest"`
	ConfigDigest   string `json:"config_digest"`
	Frontend       string `json:"frontend"`
	Acoustic       string `json:"acoustic"`
	Wave           string `json:"wave"`
	Consistency    string `json:"consistency"`
	DatasetVersion int64  `json:"dataset_version"`
	ConfigVersion  int64  `json:"config_version"`
}

// UTTResult is the complete, deterministic result for one utterance.
type UTTResult struct {
	Sample          TextSample        `json:"sample"`
	Frontend        FrontendResult    `json:"frontend"`
	Acoustic        []AcousticSegment `json:"-"`
	Wave            []WaveSegment     `json:"-"`
	ChunkAlignments []ChunkAlignment  `json:"chunk_alignments"`
	SegmentStats    []SegmentStats    `json:"segment_stats"`
	AcousticDigest  string            `json:"acoustic_digest"`
	PCMDigest       string            `json:"pcm_digest"`
	StatsDigest     string            `json:"stats_digest"`
	ManifestDigest  string            `json:"manifest_digest"`
	FrameCount      int               `json:"frame_count"`
	SampleCount     int               `json:"sample_count"`
	ClampCount      int               `json:"clamp_count"`
	GracefulStopped bool              `json:"graceful_stopped"`
}

// SynthesisResult is the result of a streaming run or recovery.
type SynthesisResult struct {
	UTTs                       []UTTResult `json:"utts"`
	ManifestDigest             string      `json:"manifest_digest"`
	ConfigDigest               string      `json:"config_digest"`
	TextSetDigest              string      `json:"text_set_digest"`
	CheckpointCount            int         `json:"checkpoint_count"`
	ReusedStageCheckpoints     int         `json:"reused_stage_checkpoints"`
	PublishedStageCheckpoints  int         `json:"published_stage_checkpoints"`
	EarliestInvalidatedSegment int         `json:"earliest_invalidated_segment"`
	EarliestInvalidatedStage   string      `json:"earliest_invalidated_stage"`
	GracefulStopped            bool        `json:"graceful_stopped"`
}

// RunOptions controls a streaming run.
type RunOptions struct {
	UTTIDs       []string
	ForceRebuild bool
	ClearStops   bool
	MaxSegments  int
	Delay        time.Duration
}

// CheckpointStatus is returned by status queries.
type CheckpointStatus struct {
	UTTID                  string   `json:"utt_id"`
	SegmentSeq             int      `json:"segment_seq"`
	StageMask              int      `json:"stage_mask"`
	Stages                 []string `json:"stages"`
	CheckpointSeq          int64    `json:"checkpoint_seq"`
	Generation             int64    `json:"generation"`
	TextDigest             string   `json:"text_digest"`
	ConfigDigest           string   `json:"config_digest"`
	UnitDigest             string   `json:"unit_digest"`
	FrontendFingerprint    string   `json:"frontend_fingerprint"`
	AcousticFingerprint    string   `json:"acoustic_fingerprint"`
	WaveFingerprint        string   `json:"wave_fingerprint"`
	ConsistencyFingerprint string   `json:"consistency_fingerprint"`
	FrontendDigest         string   `json:"frontend_digest"`
	AcousticDigest         string   `json:"acoustic_digest"`
	PCMDigest              string   `json:"pcm_digest"`
	ConsistencyDigest      string   `json:"consistency_digest"`
	FrameStart             int      `json:"frame_start"`
	FrameCount             int      `json:"frame_count"`
	SampleStart            int      `json:"sample_start"`
	SampleEnd              int      `json:"sample_end"`
	ClampCount             int      `json:"clamp_count"`
	Active                 bool     `json:"active"`
}

func (c CheckpointStatus) MarshalJSON() ([]byte, error) {
	type alias CheckpointStatus
	return json.Marshal(alias(c))
}
