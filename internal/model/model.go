package model

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	CodeParamInvalid       = "E_PARAM_INVALID"
	CodeSampleRateConflict = "E_SAMPLE_RATE_CONFLICT"
	CodeFrameShiftZero     = "E_FRAME_SHIFT_ZERO"
	CodeStreamMismatch     = "E_STREAM_MISMATCH"
	CodeStaleBase          = "E_STALE_BASE"
	CodeStaleCheckpoint    = "E_STALE_CHECKPOINT"
	CodeToleranceExceeded  = "E_TOLERANCE_EXCEEDED"
	CodeBudgetExhausted    = "E_BUDGET_EXHAUSTED"
	CodeQuotaExceeded      = "E_QUOTA_EXCEEDED"
	CodeNotFound           = "E_NOT_FOUND"
	CodeConflict           = "E_CONFLICT"
)

// BusinessError is the stable JSON error envelope used by Go, HTTP and the CLI.
type BusinessError struct {
	Code                 string         `json:"code"`
	Message              string         `json:"message"`
	SegmentID            string         `json:"segment_id,omitempty"`
	FrameIndex           *int           `json:"frame_index,omitempty"`
	CurrentVersion       int64          `json:"current_version,omitempty"`
	FirstDifferenceFrame *int           `json:"first_difference_frame,omitempty"`
	Expected             any            `json:"expected,omitempty"`
	Actual               any            `json:"actual,omitempty"`
	MaxDeviation         float64        `json:"max_deviation,omitempty"`
	Details              map[string]any `json:"details,omitempty"`
}

func (e *BusinessError) Error() string {
	if e == nil {
		return ""
	}
	b, _ := json.Marshal(e)
	return string(b)
}

func (e *BusinessError) Is(target error) bool {
	var other *BusinessError
	if errors.As(target, &other) {
		return other.Code == "" || other.Code == e.Code
	}
	return false
}

func NewError(code, segmentID string, format string, args ...any) *BusinessError {
	return &BusinessError{Code: code, SegmentID: segmentID, Message: fmt.Sprintf(format, args...)}
}

func NewParamError(segmentID string, frameIndex int, format string, args ...any) *BusinessError {
	frame := frameIndex
	return &BusinessError{
		Code:       CodeParamInvalid,
		SegmentID:  segmentID,
		FrameIndex: &frame,
		Message:    fmt.Sprintf(format, args...),
	}
}

func AsBusinessError(err error) (*BusinessError, bool) {
	var business *BusinessError
	if errors.As(err, &business) {
		return business, true
	}
	return nil, false
}

type Manifest struct {
	SchemaVersion string            `json:"schema_version"`
	SampleRate    int               `json:"sample_rate"`
	Channels      int               `json:"channels"`
	BitsPerSample int               `json:"bits_per_sample"`
	Seed          int64             `json:"seed"`
	Segments      []SegmentManifest `json:"segments"`
}

type SegmentManifest struct {
	ID string `json:"id"`
	// WAVPath is relative to the manifest file when loaded from disk.
	WAVPath string `json:"wav_path"`

	SourceSpeaker string `json:"source_speaker"`
	TargetSpeaker string `json:"target_speaker"`

	F0Shift       float64 `json:"f0_shift"`
	EnvelopeWarp  float64 `json:"envelope_warp"`
	FrameShiftMS  float64 `json:"frame_shift_ms"`
	FrameLengthMS float64 `json:"frame_length_ms,omitempty"`

	// BlockLength is measured in analysis frames. BlockFrames is an accepted alias.
	BlockLength    int `json:"block_length"`
	BlockFrames    int `json:"block_frames,omitempty"`
	FramesPerGroup int `json:"frames_per_group,omitempty"`

	// ExpectedSamplesUpperBound is the required exact output length for this build.
	ExpectedSamplesUpperBound int `json:"expected_samples_upper_bound"`
	// ExpectedSamples is a compatibility alias.
	ExpectedSamples int `json:"expected_samples,omitempty"`

	ReferenceF0MeanRange [2]float64 `json:"reference_f0_mean_range"`
	F0ToleranceHz        float64    `json:"f0_tolerance_hz,omitempty"`
	MinInRangeRatio      float64    `json:"min_in_range_ratio,omitempty"`

	MaxGroups        int   `json:"max_groups,omitempty"`
	RoundBudgetNanos int64 `json:"round_budget_nanos,omitempty"`
}

func (s SegmentManifest) EffectiveBlockFrames() int {
	if s.BlockLength > 0 {
		return s.BlockLength
	}
	if s.BlockFrames > 0 {
		return s.BlockFrames
	}
	return 4
}

func (s SegmentManifest) EffectiveExpectedSamples() int {
	if s.ExpectedSamplesUpperBound > 0 {
		return s.ExpectedSamplesUpperBound
	}
	return s.ExpectedSamples
}

func (s SegmentManifest) EffectiveFrameLengthMS() float64 {
	if s.FrameLengthMS > 0 {
		return s.FrameLengthMS
	}
	if s.FrameShiftMS > 0 {
		return s.FrameShiftMS * 2
	}
	return 20
}

const (
	MinEnvelopeWarp = 0.5
	MaxEnvelopeWarp = 2.0
)
