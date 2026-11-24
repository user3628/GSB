package cockpitaudio

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
)

const (
	ZoneDriver       = "driver"
	ZonePassenger    = "passenger"
	ZoneRearLeft     = "rear_left"
	ZoneRearRight    = "rear_right"
	StageIngest      = "ingest"
	StageVAD         = "vad"
	StageAttribution = "attribution"
	StageTimeline    = "timeline"
	StageQuery       = "query"
)

var ZoneOrder = []string{ZoneDriver, ZonePassenger, ZoneRearLeft, ZoneRearRight}

type ErrorCode string

const (
	CodeSampleCountMismatch ErrorCode = "E_SAMPLE_COUNT_MISMATCH"
	CodeRateConflict        ErrorCode = "E_RATE_CONFLICT"
	CodeInvalidParam        ErrorCode = "E_INVALID_PARAM"
	CodeLatencyBudget       ErrorCode = "E_LATENCY_BUDGET"
	CodeIncrementalMismatch ErrorCode = "E_INCREMENTAL_MISMATCH"
	CodeStaleCheckpoint     ErrorCode = "E_STALE_CHECKPOINT"
	CodeNotFound            ErrorCode = "E_NOT_FOUND"
	CodeConflict            ErrorCode = "E_CONFLICT"
	CodeStopped             ErrorCode = "E_STOPPED"
	CodeInternal            ErrorCode = "E_INTERNAL"
)

type Error struct {
	Code            ErrorCode         `json:"code"`
	Message         string            `json:"message"`
	ShardID         string            `json:"shard_id,omitempty"`
	Stage           string            `json:"stage,omitempty"`
	QueryID         string            `json:"query_id,omitempty"`
	ElapsedMS       float64           `json:"elapsed_ms,omitempty"`
	BudgetMS        float64           `json:"budget_ms,omitempty"`
	FirstDifference string            `json:"first_difference,omitempty"`
	Diagnostics     map[string]string `json:"diagnostics,omitempty"`
	Cause           string            `json:"cause,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.ShardID != "" || e.Stage != "" {
		return fmt.Sprintf("%s: stage=%s shard=%s: %s", e.Code, e.Stage, e.ShardID, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error {
	if e.Cause == "" {
		return nil
	}
	return errors.New(e.Cause)
}

func NewError(code ErrorCode, stage, shardID, format string, args ...any) *Error {
	return &Error{Code: code, Stage: stage, ShardID: shardID, Message: fmt.Sprintf(format, args...)}
}

func AsError(err error) (*Error, bool) {
	var ae *Error
	if errors.As(err, &ae) {
		return ae, true
	}
	return nil, false
}

type Point struct {
	X float64 `json:"x_m"`
	Y float64 `json:"y_m"`
}

type Config struct {
	SchemaVersion int `json:"schema_version"`
	SampleRateHz  int `json:"sample_rate_hz"`
	Channels      int `json:"channels"`
	BitsPerSample int `json:"bits_per_sample"`

	FrameLengthMS  int     `json:"frame_length_ms"`
	FrameShiftMS   int     `json:"frame_shift_ms"`
	HighThreshold  float64 `json:"high_threshold"`
	LowThreshold   float64 `json:"low_threshold"`
	MinSpeechMS    int     `json:"min_speech_ms"`
	MinSilenceMS   int     `json:"min_silence_ms"`
	MaskIterations int     `json:"mask_iterations"`

	SpeedMPS                float64            `json:"speed_mps"`
	AttributionGapThreshold float64            `json:"attribution_gap_threshold"`
	TDOASigmaMS             float64            `json:"tdoa_sigma_ms"`
	PriorityWeight          float64            `json:"priority_weight"`
	MaxSpeakers             int                `json:"max_speakers"`
	Geometry                map[string]Point   `json:"geometry"`
	Priorities              map[string]float64 `json:"priorities"`

	LatencyBudgetMS float64 `json:"latency_budget_ms"`
}

type ResolvedConfig struct {
	Config
	FrameLengthSamples int    `json:"frame_length_samples"`
	FrameShiftSamples  int    `json:"frame_shift_samples"`
	FFTSamples         int    `json:"fft_samples"`
	ConfigFingerprint  string `json:"config_fingerprint"`
	ConfigVersion      string `json:"config_version"`
}

func DefaultConfig() Config {
	return Config{
		SchemaVersion:           1,
		SampleRateHz:            16000,
		Channels:                1,
		BitsPerSample:           16,
		FrameLengthMS:           25,
		FrameShiftMS:            10,
		HighThreshold:           0.56,
		LowThreshold:            0.34,
		MinSpeechMS:             60,
		MinSilenceMS:            80,
		MaskIterations:          1,
		SpeedMPS:                343.0,
		AttributionGapThreshold: 0.18,
		TDOASigmaMS:             80.0,
		PriorityWeight:          0.35,
		MaxSpeakers:             4,
		Geometry:                DefaultGeometry(),
		Priorities:              DefaultPriorities(),
		LatencyBudgetMS:         5.0,
	}
}

func DefaultGeometry() map[string]Point {
	return map[string]Point{
		ZoneDriver:    {X: -0.82, Y: 0.72},
		ZonePassenger: {X: 0.82, Y: 0.72},
		ZoneRearLeft:  {X: -0.82, Y: -0.72},
		ZoneRearRight: {X: 0.82, Y: -0.72},
	}
}

func DefaultPriorities() map[string]float64 {
	return map[string]float64{
		ZoneDriver: 1, ZonePassenger: 1, ZoneRearLeft: 1, ZoneRearRight: 1,
	}
}

func (c Config) Clone() Config {
	out := c
	if c.Geometry != nil {
		out.Geometry = make(map[string]Point, len(c.Geometry))
		for k, v := range c.Geometry {
			out.Geometry[k] = v
		}
	}
	if c.Priorities != nil {
		out.Priorities = make(map[string]float64, len(c.Priorities))
		for k, v := range c.Priorities {
			out.Priorities[k] = v
		}
	}
	return out
}

func (c Config) ResolveVAD(shardID string) (ResolvedConfig, *Error) {
	if c.SampleRateHz <= 0 {
		return ResolvedConfig{}, NewError(CodeInvalidParam, StageVAD, shardID, "sample_rate_hz must be positive")
	}
	if c.FrameLengthMS <= 0 || c.FrameShiftMS <= 0 {
		return ResolvedConfig{}, NewError(CodeInvalidParam, StageVAD, shardID, "frame length and shift must be positive")
	}
	if c.FrameShiftMS > c.FrameLengthMS {
		return ResolvedConfig{}, NewError(CodeInvalidParam, StageVAD, shardID, "frame_shift_ms must not exceed frame_length_ms")
	}
	if c.HighThreshold <= c.LowThreshold {
		return ResolvedConfig{}, NewError(CodeInvalidParam, StageVAD, shardID, "high threshold must be greater than low threshold")
	}
	if c.MinSpeechMS < 0 || c.MinSilenceMS < 0 {
		return ResolvedConfig{}, NewError(CodeInvalidParam, StageVAD, shardID, "minimum durations must be non-negative")
	}
	if c.MaskIterations < 0 {
		return ResolvedConfig{}, NewError(CodeInvalidParam, StageVAD, shardID, "mask_iterations must be non-negative")
	}

	frameSamples := c.FrameLengthMS * c.SampleRateHz / 1000
	shiftSamples := c.FrameShiftMS * c.SampleRateHz / 1000
	if frameSamples <= 0 || shiftSamples <= 0 {
		return ResolvedConfig{}, NewError(CodeInvalidParam, StageVAD, shardID, "frame ms does not convert to positive integer samples")
	}
	fftSamples := 1
	for fftSamples < frameSamples {
		fftSamples <<= 1
	}

	rc := ResolvedConfig{
		Config:             c.Clone(),
		FrameLengthSamples: frameSamples,
		FrameShiftSamples:  shiftSamples,
		FFTSamples:         fftSamples,
	}
	fp, err := Fingerprint(rc.configFingerprintInput())
	if err != nil {
		return ResolvedConfig{}, NewError(CodeInternal, StageVAD, shardID, "config fingerprint: %v", err)
	}
	rc.ConfigFingerprint = fp
	rc.ConfigVersion = fp[:16]
	return rc, nil
}

func (c Config) ResolveAttribution(shardID string) *Error {
	if c.SpeedMPS <= 0 {
		return NewError(CodeInvalidParam, StageAttribution, shardID, "speed_mps must be positive")
	}
	if c.MaxSpeakers <= 0 {
		return NewError(CodeInvalidParam, StageAttribution, shardID, "max_speakers must be positive")
	}
	if c.AttributionGapThreshold < 0 {
		return NewError(CodeInvalidParam, StageAttribution, shardID, "attribution_gap_threshold must be non-negative")
	}
	if c.TDOASigmaMS <= 0 {
		return NewError(CodeInvalidParam, StageAttribution, shardID, "tdoa_sigma_ms must be positive")
	}
	return nil
}

type configFingerprintInput struct {
	Config Config `json:"config"`
}

func (rc ResolvedConfig) configFingerprintInput() any {
	return struct {
		SchemaVersion   int                `json:"schema_version"`
		SampleRateHz    int                `json:"sample_rate_hz"`
		FrameLengthMS   int                `json:"frame_length_ms"`
		FrameShiftMS    int                `json:"frame_shift_ms"`
		HighThreshold   float64            `json:"high_threshold"`
		LowThreshold    float64            `json:"low_threshold"`
		MinSpeechMS     int                `json:"min_speech_ms"`
		MinSilenceMS    int                `json:"min_silence_ms"`
		MaskIterations  int                `json:"mask_iterations"`
		SpeedMPS        float64            `json:"speed_mps"`
		GapThreshold    float64            `json:"attribution_gap_threshold"`
		TDOASigmaMS     float64            `json:"tdoa_sigma_ms"`
		PriorityWeight  float64            `json:"priority_weight"`
		MaxSpeakers     int                `json:"max_speakers"`
		Geometry        map[string]Point   `json:"geometry"`
		Priorities      map[string]float64 `json:"priorities"`
		LatencyBudgetMS float64            `json:"latency_budget_ms"`
	}{
		SchemaVersion: rc.SchemaVersion, SampleRateHz: rc.SampleRateHz,
		FrameLengthMS: rc.FrameLengthMS, FrameShiftMS: rc.FrameShiftMS,
		HighThreshold: rc.HighThreshold, LowThreshold: rc.LowThreshold,
		MinSpeechMS: rc.MinSpeechMS, MinSilenceMS: rc.MinSilenceMS,
		MaskIterations: rc.MaskIterations, SpeedMPS: rc.SpeedMPS,
		GapThreshold: rc.AttributionGapThreshold, TDOASigmaMS: rc.TDOASigmaMS,
		PriorityWeight: rc.PriorityWeight, MaxSpeakers: rc.MaxSpeakers,
		Geometry: rc.Geometry, Priorities: rc.Priorities,
		LatencyBudgetMS: rc.LatencyBudgetMS,
	}
}

func CanonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func MustCanonicalJSON(v any) []byte {
	b, err := CanonicalJSON(v)
	if err != nil {
		panic(err)
	}
	return b
}

func Fingerprint(v any) (string, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func FingerprintBytes(prefix string, b []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(prefix))
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

func Round(v float64, digits int) float64 {
	p := math.Pow10(digits)
	r := math.Round(v*p) / p
	if math.Abs(r) < 0.5/p {
		return 0
	}
	return r
}

func SortedZoneIDs(zones []string) {
	sort.Strings(zones)
}
