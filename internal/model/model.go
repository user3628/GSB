package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
)

const (
	MaxHTTP2StreamID int64 = 1<<31 - 1
	FixedScalePPM    int64 = 1_000_000
)

const (
	CodeFrameInvalid       = "E_FRAME_INVALID"
	CodeFlowControlBlocked = "E_FLOW_CONTROL_BLOCKED"
	CodeWindowOverflow     = "E_WINDOW_OVERFLOW"
	CodeInvalidTransition  = "E_INVALID_TRANSITION"
	CodeConflictingSample  = "E_CONFLICTING_SAMPLE"
	CodeCheckpointCorrupt  = "E_CHECKPOINT_CORRUPT"
	CodeSampleAborted      = "E_SAMPLE_ABORTED"
	CodeSchedulerLimit     = "E_SCHEDULER_LIMIT"
	CodeMethodNotAllowed   = "E_METHOD_NOT_ALLOWED"
	CodeNotFound           = "E_NOT_FOUND"
	CodeInternal           = "E_INTERNAL"
)

type BizError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *BizError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func Biz(code, message string, context map[string]any) *BizError {
	return &BizError{Code: code, Message: message, Context: context}
}

func (e *BizError) HTTPStatus() int {
	switch e.Code {
	case CodeFrameInvalid, CodeInvalidTransition:
		return http.StatusBadRequest
	case CodeNotFound:
		return http.StatusNotFound
	case CodeMethodNotAllowed:
		return http.StatusMethodNotAllowed
	case CodeFlowControlBlocked, CodeWindowOverflow, CodeConflictingSample,
		CodeSampleAborted, CodeSchedulerLimit:
		return http.StatusConflict
	case CodeCheckpointCorrupt:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

type FrameType string

const (
	FrameSettings     FrameType = "SETTINGS"
	FrameHeaders      FrameType = "HEADERS"
	FrameData         FrameType = "DATA"
	FrameWindowUpdate FrameType = "WINDOW_UPDATE"
	FrameResetStream  FrameType = "RST_STREAM"
)

type StreamState string

const (
	StateIdle          StreamState = "IDLE"
	StateOpen          StreamState = "OPEN"
	StateHalfClosedLoc StreamState = "HALF_CLOSED_LOCAL"
	StateHalfClosedRem StreamState = "HALF_CLOSED_REMOTE"
	StateClosed        StreamState = "CLOSED"
	StateReset         StreamState = "RESET"
)

type SampleConfig struct {
	RoundBytes            int64  `json:"round_bytes"`
	SchedulingThreshold   int    `json:"scheduling_threshold"`
	Approximation         string `json:"approximation"`
	WeightScalePPM        int64  `json:"weight_scale_ppm"`
	RoundingMode          string `json:"rounding_mode"`
	MaxSchedulingRounds   int    `json:"max_scheduling_rounds"`
	SamplingFallbackBelow int    `json:"sampling_fallback_below"`
}

func DefaultSampleConfig() SampleConfig {
	return SampleConfig{
		RoundBytes:            16 * 1024,
		SchedulingThreshold:   512,
		Approximation:         "exact",
		WeightScalePPM:        FixedScalePPM,
		RoundingMode:          "truncate_then_largest_remainder",
		MaxSchedulingRounds:   1_000_000,
		SamplingFallbackBelow: 32,
	}
}

type ConnectionSpec struct {
	ID                      string `json:"id"`
	InitialConnectionWindow int64  `json:"initial_connection_window"`
	MaxConnectionWindow     int64  `json:"max_connection_window"`
	InitialStreamWindow     int64  `json:"initial_stream_window"`
	MaxStreamWindow         int64  `json:"max_stream_window"`
	PriorityLevels          int    `json:"priority_levels"`
}

type StreamSpec struct {
	ConnectionID  string `json:"connection_id"`
	StreamID      int64  `json:"stream_id"`
	Weight        int64  `json:"weight"`
	Priority      int    `json:"priority"`
	InitialWindow int64  `json:"initial_window,omitempty"`
	MaxWindow     int64  `json:"max_window,omitempty"`
}

type SettingsPayload struct {
	RoundBytes *int64 `json:"round_bytes,omitempty"`
}

type Frame struct {
	Sequence        int64            `json:"sequence"`
	Timestamp       int64            `json:"timestamp"`
	ConnectionID    string           `json:"connection_id"`
	StreamID        int64            `json:"stream_id"`
	Type            FrameType        `json:"type"`
	Flags           []string         `json:"flags"`
	PayloadBytes    int64            `json:"payload_bytes"`
	WindowIncrement int64            `json:"window_increment"`
	Settings        *SettingsPayload `json:"settings,omitempty"`
}

func (f Frame) HasFlag(flag string) bool {
	for _, candidate := range f.Flags {
		if candidate == flag {
			return true
		}
	}
	return false
}

type Sample struct {
	SampleID    string           `json:"sample_id"`
	Config      SampleConfig     `json:"config"`
	Connections []ConnectionSpec `json:"connections"`
	Streams     []StreamSpec     `json:"streams"`
	Frames      []Frame          `json:"frames"`
}

type SampleMetadata struct {
	Config      SampleConfig     `json:"config"`
	Connections []ConnectionSpec `json:"connections"`
	Streams     []StreamSpec     `json:"streams"`
}

type PendingData struct {
	Sequence  int64 `json:"sequence"`
	Remaining int64 `json:"remaining"`
	EndStream bool  `json:"end_stream"`
	Requested int64 `json:"requested"`
}

type ConnectionSnapshot struct {
	ID                 string `json:"id"`
	InitialWindow      int64  `json:"initial_window"`
	MaxWindow          int64  `json:"max_window"`
	WindowRemaining    int64  `json:"window_remaining"`
	PriorityLevels     int    `json:"priority_levels"`
	WindowUpdatedBytes int64  `json:"window_updated_bytes"`
}

type StreamSnapshot struct {
	ConnectionID    string        `json:"connection_id"`
	StreamID        int64         `json:"stream_id"`
	State           StreamState   `json:"state"`
	InitialWindow   int64         `json:"initial_window"`
	MaxWindow       int64         `json:"max_window"`
	WindowRemaining int64         `json:"window_remaining"`
	SentUnacked     int64         `json:"sent_unacked"`
	TotalSent       int64         `json:"total_sent"`
	Weight          int64         `json:"weight"`
	Priority        int           `json:"priority"`
	PendingBytes    int64         `json:"pending_bytes"`
	Pending         []PendingData `json:"pending,omitempty"`
}

type Snapshot struct {
	SampleID                   string               `json:"sample_id"`
	Version                    int64                `json:"version"`
	Connections                []ConnectionSnapshot `json:"connections"`
	Streams                    []StreamSnapshot     `json:"streams"`
	CumulativeRoundingErrorPPM int64                `json:"cumulative_rounding_error_ppm"`
}

type FrameOutcome struct {
	Sequence        int64       `json:"sequence"`
	Type            FrameType   `json:"type"`
	ConnectionID    string      `json:"connection_id"`
	StreamID        int64       `json:"stream_id"`
	Result          string      `json:"result"`
	StateBefore     StreamState `json:"state_before,omitempty"`
	StateAfter      StreamState `json:"state_after,omitempty"`
	WindowIncrement int64       `json:"window_increment,omitempty"`
	Detail          string      `json:"detail,omitempty"`
}

type WeightBit struct {
	StreamID           int64  `json:"stream_id"`
	Weight             int64  `json:"weight"`
	ExactNumerator     string `json:"exact_numerator"`
	TruncatedSharePPM  int64  `json:"truncated_share_ppm"`
	RemainderNumerator string `json:"remainder_numerator"`
	AdjustmentPPM      int64  `json:"adjustment_ppm"`
	NormalizedSharePPM int64  `json:"normalized_share_ppm"`
}

type Selection struct {
	Priority              int    `json:"priority"`
	StreamID              int64  `json:"stream_id"`
	Bytes                 int64  `json:"bytes"`
	RequestedPendingBytes int64  `json:"requested_pending_bytes"`
	Weight                int64  `json:"weight"`
	NormalizedSharePPM    int64  `json:"normalized_share_ppm"`
	Basis                 string `json:"basis"`
	ConnWindowBefore      int64  `json:"connection_window_before"`
	ConnWindowAfter       int64  `json:"connection_window_after"`
	StreamWindowBefore    int64  `json:"stream_window_before"`
	StreamWindowAfter     int64  `json:"stream_window_after"`
}

type SamplingInfo struct {
	Active           bool    `json:"active"`
	EligibleStreams  int     `json:"eligible_streams"`
	Threshold        int     `json:"threshold"`
	SamplingRatePPM  int64   `json:"sampling_rate_ppm"`
	SamplingRate     string  `json:"sampling_rate"`
	BucketMethod     string  `json:"bucket_method"`
	Confidence       string  `json:"confidence"`
	ExactOrder       []int64 `json:"exact_order,omitempty"`
	EstimatedOrder   []int64 `json:"estimated_order,omitempty"`
	DifferenceReason string  `json:"difference_reason,omitempty"`
	FallbackReason   string  `json:"fallback_reason,omitempty"`
}

type RoundTrace struct {
	RoundNo                    int64        `json:"round_no"`
	Timestamp                  int64        `json:"timestamp"`
	ConnectionID               string       `json:"connection_id"`
	Priority                   int          `json:"priority"`
	BudgetBytes                int64        `json:"budget_bytes"`
	ConnectionWindowBefore     int64        `json:"connection_window_before"`
	ConnectionWindowAfter      int64        `json:"connection_window_after"`
	EligibleStreams            int          `json:"eligible_streams"`
	Sampling                   SamplingInfo `json:"sampling"`
	WeightNormalization        []WeightBit  `json:"weight_normalization"`
	RoundRoundingErrorPPM      int64        `json:"round_rounding_error_ppm"`
	CumulativeRoundingErrorPPM int64        `json:"cumulative_rounding_error_ppm"`
	Selections                 []Selection  `json:"selections"`
	Blocked                    []Blocked    `json:"blocked,omitempty"`
}

type Blocked struct {
	ConnectionID        string `json:"connection_id"`
	StreamID            int64  `json:"stream_id"`
	ConnectionRemaining int64  `json:"connection_window_remaining"`
	StreamRemaining     int64  `json:"stream_window_remaining"`
	PendingBytes        int64  `json:"pending_bytes"`
	RequestedBytes      int64  `json:"requested_bytes"`
}

type PlanCommit struct {
	Version    int64          `json:"version"`
	Kind       string         `json:"kind"`
	Timestamp  int64          `json:"timestamp"`
	RoundNo    int64          `json:"round_no,omitempty"`
	Connection string         `json:"connection_id,omitempty"`
	Outcomes   []FrameOutcome `json:"frame_outcomes,omitempty"`
	Round      *RoundTrace    `json:"round,omitempty"`
	Snapshot   Snapshot       `json:"snapshot"`
	Digest     string         `json:"digest"`
}

type RunSummary struct {
	Frames                     int   `json:"frames"`
	EventCommits               int   `json:"event_commits"`
	SchedulingRounds           int   `json:"scheduling_rounds"`
	Selections                 int   `json:"selections"`
	DATAIntentFrames           int   `json:"data_intent_frames"`
	DataBytesSent              int64 `json:"data_bytes_sent"`
	ResolvedBlockingEvents     int   `json:"resolved_blocking_events"`
	CumulativeRoundingErrorPPM int64 `json:"cumulative_rounding_error_ppm"`
	SamplingRounds             int   `json:"sampling_rounds"`
	ExactRounds                int   `json:"exact_rounds"`
}

type Plan struct {
	Sample    *Sample
	Canonical []byte
	Hash      string
	Initial   Snapshot
	Commits   []PlanCommit
	Final     Snapshot
	Summary   RunSummary
	Digest    string
}

type SampleMeta struct {
	SampleID      string `json:"sample_id"`
	CanonicalHash string `json:"canonical_hash"`
	Status        string `json:"status"`
	Version       int64  `json:"version"`
	StateDigest   string `json:"state_digest"`
	ResultJSON    []byte `json:"-"`
	ErrorJSON     []byte `json:"-"`
}

type SnapshotResponse struct {
	SampleID      string      `json:"sample_id"`
	Status        string      `json:"status"`
	CanonicalHash string      `json:"canonical_hash"`
	Snapshot      Snapshot    `json:"snapshot"`
	Summary       *RunSummary `json:"summary,omitempty"`
	TraceURL      string      `json:"trace_url,omitempty"`
	RecoveryURL   string      `json:"recovery_url,omitempty"`
}

type SubmitResponse struct {
	SampleID      string     `json:"sample_id"`
	Status        string     `json:"status"`
	CanonicalHash string     `json:"canonical_hash"`
	Reused        bool       `json:"reused"`
	Version       int64      `json:"version"`
	Summary       RunSummary `json:"summary"`
	Snapshot      Snapshot   `json:"snapshot"`
	TraceURL      string     `json:"trace_url"`
	RecoveryURL   string     `json:"recovery_url"`
}

type RecoveryReport struct {
	ID           int64          `json:"id"`
	SampleID     string         `json:"sample_id"`
	Version      int64          `json:"version"`
	IntentStatus string         `json:"intent_status"`
	Outcome      string         `json:"outcome"`
	Detail       map[string]any `json:"detail"`
	CreatedAt    string         `json:"created_at"`
}

type StoredRound struct {
	Version    int64      `json:"version"`
	RoundNo    int64      `json:"round_no"`
	Timestamp  int64      `json:"timestamp"`
	Connection string     `json:"connection_id"`
	Trace      RoundTrace `json:"trace"`
}

func (s *Sample) Normalize() {
	sort.Slice(s.Connections, func(i, j int) bool { return s.Connections[i].ID < s.Connections[j].ID })
	sort.Slice(s.Streams, func(i, j int) bool {
		if s.Streams[i].ConnectionID == s.Streams[j].ConnectionID {
			return s.Streams[i].StreamID < s.Streams[j].StreamID
		}
		return s.Streams[i].ConnectionID < s.Streams[j].ConnectionID
	})
	sort.Slice(s.Frames, func(i, j int) bool {
		if s.Frames[i].Timestamp == s.Frames[j].Timestamp {
			return s.Frames[i].Sequence < s.Frames[j].Sequence
		}
		return s.Frames[i].Timestamp < s.Frames[j].Timestamp
	})
	for i := range s.Frames {
		flags := append([]string{}, s.Frames[i].Flags...)
		sort.Strings(flags)
		s.Frames[i].Flags = uniqueStrings(flags)
	}
}

func uniqueStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func CanonicalSample(s *Sample) ([]byte, string, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:]), nil
}

func SnapshotDigest(snapshot Snapshot) string {
	copySnapshot := snapshot
	copySnapshot.Connections = append([]ConnectionSnapshot(nil), snapshot.Connections...)
	copySnapshot.Streams = append([]StreamSnapshot(nil), snapshot.Streams...)
	sort.Slice(copySnapshot.Connections, func(i, j int) bool {
		return copySnapshot.Connections[i].ID < copySnapshot.Connections[j].ID
	})
	sort.Slice(copySnapshot.Streams, func(i, j int) bool {
		if copySnapshot.Streams[i].ConnectionID == copySnapshot.Streams[j].ConnectionID {
			return copySnapshot.Streams[i].StreamID < copySnapshot.Streams[j].StreamID
		}
		return copySnapshot.Streams[i].ConnectionID < copySnapshot.Streams[j].ConnectionID
	})
	data, _ := json.Marshal(copySnapshot)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func CloneSnapshot(snapshot Snapshot) Snapshot {
	out := snapshot
	out.Connections = append([]ConnectionSnapshot(nil), snapshot.Connections...)
	out.Streams = make([]StreamSnapshot, len(snapshot.Streams))
	for i, stream := range snapshot.Streams {
		out.Streams[i] = stream
		out.Streams[i].Pending = append([]PendingData(nil), stream.Pending...)
	}
	return out
}
