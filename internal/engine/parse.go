package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"h2diag/internal/model"
)

var allowedFlags = map[string]struct{}{
	"END_STREAM":  {},
	"END_HEADERS": {},
	"ACK":         {},
	"PADDED":      {},
	"PRIORITY":    {},
}

func ParseSample(data []byte) (*model.Sample, *model.BizError) {
	var sample model.Sample
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&sample); err != nil {
		return nil, model.Biz(model.CodeFrameInvalid, "sample JSON cannot be decoded", map[string]any{"detail": err.Error()})
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, model.Biz(model.CodeFrameInvalid, "sample contains trailing JSON", map[string]any{"detail": err.Error()})
	}
	if biz := ValidateAndNormalize(&sample); biz != nil {
		return nil, biz
	}
	return &sample, nil
}

func ParseSampleReader(reader io.Reader) (*model.Sample, *model.BizError) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, model.Biz(model.CodeFrameInvalid, "sample body cannot be read", map[string]any{"detail": err.Error()})
	}
	return ParseSample(data)
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return fmt.Errorf("second JSON value")
	}
	return err
}

func ValidateAndNormalize(sample *model.Sample) *model.BizError {
	sample.SampleID = strings.TrimSpace(sample.SampleID)
	if sample.SampleID == "" {
		return frameInvalid(nil, "sample_id is required", nil)
	}
	applyDefaults(&sample.Config)
	if biz := validateConfig(sample.Config); biz != nil {
		return biz
	}
	if len(sample.Connections) == 0 {
		return frameInvalid(nil, "at least one connection is required", nil)
	}
	if len(sample.Streams) == 0 {
		return frameInvalid(nil, "at least one stream is required", nil)
	}
	if len(sample.Frames) == 0 {
		return frameInvalid(nil, "at least one frame is required", nil)
	}

	connections := make(map[string]model.ConnectionSpec, len(sample.Connections))
	for i := range sample.Connections {
		spec := &sample.Connections[i]
		spec.ID = strings.TrimSpace(spec.ID)
		if spec.ID == "" {
			return frameInvalid(nil, "connection id is required", map[string]any{"index": i})
		}
		if _, exists := connections[spec.ID]; exists {
			return frameInvalid(nil, "duplicate connection id", map[string]any{"connection_id": spec.ID})
		}
		if spec.PriorityLevels == 0 {
			spec.PriorityLevels = 2
		}
		if spec.PriorityLevels < 2 {
			return frameInvalid(nil, "priority_levels must be at least 2", map[string]any{"connection_id": spec.ID, "priority_levels": spec.PriorityLevels})
		}
		if spec.InitialConnectionWindow <= 0 || spec.MaxConnectionWindow <= 0 ||
			spec.InitialStreamWindow <= 0 || spec.MaxStreamWindow <= 0 {
			return frameInvalid(nil, "window sizes must be positive", map[string]any{"connection_id": spec.ID})
		}
		if spec.InitialConnectionWindow > spec.MaxConnectionWindow {
			return frameInvalid(nil, "initial connection window exceeds maximum", map[string]any{"connection_id": spec.ID})
		}
		if spec.InitialStreamWindow > spec.MaxStreamWindow {
			return frameInvalid(nil, "initial stream window exceeds maximum", map[string]any{"connection_id": spec.ID})
		}
		connections[spec.ID] = *spec
	}

	streams := make(map[streamKey]model.StreamSpec, len(sample.Streams))
	for i := range sample.Streams {
		spec := &sample.Streams[i]
		if _, exists := connections[spec.ConnectionID]; !exists {
			return frameInvalid(nil, "stream references unknown connection", map[string]any{"index": i, "connection_id": spec.ConnectionID})
		}
		if spec.StreamID <= 0 || spec.StreamID > model.MaxHTTP2StreamID {
			return frameInvalid(nil, "stream id is outside HTTP/2 range", map[string]any{"connection_id": spec.ConnectionID, "stream_id": spec.StreamID})
		}
		key := streamKey{ConnectionID: spec.ConnectionID, StreamID: spec.StreamID}
		if _, exists := streams[key]; exists {
			return frameInvalid(nil, "duplicate stream id", map[string]any{"connection_id": spec.ConnectionID, "stream_id": spec.StreamID})
		}
		conn := connections[spec.ConnectionID]
		if spec.Weight <= 0 {
			return frameInvalid(nil, "stream weight must be positive", map[string]any{"connection_id": spec.ConnectionID, "stream_id": spec.StreamID})
		}
		if spec.Priority < 0 || spec.Priority >= conn.PriorityLevels {
			return frameInvalid(nil, "stream priority is outside configured priority levels", map[string]any{
				"connection_id": spec.ConnectionID, "stream_id": spec.StreamID, "priority": spec.Priority, "priority_levels": conn.PriorityLevels,
			})
		}
		if spec.InitialWindow == 0 {
			spec.InitialWindow = conn.InitialStreamWindow
		}
		if spec.MaxWindow == 0 {
			spec.MaxWindow = conn.MaxStreamWindow
		}
		if spec.InitialWindow <= 0 || spec.MaxWindow <= 0 || spec.InitialWindow > spec.MaxWindow {
			return frameInvalid(nil, "stream windows must be positive and initial must not exceed max", map[string]any{
				"connection_id": spec.ConnectionID, "stream_id": spec.StreamID,
			})
		}
		streams[key] = *spec
	}

	frames := append([]model.Frame(nil), sample.Frames...)
	sort.Slice(frames, func(i, j int) bool { return frames[i].Sequence < frames[j].Sequence })
	seenSequences := make(map[int64]struct{}, len(frames))
	var previousTimestamp int64
	for i := range frames {
		frame := &frames[i]
		if frame.Sequence <= 0 {
			return frameInvalid(frame, "frame sequence must be positive", nil)
		}
		if _, exists := seenSequences[frame.Sequence]; exists {
			return frameInvalid(frame, "duplicate frame sequence", nil)
		}
		seenSequences[frame.Sequence] = struct{}{}
		if frame.Timestamp < 0 {
			return frameInvalid(frame, "timestamp must be non-negative", nil)
		}
		if i > 0 && frame.Timestamp < previousTimestamp {
			return frameInvalid(frame, "timestamps must be non-decreasing in sequence order", map[string]any{
				"previous_timestamp": previousTimestamp, "timestamp": frame.Timestamp,
			})
		}
		previousTimestamp = frame.Timestamp
		if _, exists := connections[frame.ConnectionID]; !exists {
			return frameInvalid(frame, "frame references unknown connection", nil)
		}
		switch frame.Type {
		case model.FrameSettings, model.FrameHeaders, model.FrameData, model.FrameWindowUpdate, model.FrameResetStream:
		default:
			return frameInvalid(frame, "unsupported frame type", map[string]any{"frame_type": frame.Type})
		}
		if frame.PayloadBytes < 0 {
			return frameInvalid(frame, "payload_bytes must be non-negative", nil)
		}
		if frame.WindowIncrement < 0 {
			return frameInvalid(frame, "window_increment must be non-negative", nil)
		}
		if frame.Type != model.FrameWindowUpdate && frame.WindowIncrement != 0 {
			return frameInvalid(frame, "window_increment is only legal on WINDOW_UPDATE", nil)
		}
		if err := validateStreamIDForFrame(frame, streams); err != nil {
			return err
		}
		seenFlags := make(map[string]struct{}, len(frame.Flags))
		for _, flag := range frame.Flags {
			if _, ok := allowedFlags[flag]; !ok {
				return frameInvalid(frame, "unsupported frame flag", map[string]any{"flag": flag})
			}
			if _, duplicate := seenFlags[flag]; duplicate {
				return frameInvalid(frame, "duplicate frame flag", map[string]any{"flag": flag})
			}
			seenFlags[flag] = struct{}{}
		}
		if frame.HasFlag("END_STREAM") && frame.Type != model.FrameHeaders && frame.Type != model.FrameData {
			return frameInvalid(frame, "END_STREAM is only legal on HEADERS or DATA", nil)
		}
		if frame.HasFlag("END_HEADERS") && frame.Type != model.FrameHeaders {
			return frameInvalid(frame, "END_HEADERS is only legal on HEADERS", nil)
		}
		if frame.HasFlag("ACK") && frame.Type != model.FrameSettings {
			return frameInvalid(frame, "ACK is only legal on SETTINGS", nil)
		}
		if frame.Type == model.FrameSettings {
			if frame.Settings != nil && frame.Settings.RoundBytes != nil && *frame.Settings.RoundBytes <= 0 {
				return frameInvalid(frame, "SETTINGS round_bytes must be positive", nil)
			}
			if frame.Settings == nil {
				frame.Settings = &model.SettingsPayload{}
			}
		}
	}
	if biz := validateEndStreamOrdering(frames); biz != nil {
		return biz
	}
	sample.Frames = frames
	sample.Normalize()
	return nil
}

func validateStreamIDForFrame(frame *model.Frame, streams map[streamKey]model.StreamSpec) *model.BizError {
	if frame.StreamID < 0 || frame.StreamID > model.MaxHTTP2StreamID {
		return frameInvalid(frame, "stream id is outside HTTP/2 range", map[string]any{"stream_id": frame.StreamID})
	}
	switch frame.Type {
	case model.FrameSettings:
		if frame.StreamID != 0 {
			return frameInvalid(frame, "SETTINGS must use stream id 0", nil)
		}
		return nil
	case model.FrameWindowUpdate:
		if frame.StreamID == 0 {
			return nil
		}
	case model.FrameHeaders, model.FrameData, model.FrameResetStream:
		if frame.StreamID == 0 {
			return frameInvalid(frame, "frame type requires a non-zero stream id", nil)
		}
	}
	key := streamKey{ConnectionID: frame.ConnectionID, StreamID: frame.StreamID}
	if _, exists := streams[key]; !exists {
		return frameInvalid(frame, "frame references undeclared stream", map[string]any{"stream_id": frame.StreamID})
	}
	return nil
}

func validateEndStreamOrdering(frames []model.Frame) *model.BizError {
	closedIntent := make(map[streamKey]int64)
	for i := range frames {
		frame := &frames[i]
		if frame.Type != model.FrameData {
			continue
		}
		key := streamKey{ConnectionID: frame.ConnectionID, StreamID: frame.StreamID}
		if previous, exists := closedIntent[key]; exists {
			return frameInvalid(frame, "DATA follows an END_STREAM DATA intent", map[string]any{"end_stream_sequence": previous})
		}
		if frame.HasFlag("END_STREAM") {
			closedIntent[key] = frame.Sequence
		}
	}
	return nil
}

func applyDefaults(config *model.SampleConfig) {
	defaults := model.DefaultSampleConfig()
	if config.RoundBytes == 0 {
		config.RoundBytes = defaults.RoundBytes
	}
	if config.SchedulingThreshold == 0 {
		config.SchedulingThreshold = defaults.SchedulingThreshold
	}
	if config.Approximation == "" {
		config.Approximation = defaults.Approximation
	}
	if config.WeightScalePPM == 0 {
		config.WeightScalePPM = defaults.WeightScalePPM
	}
	if config.RoundingMode == "" {
		config.RoundingMode = defaults.RoundingMode
	}
	if config.MaxSchedulingRounds == 0 {
		config.MaxSchedulingRounds = defaults.MaxSchedulingRounds
	}
	if config.SamplingFallbackBelow == 0 {
		config.SamplingFallbackBelow = defaults.SamplingFallbackBelow
	}
}

func validateConfig(config model.SampleConfig) *model.BizError {
	if config.RoundBytes <= 0 {
		return frameInvalid(nil, "round_bytes must be positive", nil)
	}
	if config.SchedulingThreshold < 0 {
		return frameInvalid(nil, "scheduling_threshold must be non-negative", nil)
	}
	if config.Approximation != "exact" && config.Approximation != "bucketed" {
		return frameInvalid(nil, "approximation must be exact or bucketed", map[string]any{"approximation": config.Approximation})
	}
	if config.WeightScalePPM != model.FixedScalePPM {
		return frameInvalid(nil, "weight_scale_ppm must be 1000000", map[string]any{"weight_scale_ppm": config.WeightScalePPM})
	}
	if config.RoundingMode != "truncate_then_largest_remainder" {
		return frameInvalid(nil, "unsupported rounding mode", map[string]any{"rounding_mode": config.RoundingMode})
	}
	if config.MaxSchedulingRounds <= 0 {
		return frameInvalid(nil, "max_scheduling_rounds must be positive", nil)
	}
	if config.SamplingFallbackBelow < 0 {
		return frameInvalid(nil, "sampling_fallback_below must be non-negative", nil)
	}
	return nil
}

type streamKey struct {
	ConnectionID string
	StreamID     int64
}

func frameInvalid(frame *model.Frame, message string, context map[string]any) *model.BizError {
	if context == nil {
		context = make(map[string]any)
	}
	if frame != nil {
		context["sequence"] = frame.Sequence
		context["timestamp"] = frame.Timestamp
		context["connection_id"] = frame.ConnectionID
		context["stream_id"] = frame.StreamID
		context["frame_type"] = frame.Type
	}
	return model.Biz(model.CodeFrameInvalid, message, context)
}
