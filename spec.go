package feedbackstream

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
)

type rawEvent struct {
	TimestampMS json.Number `json:"timestamp_ms"`
	PartitionID json.Number `json:"partition_id"`
	GroupKey    string      `json:"group_key"`
	MetricFixed json.Number `json:"metric_fixed"`
	Sequence    json.Number `json:"sequence"`
}

func parseRequiredInt64(lineNo int, field string, value json.Number) (int64, *Error) {
	if strings.TrimSpace(value.String()) == "" {
		return 0, SpecInvalid(fmt.Sprintf("line %d: missing %s", lineNo, field))
	}
	n, intErr := value.Int64()
	if intErr != nil {
		return 0, SpecInvalid(fmt.Sprintf("line %d: %s is outside int64 or not an integer: %q", lineNo, field, value.String()))
	}
	return n, nil
}

func ParseEventsJSONL(r io.Reader) ([]Event, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	events := make([]Event, 0, 64)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		var raw rawEvent
		if err := dec.Decode(&raw); err != nil {
			return nil, SpecInvalid(fmt.Sprintf("line %d: invalid event JSON: %v", lineNo, err))
		}
		var extra any
		if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
			return nil, SpecInvalid(fmt.Sprintf("line %d: unexpected trailing JSON value", lineNo))
		}
		ts, eerr := parseRequiredInt64(lineNo, "timestamp_ms", raw.TimestampMS)
		if eerr != nil {
			return nil, eerr
		}
		partition64, eerr := parseRequiredInt64(lineNo, "partition_id", raw.PartitionID)
		if eerr != nil {
			return nil, eerr
		}
		metric, eerr := parseRequiredInt64(lineNo, "metric_fixed", raw.MetricFixed)
		if eerr != nil {
			return nil, eerr
		}
		sequence, eerr := parseRequiredInt64(lineNo, "sequence", raw.Sequence)
		if eerr != nil {
			return nil, eerr
		}
		if partition64 < 0 || partition64 > math.MaxInt32 {
			return nil, SpecInvalid(fmt.Sprintf("line %d: partition_id must be a non-negative int32", lineNo)).
				With("partition_id", partition64)
		}
		if sequence < 0 {
			return nil, SpecInvalid(fmt.Sprintf("line %d: sequence must be non-negative", lineNo)).
				With("sequence", sequence)
		}
		if ts < 0 {
			return nil, SpecInvalid(fmt.Sprintf("line %d: timestamp_ms must be non-negative", lineNo)).
				With("timestamp_ms", ts)
		}
		if strings.TrimSpace(raw.GroupKey) == "" {
			return nil, SpecInvalid(fmt.Sprintf("line %d: group_key is empty", lineNo))
		}
		if metric < math.MinInt64 || metric > math.MaxInt64 {
			return nil, SpecInvalid(fmt.Sprintf("line %d: metric_fixed is outside int64", lineNo))
		}
		events = append(events, Event{
			TimestampMS: ts,
			PartitionID: int(partition64),
			GroupKey:    raw.GroupKey,
			MetricFixed: metric,
			Sequence:    sequence,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, SpecInvalid("read event stream: " + err.Error())
	}
	if err := ValidateEventStream(events); err != nil {
		return nil, err
	}
	return events, nil
}

// ValidateEventStream enforces partition-local strictly increasing sequences.
func ValidateEventStream(events []Event) error {
	last := map[int]int64{}
	seen := map[int]map[int64]struct{}{}
	for i, event := range events {
		if event.PartitionID < 0 {
			return SpecInvalid("partition_id must be non-negative").
				With("index", i).With("partition_id", event.PartitionID)
		}
		if event.MetricFixed < math.MinInt64 || event.MetricFixed > math.MaxInt64 {
			return SpecInvalid("metric_fixed is outside int64").
				With("index", i).With("sequence", event.Sequence)
		}
		if event.Sequence < 0 {
			return SpecInvalid("sequence must be non-negative").
				With("index", i).With("partition_id", event.PartitionID)
		}
		if prev, ok := last[event.PartitionID]; ok && event.Sequence <= prev {
			return SpecInvalid("event sequence is not strictly monotonic in partition").
				With("index", i).
				With("partition_id", event.PartitionID).
				With("previous_sequence", prev).
				With("sequence", event.Sequence)
		}
		if seen[event.PartitionID] == nil {
			seen[event.PartitionID] = map[int64]struct{}{}
		}
		if _, dup := seen[event.PartitionID][event.Sequence]; dup {
			return SpecInvalid("duplicate event sequence in partition").
				With("partition_id", event.PartitionID).
				With("sequence", event.Sequence)
		}
		seen[event.PartitionID][event.Sequence] = struct{}{}
		last[event.PartitionID] = event.Sequence
	}
	return nil
}

func PolicyChecksum(p *PolicySnapshot) (string, error) {
	if p == nil {
		return "", SpecInvalid("nil policy")
	}
	copyValue := *p
	copyValue.ChecksumSHA256 = ""
	data, err := CanonicalJSON(&copyValue)
	if err != nil {
		return "", err
	}
	return SHA256Hex(data), nil
}

func ParsePolicySnapshot(data []byte) (*PolicySnapshot, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var policy PolicySnapshot
	if err := dec.Decode(&policy); err != nil {
		return nil, SpecInvalid("invalid policy JSON: " + err.Error())
	}
	if err := NormalizeAndValidatePolicy(&policy); err != nil {
		return nil, err
	}
	expected, err := PolicyChecksum(&policy)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(expected, policy.ChecksumSHA256) {
		return nil, SpecInvalid("policy checksum mismatch").
			With("expected_sha256", expected).
			With("actual_sha256", policy.ChecksumSHA256).
			With("version", policy.Version)
	}
	return &policy, nil
}

func NormalizeAndValidatePolicy(p *PolicySnapshot) error {
	if p == nil {
		return SpecInvalid("nil policy")
	}
	if p.SchemaVersion == "" {
		p.SchemaVersion = SchemaPolicySnapshot
	}
	if p.Version <= 0 {
		return SpecInvalid("policy version must be positive").With("version", p.Version)
	}
	if p.MetricScale != MetricScale {
		return SpecInvalid("metric scale must be 1000000").With("metric_scale", p.MetricScale)
	}
	if p.Window.Type != "tumbling" {
		return SpecInvalid("only tumbling windows are supported").With("window_type", p.Window.Type)
	}
	if p.Window.SizeMS <= 0 {
		return SpecInvalid("window size_ms must be positive").With("size_ms", p.Window.SizeMS)
	}
	if p.Window.BoundaryRule == "" {
		return SpecInvalid("window boundary rule must be explicit")
	}
	if p.Window.BoundaryRule != BoundaryStartInclEndExcl {
		return SpecInvalid("unsupported window boundary rule").With("boundary_rule", p.Window.BoundaryRule)
	}
	if p.Window.LatePolicy != LateOriginalWindow {
		return SpecInvalid("unsupported late-event policy").With("late_policy", p.Window.LatePolicy)
	}
	if len(p.Pipeline.Operators) == 0 {
		return SpecInvalid("pipeline has no operators")
	}
	if p.PartitionConcurrency <= 0 {
		return SpecInvalid("partition_concurrency must be positive")
	}
	if p.RoundTimeBudgetMS <= 0 {
		return SpecInvalid("round_time_budget_ms must be positive")
	}
	if p.MaxWaitQueue <= 0 {
		p.MaxWaitQueue = 1
	}
	if p.CostModel.BaseCostMS < 0 || p.CostModel.PerEventCostMS < 0 {
		return SpecInvalid("cost model values must be non-negative")
	}
	if p.CostModel.BaseCostMS == 0 && p.CostModel.PerEventCostMS == 0 {
		p.CostModel.BaseCostMS = 1
	}
	normalizeBuffer := func(b *BufferLimit) {
		if b.HighWatermarkRatio == 0 {
			b.HighWatermarkRatio = 0.8
		}
	}
	normalizeBuffer(&p.DefaultBuffer)
	if p.DefaultBuffer.MaxInflightBatches <= 0 {
		return SpecInvalid("default buffer max_inflight_batches must be positive")
	}
	if p.DefaultBuffer.MaxBytes <= 0 {
		return SpecInvalid("default buffer max_bytes must be positive")
	}
	if p.DefaultBuffer.HighWatermarkRatio <= 0 || p.DefaultBuffer.HighWatermarkRatio > 1 {
		return SpecInvalid("high_watermark_ratio must be in (0,1]")
	}
	for key, buffer := range p.OperatorBuffers {
		b := buffer
		normalizeBuffer(&b)
		if b.MaxInflightBatches <= 0 || b.MaxBytes <= 0 || b.HighWatermarkRatio <= 0 || b.HighWatermarkRatio > 1 {
			return SpecInvalid("invalid operator buffer").With("operator", key)
		}
		p.OperatorBuffers[key] = b
	}
	if p.ChecksumSHA256 == "" {
		sum, err := PolicyChecksum(p)
		if err != nil {
			return err
		}
		p.ChecksumSHA256 = sum
	}
	return nil
}

func (p *PolicySnapshot) BufferFor(groupOrOperatorID string) BufferLimit {
	if b, ok := p.OperatorBuffers[groupOrOperatorID]; ok {
		return b
	}
	return p.DefaultBuffer
}

func ReadEventFile(path string) ([]Event, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseEventsJSONL(bytes.NewReader(data))
}

func ParseEventSnapshot(data []byte) (*EventSnapshot, error) {
	var snapshot EventSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, SpecInvalid("invalid event snapshot JSON: " + err.Error())
	}
	if snapshot.MetricScale != MetricScale {
		return nil, SpecInvalid("event snapshot metric_scale must be 1000000").
			With("metric_scale", snapshot.MetricScale)
	}
	if len(snapshot.Events) == 0 {
		return nil, SpecInvalid("event snapshot contains no events")
	}
	if err := ValidateEventStream(snapshot.Events); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func ReadEventSnapshotFile(path string) (*EventSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseEventSnapshot(data)
}

func ReadPolicyFile(path string) (*PolicySnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParsePolicySnapshot(data)
}
