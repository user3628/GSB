package domain

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

type RawRecord struct {
	LineNo      int    `json:"line_no"`
	TimestampMS int64  `json:"timestamp_ms"`
	ID          string `json:"id"`
	TextLength  int    `json:"text_length"`
	Label       string `json:"label"`
	ShardKey    string `json:"shard_key"`
	Raw         string `json:"raw"`
}

type CleanRecord struct {
	RawRecord
	EffectiveLength int      `json:"effective_length"`
	TextNull        bool     `json:"text_null"`
	LabelValid      bool     `json:"label_valid"`
	Actions         []string `json:"actions,omitempty"`
	RecordDigest    string   `json:"record_digest"`
}

type FilteredRecord struct {
	CleanRecord
	FilterReason string `json:"filter_reason,omitempty"`
}

type ShardAssignment struct {
	Record       FilteredRecord `json:"record"`
	ShardIndex   int            `json:"shard_index"`
	AssignmentID string         `json:"assignment_id"`
}

type SampledRecord struct {
	Assignment ShardAssignment `json:"assignment"`
	Selected   bool            `json:"selected"`
	SampleRank int             `json:"sample_rank"`
}

type ShardStat struct {
	ShardIndex     int      `json:"shard_index"`
	RecordCount    int      `json:"record_count"`
	SampledCount   int      `json:"sampled_count"`
	ByteCount      int64    `json:"byte_count"`
	SampledBytes   int64    `json:"sampled_bytes"`
	Warnings       []string `json:"warnings,omitempty"`
	ArtifactPath   string   `json:"artifact_path,omitempty"`
	ArtifactDigest string   `json:"artifact_digest,omitempty"`
}

type StageResult struct {
	Stage        string         `json:"stage"`
	InputDigest  string         `json:"input_digest"`
	OutputDigest string         `json:"output_digest"`
	Stats        map[string]any `json:"stats"`
}

type PipelineResult struct {
	PolicyVersion int64             `json:"policy_version"`
	RawInput      string            `json:"raw_input"`
	RawDigest     string            `json:"raw_digest"`
	Stages        []StageResult     `json:"stages"`
	Clean         []CleanRecord     `json:"-"`
	Filtered      []FilteredRecord  `json:"-"`
	Assignments   []ShardAssignment `json:"-"`
	Sampled       []SampledRecord   `json:"-"`
	ShardStats    []ShardStat       `json:"shard_stats"`
	Artifacts     []TensorArtifact  `json:"-"`
	Warnings      []string          `json:"warnings"`
}

func ParseRawLines(raw string) ([]RawRecord, error) {
	scanner := bufio.NewScanner(strings.NewReader(raw))
	scanner.Buffer(make([]byte, 1024), 16*1024*1024)
	var records []RawRecord
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			return nil, NewError(CodeRecordInvalid, fmt.Sprintf("line %d is empty", lineNo),
				map[string]any{"line": lineNo})
		}
		parts := strings.Split(line, "|")
		if len(parts) != 5 {
			return nil, NewError(CodeRecordInvalid,
				fmt.Sprintf("line %d must contain 5 pipe-separated fields", lineNo),
				map[string]any{"line": lineNo, "fields": len(parts)})
		}
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		ts, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || ts < 0 {
			return nil, NewError(CodeRecordInvalid, fmt.Sprintf("line %d has invalid timestamp", lineNo),
				map[string]any{"line": lineNo, "value": parts[0]})
		}
		textLength, err := strconv.Atoi(parts[2])
		if err != nil || textLength < 0 || textLength > 1_000_000 {
			return nil, NewError(CodeRecordInvalid, fmt.Sprintf("line %d has invalid text length", lineNo),
				map[string]any{"line": lineNo, "value": parts[2]})
		}
		record := RawRecord{
			LineNo:      lineNo,
			TimestampMS: ts,
			ID:          parts[1],
			TextLength:  textLength,
			Label:       parts[3],
			ShardKey:    parts[4],
			Raw:         line,
		}
		if record.ID == "" {
			return nil, NewError(CodeRecordInvalid, fmt.Sprintf("line %d has empty sample id", lineNo),
				map[string]any{"line": lineNo})
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, NewError(CodeRecordInvalid, "raw input could not be scanned: "+err.Error())
	}
	if len(records) == 0 {
		return nil, NewError(CodeRecordInvalid, "raw input contains no records")
	}
	return records, nil
}

func FNVShardIndex(shardKey string, count int, salt string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(salt))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(shardKey))
	return int(h.Sum64() % uint64(count))
}

func RecordDigest(r any) string {
	return DigestValue(r)
}
