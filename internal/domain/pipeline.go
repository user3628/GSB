package domain

import (
	"fmt"
	"sort"
	"strings"
)

const (
	StageParseClean = "parse_clean"
	StageFilter     = "filter"
	StageShard      = "shard"
	StageSample     = "sample"
	StageTensorize  = "tensorize"
)

type ParseOutput struct {
	RawDigest      string
	Records        []CleanRecord
	DuplicateCount int
	DuplicateIDs   []string
	Stats          map[string]any
	Result         StageResult
}

func RunParseClean(raw string, policy Policy) (*ParseOutput, error) {
	rawDigest := DigestString("raw-lines-v1", raw)
	parsed, err := ParseRawLines(raw)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(parsed))
	records := make([]CleanRecord, 0, len(parsed))
	duplicateIDs := make([]string, 0)
	duplicateCount := 0
	emptyTextCount := 0
	overlongCount := 0
	invalidLabelCount := 0
	droppedCount := 0

	for _, rawRecord := range parsed {
		if _, exists := seen[rawRecord.ID]; exists {
			duplicateCount++
			if len(duplicateIDs) < 50 {
				duplicateIDs = append(duplicateIDs, rawRecord.ID)
			}
			continue
		}
		seen[rawRecord.ID] = struct{}{}

		if strings.TrimSpace(rawRecord.ShardKey) == "" {
			return nil, NewError(CodeShardKeyMissing,
				fmt.Sprintf("line %d has empty shard key", rawRecord.LineNo),
				map[string]any{"line": rawRecord.LineNo, "sample_id": rawRecord.ID})
		}

		labelValid := containsString(policy.Cleaning.AllowedLabels, rawRecord.Label)
		actions := make([]string, 0, 2)
		if !labelValid {
			invalidLabelCount++
			if policy.Cleaning.InvalidLabelAction == "reject_batch" {
				return nil, NewError(CodeLabelInvalid,
					fmt.Sprintf("line %d label %q is not declared", rawRecord.LineNo, rawRecord.Label),
					map[string]any{
						"line":           rawRecord.LineNo,
						"sample_id":      rawRecord.ID,
						"label":          rawRecord.Label,
						"allowed_labels": policy.Cleaning.AllowedLabels,
					})
			}
			actions = append(actions, "drop_invalid_label")
			droppedCount++
			continue
		}

		effectiveLength := rawRecord.TextLength
		textNull := false
		if rawRecord.TextLength == 0 {
			emptyTextCount++
			switch policy.Cleaning.EmptyTextAction {
			case "drop":
				actions = append(actions, "drop_empty_text")
				droppedCount++
				continue
			default:
				textNull = true
				actions = append(actions, "null_mask_empty_text")
			}
		}
		if rawRecord.TextLength > policy.Cleaning.MaxTextLength {
			overlongCount++
			switch policy.Cleaning.OverlongTextAction {
			case "drop":
				actions = append(actions, "drop_overlong_text")
				droppedCount++
				continue
			default:
				effectiveLength = policy.Cleaning.MaxTextLength
				actions = append(actions, "truncate_overlong_text")
			}
		}

		records = append(records, CleanRecord{
			RawRecord:       rawRecord,
			EffectiveLength: effectiveLength,
			TextNull:        textNull,
			LabelValid:      labelValid,
			Actions:         actions,
			RecordDigest:    DigestValue(rawRecord),
		})
	}

	outputDigest := DigestValue(records)
	stats := map[string]any{
		"line_count":           len(parsed),
		"accepted_count":       len(records),
		"unique_id_count":      len(seen),
		"duplicate_count":      duplicateCount,
		"duplicate_ids":        duplicateIDs,
		"empty_text_count":     emptyTextCount,
		"overlong_text_count":  overlongCount,
		"invalid_label_count":  invalidLabelCount,
		"dropped_count":        droppedCount,
		"empty_text_action":    policy.Cleaning.EmptyTextAction,
		"overlong_action":      policy.Cleaning.OverlongTextAction,
		"invalid_label_action": policy.Cleaning.InvalidLabelAction,
	}
	return &ParseOutput{
		RawDigest:      rawDigest,
		Records:        records,
		DuplicateCount: duplicateCount,
		DuplicateIDs:   duplicateIDs,
		Stats:          stats,
		Result: StageResult{
			Stage:        StageParseClean,
			InputDigest:  rawDigest,
			OutputDigest: outputDigest,
			Stats:        stats,
		},
	}, nil
}

type FilterOutput struct {
	Records []FilteredRecord
	Result  StageResult
}

func RunFilter(input *ParseOutput, policy Policy) (*FilterOutput, error) {
	if err := verifyStageInput(input.Result.OutputDigest, input.Records); err != nil {
		return nil, err
	}
	records := make([]FilteredRecord, 0, len(input.Records))
	reasonCounts := map[string]int{
		"timestamp_before": 0,
		"timestamp_after":  0,
		"text_too_short":   0,
	}
	for _, record := range input.Records {
		reason := ""
		switch {
		case record.TimestampMS < policy.Filter.MinTimestampMS:
			reason = "timestamp_before"
		case record.TimestampMS > policy.Filter.MaxTimestampMS:
			reason = "timestamp_after"
		case record.EffectiveLength < policy.Filter.MinTextLength:
			reason = "text_too_short"
		}
		if reason != "" {
			reasonCounts[reason]++
			continue
		}
		records = append(records, FilteredRecord{CleanRecord: record})
	}
	outputDigest := DigestValue(records)
	stats := map[string]any{
		"input_count":      len(input.Records),
		"output_count":     len(records),
		"filtered_out":     len(input.Records) - len(records),
		"reason_counts":    reasonCounts,
		"min_timestamp_ms": policy.Filter.MinTimestampMS,
		"max_timestamp_ms": policy.Filter.MaxTimestampMS,
		"min_text_length":  policy.Filter.MinTextLength,
	}
	return &FilterOutput{
		Records: records,
		Result: StageResult{
			Stage:        StageFilter,
			InputDigest:  input.Result.OutputDigest,
			OutputDigest: outputDigest,
			Stats:        stats,
		},
	}, nil
}

type ShardOutput struct {
	Assignments []ShardAssignment
	Stats       []ShardStat
	Warnings    []string
	Result      StageResult
}

func RunShard(input *FilterOutput, policy Policy) (*ShardOutput, error) {
	if err := verifyStageInput(input.Result.OutputDigest, input.Records); err != nil {
		return nil, err
	}
	count := policy.Sharding.Count
	assignments := make([]ShardAssignment, 0, len(input.Records))
	stats := make([]ShardStat, count)
	for i := 0; i < count; i++ {
		stats[i] = ShardStat{ShardIndex: i, Warnings: []string{}}
	}
	var totalBytes int64
	for _, record := range input.Records {
		index := FNVShardIndex(record.ShardKey, count, policy.Sharding.Salt)
		assignment := ShardAssignment{
			Record:       record,
			ShardIndex:   index,
			AssignmentID: DigestString(record.RecordDigest, policy.Sharding.Salt, fmt.Sprintf("%d", index)),
		}
		assignments = append(assignments, assignment)
		stats[index].RecordCount++
		stats[index].ByteCount += int64(record.EffectiveLength)
		totalBytes += int64(record.EffectiveLength)
	}

	warnings := make([]string, 0)
	avgCount := float64(len(input.Records)) / float64(count)
	maxRecordRatio := 0.0
	maxByteRatio := 0.0
	avgBytes := float64(totalBytes) / float64(count)
	for i := range stats {
		if avgCount > 0 {
			ratio := (float64(stats[i].RecordCount) - avgCount) / avgCount
			if ratio < 0 {
				ratio = -ratio
			}
			if ratio > maxRecordRatio {
				maxRecordRatio = ratio
			}
			if ratio > policy.Sharding.RecordSkewThreshold {
				message := fmt.Sprintf("W_RECORD_SKEW: shard %d deviation %.4f > %.4f", i, ratio, policy.Sharding.RecordSkewThreshold)
				stats[i].Warnings = append(stats[i].Warnings, message)
				warnings = append(warnings, message)
			}
		}
		if avgBytes > 0 {
			ratio := (float64(stats[i].ByteCount) - avgBytes) / avgBytes
			if ratio < 0 {
				ratio = -ratio
			}
			if ratio > maxByteRatio {
				maxByteRatio = ratio
			}
			if ratio > policy.Sharding.ByteSkewThreshold {
				message := fmt.Sprintf("W_BYTE_SKEW: shard %d deviation %.4f > %.4f", i, ratio, policy.Sharding.ByteSkewThreshold)
				stats[i].Warnings = append(stats[i].Warnings, message)
				warnings = append(warnings, message)
			}
		}
	}

	outputDigest := DigestValue(assignments)
	stageStats := map[string]any{
		"shard_count":      count,
		"total_records":    len(assignments),
		"total_bytes":      totalBytes,
		"average_records":  avgCount,
		"average_bytes":    avgBytes,
		"max_record_skew":  maxRecordRatio,
		"max_byte_skew":    maxByteRatio,
		"record_threshold": policy.Sharding.RecordSkewThreshold,
		"byte_threshold":   policy.Sharding.ByteSkewThreshold,
		"warnings":         warnings,
		"shard_stats":      stats,
	}
	return &ShardOutput{
		Assignments: assignments,
		Stats:       stats,
		Warnings:    warnings,
		Result: StageResult{
			Stage:        StageShard,
			InputDigest:  input.Result.OutputDigest,
			OutputDigest: outputDigest,
			Stats:        stageStats,
		},
	}, nil
}

type SampleOutput struct {
	Records []SampledRecord
	Result  StageResult
}

func RunSample(input *ShardOutput, policy Policy) (*SampleOutput, error) {
	if err := verifyStageInput(input.Result.OutputDigest, input.Assignments); err != nil {
		return nil, err
	}
	if policy.SampleRatio < 0 || policy.SampleRatio > 1 {
		return nil, NewError(CodeSampleRatioInvalid,
			fmt.Sprintf("sample_ratio must be in [0,1], got %.4f", policy.SampleRatio),
			map[string]any{"sample_ratio": policy.SampleRatio})
	}
	rng := NewStageRNG(policy.Seed, StageSample, input.Result.OutputDigest)
	records := make([]SampledRecord, len(input.Assignments))
	selectedCount := 0
	perShard := make([]int, policy.Sharding.Count)
	for i, assignment := range input.Assignments {
		selected := rng.Float64() < policy.SampleRatio
		rank := -1
		if selected {
			selectedCount++
			rank = selectedCount
			perShard[assignment.ShardIndex]++
		}
		records[i] = SampledRecord{Assignment: assignment, Selected: selected, SampleRank: rank}
	}
	outputDigest := DigestValue(records)
	stats := map[string]any{
		"input_count":        len(records),
		"selected_count":     selectedCount,
		"unselected_count":   len(records) - selectedCount,
		"sample_ratio":       policy.SampleRatio,
		"effective_ratio":    safeRatio(selectedCount, len(records)),
		"selected_per_shard": perShard,
		"rng_algorithm":      "splitmix64-sha256-stage-seed",
	}
	return &SampleOutput{
		Records: records,
		Result: StageResult{
			Stage:        StageSample,
			InputDigest:  input.Result.OutputDigest,
			OutputDigest: outputDigest,
			Stats:        stats,
		},
	}, nil
}

type TensorOutput struct {
	Artifacts  []TensorArtifact
	ShardStats []ShardStat
	Result     StageResult
}

func RunTensorize(input *SampleOutput, shardStats []ShardStat, policy Policy, corruptDigestShard int) (*TensorOutput, error) {
	if err := verifyStageInput(input.Result.OutputDigest, input.Records); err != nil {
		return nil, err
	}
	artifacts := make([]TensorArtifact, 0, policy.Sharding.Count)
	stats := make([]ShardStat, len(shardStats))
	copy(stats, shardStats)
	totalBytes := 0
	totalNulls := 0
	totalSamples := 0
	for shardIndex := 0; shardIndex < policy.Sharding.Count; shardIndex++ {
		artifact, err := BuildTensorArtifact(shardIndex, input.Records, policy)
		if err != nil {
			return nil, err
		}
		if corruptDigestShard == shardIndex {
			artifact.Digest = strings.Repeat("0", 64)
		}
		if err := artifact.Verify(); err != nil {
			return nil, err
		}
		artifacts = append(artifacts, *artifact)
		stats[shardIndex].SampledCount = artifact.Header.SampleCount
		stats[shardIndex].ArtifactDigest = artifact.Digest
		var sampledBytes int64
		for _, item := range input.Records {
			if item.Selected && item.Assignment.ShardIndex == shardIndex {
				sampledBytes += int64(item.Assignment.Record.EffectiveLength)
			}
		}
		stats[shardIndex].SampledBytes = sampledBytes
		totalBytes += len(artifact.Bytes)
		totalNulls += artifact.Header.NullCount
		totalSamples += artifact.Header.SampleCount
	}
	manifest := make([]map[string]any, 0, len(artifacts))
	for _, artifact := range artifacts {
		manifest = append(manifest, map[string]any{
			"shard_index":  artifact.ShardIndex,
			"digest":       artifact.Digest,
			"body_digest":  artifact.Header.BodyDigest,
			"sample_count": artifact.Header.SampleCount,
			"null_count":   artifact.Header.NullCount,
			"byte_length":  len(artifact.Bytes),
		})
	}
	outputDigest := DigestValue(manifest)
	stageStats := map[string]any{
		"artifact_count": policy.Sharding.Count,
		"dtype":          "float32",
		"total_samples":  totalSamples,
		"total_nulls":    totalNulls,
		"total_bytes":    totalBytes,
		"artifacts":      manifest,
		"shard_stats":    stats,
	}
	return &TensorOutput{
		Artifacts:  artifacts,
		ShardStats: stats,
		Result: StageResult{
			Stage:        StageTensorize,
			InputDigest:  input.Result.OutputDigest,
			OutputDigest: outputDigest,
			Stats:        stageStats,
		},
	}, nil
}

func RunAllStages(raw string, policy Policy, corruptDigestShard int) (*PipelineResult, error) {
	parseOut, err := RunParseClean(raw, policy)
	if err != nil {
		return nil, err
	}
	filterOut, err := RunFilter(parseOut, policy)
	if err != nil {
		return nil, err
	}
	shardOut, err := RunShard(filterOut, policy)
	if err != nil {
		return nil, err
	}
	sampleOut, err := RunSample(shardOut, policy)
	if err != nil {
		return nil, err
	}
	tensorOut, err := RunTensorize(sampleOut, shardOut.Stats, policy, corruptDigestShard)
	if err != nil {
		return nil, err
	}
	return &PipelineResult{
		PolicyVersion: policy.Version,
		RawInput:      raw,
		RawDigest:     parseOut.RawDigest,
		Stages: []StageResult{
			parseOut.Result, filterOut.Result, shardOut.Result, sampleOut.Result, tensorOut.Result,
		},
		Clean:       parseOut.Records,
		Filtered:    filterOut.Records,
		Assignments: shardOut.Assignments,
		Sampled:     sampleOut.Records,
		ShardStats:  tensorOut.ShardStats,
		Artifacts:   tensorOut.Artifacts,
		Warnings:    shardOut.Warnings,
	}, nil
}

func verifyStageInput(expected string, value any) error {
	if actual := DigestValue(value); actual != expected {
		return NewError(CodeInternal, "stage input digest verification failed",
			map[string]any{"expected": expected, "actual": actual})
	}
	return nil
}

func containsString(values []string, target string) bool {
	sorted := sortedCopy(values)
	i := sort.SearchStrings(sorted, target)
	return i < len(sorted) && sorted[i] == target
}

func sortedCopy(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}

func safeRatio(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}
