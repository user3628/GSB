package featurex

import (
	"fmt"

	"featureplatform/internal/model"
)

const (
	StageNormalize  = "normalize"
	StageLabel      = "label"
	StageFeatureMap = "feature_map"
	StageCoverage   = "coverage"
	StageRecord     = "record"
)

func digestValue(prefix string, v any) string {
	d, _, err := model.DigestValue(prefix, v)
	if err != nil {
		panic(err)
	}
	return d
}

func StageDigest(rec model.Record, stage string) (string, error) {
	switch stage {
	case StageNormalize:
		return digestValue("checkpoint-normalize/v1:", struct {
			SampleID       string        `json:"sample_id"`
			Task           string        `json:"task"`
			NormalizedText string        `json:"normalized_text"`
			Tokens         []model.Token `json:"tokens"`
		}{rec.SampleID, rec.Task, rec.NormalizedText, rec.Tokens}), nil
	case StageLabel:
		codes := []string{}
		for _, rej := range rec.Rejections {
			if rej.Code == model.CodeLabelUnmapped {
				codes = append(codes, rej.Code)
			}
		}
		return digestValue("checkpoint-label/v1:", struct {
			SampleID          string   `json:"sample_id"`
			RawLabel          string   `json:"raw_label"`
			UnifiedLabel      string   `json:"unified_label"`
			LabelTableVersion string   `json:"label_table_version"`
			RejectionCodes    []string `json:"rejection_codes"`
		}{rec.SampleID, rec.RawLabel, rec.UnifiedLabel, rec.LabelTableVersion, codes}), nil
	case StageFeatureMap:
		return digestValue("checkpoint-feature/v1:", struct {
			SampleID       string             `json:"sample_id"`
			TaskVersion    int                `json:"task_version"`
			MappingVersion string             `json:"mapping_version"`
			VersionStatus  string             `json:"version_status"`
			Warnings       []string           `json:"warnings"`
			Features       []model.FeatureBit `json:"features"`
			ActiveBitCount int                `json:"active_bit_count"`
		}{rec.SampleID, rec.TaskVersion, rec.MappingVersion, rec.VersionStatus, rec.Warnings, rec.Features, rec.ActiveBitCount}), nil
	case StageCoverage:
		return digestValue("checkpoint-coverage/v1:", struct {
			SampleID string                `json:"sample_id"`
			Coverage *model.CoverageResult `json:"coverage"`
			Status   string                `json:"status"`
		}{rec.SampleID, rec.Coverage, rec.Status}), nil
	case StageRecord:
		return rec.Digest()
	default:
		return "", fmt.Errorf("unknown stage %q", stage)
	}
}

func RequiredStages() []string {
	return []string{StageNormalize, StageLabel, StageFeatureMap, StageCoverage, StageRecord}
}
