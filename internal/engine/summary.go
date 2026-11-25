package engine

import (
	"context"
	"sort"

	"featureplatform/internal/configx"
	"featureplatform/internal/model"
	"featureplatform/internal/store"
)

type SummaryRow struct {
	Task                     string   `json:"task"`
	UnifiedLabel             string   `json:"unified_label"`
	MappingVersion           string   `json:"mapping_version"`
	SampleCount              int      `json:"sample_count"`
	HitFeatureBits           int      `json:"hit_feature_bits"`
	CoverageHitBits          int      `json:"coverage_hit_bits"`
	RejectedRecords          int      `json:"rejected_records"`
	CoverageDenominator      int      `json:"coverage_denominator"`
	ApplicableSampleCount    int      `json:"applicable_sample_count"`
	NotApplicableSampleCount int      `json:"not_applicable_sample_count"`
	CoverageApplicable       bool     `json:"coverage_applicable"`
	CoverageRatio            *float64 `json:"coverage_ratio,omitempty"`
	CoverageThreshold        float64  `json:"coverage_threshold"`
	ThresholdMargin          *float64 `json:"threshold_margin,omitempty"`
}

type Summary struct {
	Groups               []SummaryRow `json:"groups"`
	TotalSamples         int          `json:"total_samples"`
	TotalHitFeatureBits  int          `json:"total_hit_feature_bits"`
	TotalRejectedRecords int          `json:"total_rejected_records"`
	NotApplicableSamples int          `json:"not_applicable_samples"`
	NotApplicableGroups  int          `json:"not_applicable_groups"`
	InputSampleCount     int          `json:"input_sample_count"`
	ValidRecordCount     int          `json:"valid_record_count"`
	ConservationOK       bool         `json:"conservation_ok"`
}

func BuildSummary(sampleCount int, rows []store.ValidRecordRow, cfg configx.Config) Summary {
	records := make([]model.Record, 0, len(rows))
	for _, row := range rows {
		records = append(records, row.Record)
	}
	// Explicitly sort by sample_id before accumulation; this is the canonical
	// accumulation order required by the acceptance criterion.
	sort.Slice(records, func(i, j int) bool { return records[i].SampleID < records[j].SampleID })

	type key struct{ task, label, mapping string }
	order := []key{}
	groups := map[key]*SummaryRow{}
	summary := Summary{Groups: []SummaryRow{}}

	for _, rec := range records {
		mappingVersion := rec.MappingVersion
		if mappingVersion == "" {
			mappingVersion = "__UNRESOLVED__"
		}
		k := key{rec.Task, rec.UnifiedLabel, mappingVersion}
		row, ok := groups[k]
		if !ok {
			threshold := cfg.ThresholdFor(rec.Task)
			if rec.Coverage != nil {
				threshold = rec.Coverage.Threshold
			}
			row = &SummaryRow{
				Task: rec.Task, UnifiedLabel: rec.UnifiedLabel, MappingVersion: mappingVersion,
				CoverageThreshold: threshold,
			}
			groups[k] = row
			order = append(order, k)
		}
		row.SampleCount++
		row.HitFeatureBits += rec.ActiveBitCount
		summary.TotalSamples++
		summary.TotalHitFeatureBits += rec.ActiveBitCount
		if rec.Status == "rejected" {
			row.RejectedRecords++
			summary.TotalRejectedRecords++
		}
		if rec.Coverage == nil || !rec.Coverage.Applicable || rec.Coverage.Denominator == 0 {
			row.NotApplicableSampleCount++
			summary.NotApplicableSamples++
		} else {
			row.ApplicableSampleCount++
			row.CoverageDenominator += rec.Coverage.Denominator
			row.CoverageHitBits += rec.Coverage.HitBits
		}
	}

	sort.Slice(order, func(i, j int) bool {
		a, b := order[i], order[j]
		if a.task != b.task {
			return a.task < b.task
		}
		if a.label != b.label {
			return a.label < b.label
		}
		return a.mapping < b.mapping
	})
	for _, k := range order {
		row := groups[k]
		if row.CoverageDenominator == 0 {
			row.CoverageApplicable = false
			summary.NotApplicableGroups++
		} else {
			row.CoverageApplicable = true
			ratio := float64(row.CoverageHitBits) / float64(row.CoverageDenominator)
			margin := ratio - row.CoverageThreshold
			row.CoverageRatio = &ratio
			row.ThresholdMargin = &margin
		}
		summary.Groups = append(summary.Groups, *row)
	}
	summary.InputSampleCount = sampleCount
	summary.ValidRecordCount = len(records)
	summary.ConservationOK = sampleCount == len(records)
	return summary
}

func (e *Engine) Summary(ctx context.Context) (Summary, error) {
	cfg, err := e.Store.LoadConfig(ctx)
	if err != nil {
		return Summary{}, err
	}
	samples, err := e.Store.ListSamples(ctx)
	if err != nil {
		return Summary{}, err
	}
	rows, err := e.Store.ListValidRecordsSorted(ctx)
	if err != nil {
		return Summary{}, err
	}
	return BuildSummary(len(samples), rows, cfg), nil
}

type RejectionItem struct {
	SampleID string              `json:"sample_id"`
	Task     string              `json:"task"`
	Record   model.Record        `json:"record"`
	Reasons  []*model.CodedError `json:"reasons"`
}

func (e *Engine) Rejections(ctx context.Context) ([]RejectionItem, error) {
	rows, err := e.Store.ListValidRecordsSorted(ctx)
	if err != nil {
		return nil, err
	}
	items := []RejectionItem{}
	for _, row := range rows {
		if row.Record.Status != "rejected" {
			continue
		}
		items = append(items, RejectionItem{
			SampleID: row.Record.SampleID, Task: row.Record.Task,
			Record: row.Record, Reasons: row.Record.Rejections,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].SampleID < items[j].SampleID })
	return items, nil
}
