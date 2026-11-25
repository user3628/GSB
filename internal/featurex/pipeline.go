package featurex

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"

	"featureplatform/internal/configx"
	"featureplatform/internal/model"
	"featureplatform/internal/textx"
)

const FeatureSchemaVersion = "unified-hash-v1"

type provenanceKey struct {
	index          int
	sourceTask     string
	sourceToken    string
	mappedTerm     string
	mappingVersion string
	mappingBasis   string
}

func HashFeatureTerm(term string, dim int) int {
	sum := sha256.Sum256([]byte("unified-feature/v1\n" + term))
	v := binary.BigEndian.Uint64(sum[:8])
	return int(v % uint64(dim))
}

func BuildRecord(sample model.Sample, cfg configx.Config, taskSequence int64) (model.Record, *model.CodedError) {
	rec := model.Record{
		RecordSchema:      "feature-record/v1",
		SampleID:          sample.SampleID,
		Task:              sample.Task,
		TaskSequence:      taskSequence,
		TaskVersion:       sample.TaskVersion,
		ConfigVersion:     cfg.ConfigVersion,
		LabelTableVersion: cfg.LabelTableVersion,
		RawLabel:          sample.Label,
		VersionStatus:     "not_evaluated",
		Warnings:          []string{},
		Features:          []model.FeatureBit{},
		Tokens:            []model.Token{},
		Rejections:        []*model.CodedError{},
	}

	addRejection := func(err *model.CodedError) *model.CodedError {
		if err == nil {
			return nil
		}
		if err.SampleID == "" {
			err.SampleID = sample.SampleID
		}
		if err.Task == "" {
			err.Task = sample.Task
		}
		rec.Rejections = append(rec.Rejections, err)
		return err
	}

	if cfg.HashDim == 0 {
		err := addRejection(model.HashDimZero(sample.SampleID, sample.Task))
		rec.Status = "rejected"
		_ = rec.FillDigest()
		return rec, err
	}
	if cfg.HashDim < 0 {
		err := addRejection(&model.CodedError{Code: model.CodeHashDimZero, Message: "hash_dim must be non-negative", SampleID: sample.SampleID, Task: sample.Task})
		rec.Status = "rejected"
		_ = rec.FillDigest()
		return rec, err
	}

	rule, appliedVersion, versionStatus, warnings, ruleErr := cfg.RuleFor(sample.Task, sample.TaskVersion)
	rec.VersionStatus = versionStatus
	rec.Warnings = append(rec.Warnings, warnings...)
	if ruleErr != nil {
		addRejection(ruleErr)
	} else {
		rec.MappingVersion = rule.MappingVersion
		_ = appliedVersion
	}

	if labels, ok := cfg.LabelMappings[sample.Task]; ok {
		if unified, ok := labels[sample.Label]; ok {
			rec.UnifiedLabel = unified
		} else {
			addRejection(model.LabelUnmapped(sample.SampleID, sample.Task, sample.Label))
			rec.UnifiedLabel = "__UNMAPPED__"
		}
	} else {
		addRejection(model.LabelUnmapped(sample.SampleID, sample.Task, sample.Label))
		rec.UnifiedLabel = "__UNMAPPED__"
	}

	normalized, tokens := textx.NormalizeAndTokenize(sample.RawText)
	rec.NormalizedText = normalized
	rec.Tokens = tokens
	if normalized == "" {
		addRejection(model.EmptyText(sample.SampleID, sample.Task))
	}

	if ruleErr == nil && normalized != "" {
		seenProvenance := map[provenanceKey]struct{}{}
		active := map[int]struct{}{}
		for _, tok := range tokens {
			mappedTerm := tok.Text
			basis := "identity"
			if mapped, ok := rule.Terms[tok.Text]; ok {
				mappedTerm = mapped
				basis = "explicit_term_map"
			}
			idx := HashFeatureTerm(mappedTerm, cfg.HashDim)
			active[idx] = struct{}{}
			key := provenanceKey{idx, sample.Task, tok.Text, mappedTerm, rule.MappingVersion, basis}
			if _, exists := seenProvenance[key]; exists {
				continue
			}
			seenProvenance[key] = struct{}{}
			rec.Features = append(rec.Features, model.FeatureBit{
				FeatureIndex:   idx,
				SourceTask:     sample.Task,
				SourceToken:    tok.Text,
				MappedTerm:     mappedTerm,
				MappingBasis:   basis,
				MappingVersion: rule.MappingVersion,
			})
		}
		sort.Slice(rec.Features, func(i, j int) bool {
			a, b := rec.Features[i], rec.Features[j]
			if a.FeatureIndex != b.FeatureIndex {
				return a.FeatureIndex < b.FeatureIndex
			}
			if a.MappedTerm != b.MappedTerm {
				return a.MappedTerm < b.MappedTerm
			}
			if a.SourceToken != b.SourceToken {
				return a.SourceToken < b.SourceToken
			}
			return a.MappingBasis < b.MappingBasis
		})
		rec.ActiveBitCount = len(active)
	}

	threshold := cfg.ThresholdFor(sample.Task)
	cov := &model.CoverageResult{Threshold: threshold, DeclarationValid: true, Messages: []string{}}
	if sample.Coverage == nil || sample.Coverage.Denominator == 0 {
		cov.Applicable = false
		cov.Denominator = 0
		cov.HitBits = rec.ActiveBitCount
		cov.Messages = append(cov.Messages, "coverage_denominator_zero_not_applicable")
	} else if sample.Coverage.Denominator < 0 {
		cov.Applicable = false
		cov.Denominator = sample.Coverage.Denominator
		cov.HitBits = rec.ActiveBitCount
		cov.DeclarationValid = false
		cov.Messages = append(cov.Messages, "E_COVERAGE_DECL_INVALID: denominator must be non-negative")
		addRejection(&model.CodedError{Code: model.CodeCoverageDeclInvalid, Message: "coverage denominator is negative", SampleID: sample.SampleID, Task: sample.Task})
	} else {
		cov.Applicable = true
		cov.Denominator = sample.Coverage.Denominator
		cov.HitBits = rec.ActiveBitCount
		ratio := float64(cov.HitBits) / float64(cov.Denominator)
		margin := ratio - threshold
		cov.CoverageRatio = &ratio
		cov.ThresholdMargin = &margin
		if sample.Coverage.DeclaredHitBits != nil && *sample.Coverage.DeclaredHitBits != cov.HitBits {
			cov.DeclarationValid = false
			cov.Messages = append(cov.Messages, "W_COVERAGE_DECL_HIT_MISMATCH: declared_hit_bits does not equal actual hash bits")
			rec.Warnings = append(rec.Warnings, "W_COVERAGE_DECL_HIT_MISMATCH")
		}
		if sample.Coverage.DeclaredRatio != nil {
			declared := *sample.Coverage.DeclaredRatio
			if declared < 0 || declared > 1 {
				cov.DeclarationValid = false
				cov.Messages = append(cov.Messages, "W_COVERAGE_DECL_RATIO_INVALID")
			} else {
				diff := declared - ratio
				if diff < 0 {
					diff = -diff
				}
				if diff > 0.000001 {
					cov.DeclarationValid = false
					cov.Messages = append(cov.Messages, "W_COVERAGE_DECL_RATIO_MISMATCH")
					rec.Warnings = append(rec.Warnings, "W_COVERAGE_DECL_RATIO_MISMATCH")
				}
			}
		}
		if ratio < threshold {
			addRejection(model.CoverageLow(sample.SampleID, sample.Task, ratio, threshold))
		}
	}
	rec.Coverage = cov
	sort.Strings(rec.Warnings)
	if len(rec.Rejections) > 0 {
		rec.Status = "rejected"
	} else {
		rec.Status = "migratable"
	}
	if err := rec.FillDigest(); err != nil {
		return rec, &model.CodedError{Code: "E_DIGEST", Message: err.Error(), SampleID: sample.SampleID, Task: sample.Task}
	}
	if len(rec.Rejections) > 0 {
		return rec, rec.Rejections[0]
	}
	return rec, nil
}
