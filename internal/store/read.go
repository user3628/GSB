package store

import (
	"context"

	"featureplatform/internal/featurex"
	"featureplatform/internal/model"
)

func (s *Store) ReadAdjudicated(ctx context.Context, sampleID string, observedVersion int64) (*model.Record, *model.CodedError) {
	current, minAccepted, err := s.CurrentVersion(ctx)
	if err != nil {
		return nil, &model.CodedError{Code: model.CodeNotFound, Message: err.Error(), SampleID: sampleID}
	}
	if current == 0 {
		return nil, &model.CodedError{Code: model.CodeNotFound, Message: "no effective config version", SampleID: sampleID}
	}
	if observedVersion < current {
		return nil, &model.CodedError{
			Code: model.CodeStaleRead, Message: "observed version is below current effective version",
			SampleID: sampleID, CurrentVersion: current, MinimumAcceptableVersion: minAccepted,
		}
	}
	if observedVersion > current {
		return nil, &model.CodedError{
			Code: model.CodeStaleVersion, Message: "observed version is ahead of current effective version",
			SampleID: sampleID, CurrentVersion: current, MinimumAcceptableVersion: current,
		}
	}
	cfg, err := s.LoadConfig(ctx)
	if err != nil {
		return nil, &model.CodedError{Code: model.CodeNotFound, Message: err.Error(), SampleID: sampleID}
	}

	row, err := s.GetValidRecord(ctx, sampleID)
	if err != nil {
		return nil, &model.CodedError{Code: model.CodeConflict, Message: err.Error(), SampleID: sampleID}
	}
	if row == nil {
		return nil, &model.CodedError{
			Code: model.CodeStaleCheckpoint, Message: "record checkpoint is missing",
			SampleID: sampleID, MissingStage: featurex.StageRecord, CurrentVersion: current,
		}
	}
	rec := row.Record
	actualDigest, err := rec.Digest()
	if err != nil || actualDigest != rec.RecordDigest || actualDigest != row.Record.RecordDigest {
		return nil, &model.CodedError{
			Code: model.CodeStaleCheckpoint, Message: "record digest does not match canonical product",
			SampleID: sampleID, MissingStage: featurex.StageRecord, CurrentVersion: current,
			ExpectedDigest: rec.RecordDigest, ActualDigest: actualDigest,
		}
	}

	for _, stage := range featurex.RequiredStages() {
		cp, err := s.GetCheckpoint(ctx, stage, sampleID)
		if err != nil {
			return nil, &model.CodedError{Code: model.CodeConflict, Message: err.Error(), SampleID: sampleID, MissingStage: stage}
		}
		if cp == nil {
			return nil, &model.CodedError{
				Code: model.CodeStaleCheckpoint, Message: "required stage checkpoint is missing",
				SampleID: sampleID, MissingStage: stage, CurrentVersion: current,
			}
		}
		expected, err := featurex.StageDigest(rec, stage)
		if err != nil {
			return nil, &model.CodedError{Code: model.CodeConflict, Message: err.Error(), SampleID: sampleID, MissingStage: stage}
		}
		if cp.ArtifactDigest != expected {
			return nil, &model.CodedError{
				Code: model.CodeStaleCheckpoint, Message: "stage checkpoint digest does not match record",
				SampleID: sampleID, MissingStage: stage, CurrentVersion: current,
				ExpectedDigest: expected, ActualDigest: cp.ArtifactDigest,
			}
		}
		if stage == featurex.StageNormalize || stage == featurex.StageLabel || stage == featurex.StageFeatureMap || stage == featurex.StageCoverage {
			expectedConfigFP := cfg.ScopedStageFingerprint(rec.Task, stage)
			if cp.StageFingerprint != expectedConfigFP {
				return nil, &model.CodedError{
					Code: model.CodeStaleCheckpoint, Message: "stage configuration fingerprint does not match current version",
					SampleID: sampleID, MissingStage: stage, CurrentVersion: current,
					ExpectedDigest: expectedConfigFP, ActualDigest: cp.StageFingerprint,
				}
			}
		}
	}
	return &rec, nil
}
