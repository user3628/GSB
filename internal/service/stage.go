package service

import (
	"context"
	"database/sql"
	"encoding/json"

	"tm-sync/internal/tm"
)

type StageRequest struct {
	SegmentID string `json:"segment_id"`
	Stage     string `json:"stage"`
	FromStage string `json:"from_stage,omitempty"`
}

type StageResult struct {
	SegmentID string `json:"segment_id"`
	Stage     string `json:"stage"`
	SeqNo     int64  `json:"seq_no"`
	Status    string `json:"status"`
	Detail    any    `json:"detail,omitempty"`
}

func (s *Service) RunStage(ctx context.Context, req StageRequest) (*StageResult, error) {
	if req.SegmentID == "" {
		return nil, &tm.BizError{Code: "E_SEGMENT_ID_REQUIRED", Message: "segment_id is required", Stage: req.Stage, HTTPStatus: 400}
	}
	if stageOrderIndex(req.Stage) < 0 || req.Stage == tm.StageUnit {
		return nil, &tm.BizError{Code: "E_UNKNOWN_STAGE", Message: "stage must be segmentation, alignment, merge, or invalidation", SegmentID: req.SegmentID, Stage: req.Stage, HTTPStatus: 400}
	}
	if req.FromStage != "" {
		if err := validateStageJump(req.SegmentID, req.FromStage, req.Stage); err != nil {
			return nil, err
		}
	}
	version, err := s.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}
	row, err := s.getSegmentRow(req.SegmentID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, &tm.BizError{Code: "E_SEGMENT_NOT_FOUND", Message: "segment not found", SegmentID: req.SegmentID, Stage: req.Stage, HTTPStatus: 404}
	}

	switch req.Stage {
	case tm.StageSegmentation:
		fact, bizErr := tm.ProcessSegment(row.Record, s.Config)
		if bizErr != nil {
			seq, _ := s.writeCheckpoint(CheckpointInput{
				Stage: bizErr.Stage, SegmentID: req.SegmentID, ContentFP: row.ContentFP,
				Version: version, Status: "error", ErrorCode: bizErr.Code,
				ArtifactDigest: tm.SHA256Hex([]byte(bizErr.Error())), Detail: bizErr.Details,
			})
			return &StageResult{SegmentID: req.SegmentID, Stage: bizErr.Stage, SeqNo: seq, Status: "error"}, bizErr
		}
		digest := stageDigest(tm.StageSegmentation, map[string]any{
			"source": fact.Source.ArtifactSum, "target": fact.Target.ArtifactSum,
		})
		seq, err := s.writeCheckpoint(CheckpointInput{
			Stage: tm.StageSegmentation, SegmentID: req.SegmentID, ContentFP: row.ContentFP,
			Version: version, ArtifactDigest: digest,
		})
		return &StageResult{SegmentID: req.SegmentID, Stage: req.Stage, SeqNo: seq, Status: "active"}, err

	case tm.StageAlignment:
		if err := s.withRequiredStage(req.SegmentID, req.Stage, tm.StageSegmentation); err != nil {
			return nil, err
		}
		fact, bizErr := tm.ProcessSegment(row.Record, s.Config)
		if bizErr != nil {
			return nil, bizErr
		}
		seq, err := s.writeCheckpoint(CheckpointInput{
			Stage: tm.StageAlignment, SegmentID: req.SegmentID, ContentFP: row.ContentFP,
			Version: version, ArtifactDigest: fact.Alignment.Digest,
			Detail: map[string]any{
				"pair_count":      len(fact.Alignment.Pairs),
				"candidate_count": len(fact.Alignment.Candidates),
				"alignment":       fact.Alignment,
			},
		})
		return &StageResult{SegmentID: req.SegmentID, Stage: req.Stage, SeqNo: seq, Status: "active"}, err

	case tm.StageMerge:
		if err := s.withRequiredStage(req.SegmentID, req.Stage, tm.StageAlignment); err != nil {
			return nil, err
		}
		fact, bizErr := tm.ProcessSegment(row.Record, s.Config)
		if bizErr != nil {
			return nil, bizErr
		}
		var unitIDs []string
		err := s.Store.WithTx(func(tx *sql.Tx) error {
			var err error
			unitIDs, err = replaceOccurrencesTx(tx, fact)
			if err != nil {
				return err
			}
			_, err = putCheckpointTx(tx, CheckpointInput{
				Stage: tm.StageMerge, SegmentID: req.SegmentID, ContentFP: row.ContentFP,
				Version: version, ArtifactDigest: stageDigest(tm.StageMerge, unitIDs),
				Detail: map[string]any{"unit_ids": unitIDs},
			})
			return err
		})
		if err != nil {
			return nil, storeError(req.SegmentID, tm.StageMerge, err)
		}
		s.maybeCrashAfterCheckpoint()
		for _, unitID := range unitIDs {
			if _, err := s.reconcileUnit(unitID, version); err != nil {
				return nil, err
			}
		}
		cp, err := s.activeCheckpoint(tm.StageMerge, req.SegmentID, "")
		if err != nil || cp == nil {
			return nil, err
		}
		return &StageResult{SegmentID: req.SegmentID, Stage: req.Stage, SeqNo: cp.SeqNo, Status: "active", Detail: map[string]any{"unit_ids": unitIDs}}, nil

	case tm.StageInvalidation:
		if err := s.withRequiredStage(req.SegmentID, req.Stage, tm.StageMerge); err != nil {
			return nil, err
		}
		cp, err := s.activeCheckpoint(tm.StageMerge, req.SegmentID, "")
		if err != nil {
			return nil, err
		}
		var detail struct {
			UnitIDs []string `json:"unit_ids"`
		}
		_ = json.Unmarshal([]byte(cp.DetailJSON), &detail)
		for _, unitID := range detail.UnitIDs {
			unitCP, err := s.activeCheckpoint(tm.StageUnit, "", unitID)
			if err != nil {
				return nil, err
			}
			if unitCP == nil {
				return nil, tm.NewMissingPrerequisite(req.SegmentID, tm.StageInvalidation, "unit:"+unitID)
			}
		}
		seq, err := s.writeInvalidationCheckpoint(*row, version)
		if err != nil {
			return nil, err
		}
		return &StageResult{SegmentID: req.SegmentID, Stage: req.Stage, SeqNo: seq, Status: "active"}, nil
	}
	return nil, &tm.BizError{Code: "E_UNKNOWN_STAGE", Message: "unsupported stage", SegmentID: req.SegmentID, Stage: req.Stage, HTTPStatus: 400}
}

func (s *Service) withRequiredStage(segmentID, target, prerequisite string) error {
	return s.Store.WithTx(func(tx *sql.Tx) error {
		return requireStageTx(tx, segmentID, target, prerequisite)
	})
}
