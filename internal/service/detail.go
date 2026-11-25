package service

import (
	"context"
	"encoding/json"
)

type CheckpointDetail struct {
	CheckpointView
	Detail json.RawMessage `json:"detail"`
}

func (s *Service) CheckpointDetail(ctx context.Context, stage, segmentID, unitID string) (*CheckpointDetail, error) {
	cp, err := s.activeCheckpoint(stage, segmentID, unitID)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return nil, nil
	}
	var detail json.RawMessage
	if cp.DetailJSON != "" {
		detail = json.RawMessage(cp.DetailJSON)
	} else {
		detail = json.RawMessage(`{}`)
	}
	return &CheckpointDetail{
		CheckpointView: CheckpointView{
			SeqNo: cp.SeqNo, Stage: cp.Stage, SegmentID: cp.SegmentID, UnitID: cp.UnitID,
			ContentFP: cp.ContentFP, Version: cp.Version, ArtifactDigest: cp.ArtifactDigest,
			Status: cp.Status, ErrorCode: cp.ErrorCode,
		},
		Detail: detail,
	}, nil
}
