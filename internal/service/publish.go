package service

import (
	"context"
	"database/sql"
	"strconv"

	"tm-sync/internal/tm"
)

type PublishRequest struct {
	SegmentID     string `json:"segment_id"`
	Seq           int64  `json:"seq"`
	Kind          string `json:"kind"`
	EntityID      string `json:"entity_id"`
	CheckpointSeq int64  `json:"checkpoint_seq,omitempty"`
	Payload       any    `json:"payload,omitempty"`
}

type PublishResult struct {
	SegmentID    string `json:"segment_id"`
	SubmittedSeq int64  `json:"submitted_seq"`
	CurrentSeq   int64  `json:"current_seq"`
	Kind         string `json:"kind"`
	Status       string `json:"status"`
}

func (s *Service) Publish(ctx context.Context, req PublishRequest) (*PublishResult, error) {
	if req.SegmentID == "" {
		return nil, &tm.BizError{Code: "E_SEGMENT_ID_REQUIRED", Message: "segment_id is required for ordered publication", Stage: "publish", HTTPStatus: 400}
	}
	if req.Seq <= 0 {
		return nil, tm.NewStaleCheckpoint(req.Seq, 0, "sequence_must_be_positive")
	}
	if req.Kind == "" {
		req.Kind = "unit"
	}
	result := &PublishResult{SegmentID: req.SegmentID, SubmittedSeq: req.Seq, Kind: req.Kind, Status: "accepted"}
	payload := mustJSON(req.Payload)

	err := s.Store.WithTx(func(tx *sql.Tx) error {
		current, err := currentPublishSeqTx(tx, req.SegmentID)
		if err != nil {
			return err
		}
		result.CurrentSeq = current
		if req.Seq <= current {
			reason := "behind"
			if req.Seq == current {
				reason = "duplicate"
			}
			return tm.NewStaleCheckpoint(req.Seq, current, reason)
		}
		if req.Seq != current+1 {
			return tm.NewStaleCheckpoint(req.Seq, current, "cross_segment_or_gap")
		}
		if req.CheckpointSeq > 0 {
			var segmentID, stage, status string
			err := tx.QueryRow(`SELECT segment_id,stage,status FROM checkpoints WHERE seq_no=?`, req.CheckpointSeq).
				Scan(&segmentID, &stage, &status)
			if err == sql.ErrNoRows {
				return tm.NewMissingPrerequisite(req.SegmentID, "publish", "checkpoint:"+strconv.FormatInt(req.CheckpointSeq, 10))
			}
			if err != nil {
				return err
			}
			if status != "active" {
				return tm.NewMissingPrerequisite(req.SegmentID, "publish", "active_checkpoint:"+stage)
			}
			if segmentID != "" && segmentID != req.SegmentID {
				be := tm.NewStaleCheckpoint(req.Seq, current, "cross_segment_checkpoint")
				be.Details["checkpoint_segment_id"] = segmentID
				be.Details["checkpoint_stage"] = stage
				return be
			}
		}

		res, err := tx.Exec(`INSERT INTO publish_cursors(segment_id,last_seq) VALUES(?,?)
			ON CONFLICT(segment_id) DO UPDATE SET last_seq=excluded.last_seq
			WHERE publish_cursors.last_seq=?`, req.SegmentID, req.Seq, current)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			again, _ := currentPublishSeqTx(tx, req.SegmentID)
			return tm.NewStaleCheckpoint(req.Seq, again, "conditional_update_failed")
		}
		_, err = tx.Exec(`INSERT INTO publications(segment_id,seq,kind,entity_id,checkpoint_seq,payload_json)
			VALUES(?,?,?,?,?,?)`, req.SegmentID, req.Seq, req.Kind, req.EntityID, req.CheckpointSeq, payload)
		if err != nil {
			return err
		}
		result.CurrentSeq = req.Seq
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func currentPublishSeqTx(tx *sql.Tx, segmentID string) (int64, error) {
	var seq int64
	err := tx.QueryRow(`SELECT last_seq FROM publish_cursors WHERE segment_id=?`, segmentID).Scan(&seq)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return seq, err
}
