package service

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"tm-sync/internal/store"
	"tm-sync/internal/tm"
)

type CheckpointInput struct {
	Stage          string
	SegmentID      string
	UnitID         string
	ContentFP      string
	Version        int64
	ArtifactDigest string
	Status         string
	ErrorCode      string
	Detail         any
}

func (s *Service) writeCheckpoint(in CheckpointInput) (int64, error) {
	var seq int64
	err := s.Store.WithTx(func(tx *sql.Tx) error {
		var err error
		seq, err = putCheckpointTx(tx, in)
		return err
	})
	if err == nil {
		s.maybeCrashAfterCheckpoint()
	}
	return seq, err
}

func putCheckpointTx(tx *sql.Tx, in CheckpointInput) (int64, error) {
	if in.Status == "" {
		in.Status = "active"
	}
	detail := "{}"
	if in.Detail != nil {
		b, err := tm.CanonicalJSON(in.Detail)
		if err != nil {
			return 0, err
		}
		detail = string(b)
	}
	seq, err := store.NextMonotonicSeq(tx, "checkpoint_seq")
	if err != nil {
		return 0, err
	}
	if in.Status == "active" {
		if in.SegmentID != "" {
			if _, err := tx.Exec(`UPDATE checkpoints SET status='superseded'
				WHERE stage=? AND segment_id=? AND status='active'`, in.Stage, in.SegmentID); err != nil {
				return 0, err
			}
		}
		if in.UnitID != "" {
			if _, err := tx.Exec(`UPDATE checkpoints SET status='superseded'
				WHERE stage='unit' AND unit_id=? AND status='active'`, in.UnitID); err != nil {
				return 0, err
			}
		}
	}
	_, err = tx.Exec(`INSERT INTO checkpoints
		(seq_no,stage,segment_id,unit_id,content_fp,version,artifact_digest,status,error_code,detail_json)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		seq, in.Stage, in.SegmentID, in.UnitID, in.ContentFP, in.Version,
		in.ArtifactDigest, in.Status, in.ErrorCode, detail)
	return seq, err
}

type checkpointRow struct {
	SeqNo          int64
	Stage          string
	SegmentID      string
	UnitID         string
	ContentFP      string
	Version        int64
	ArtifactDigest string
	Status         string
	ErrorCode      string
	DetailJSON     string
}

func getActiveCheckpointTx(tx *sql.Tx, stage, segmentID, unitID string) (*checkpointRow, error) {
	row := tx.QueryRow(`SELECT seq_no,stage,segment_id,unit_id,content_fp,version,artifact_digest,status,error_code,detail_json
		FROM checkpoints WHERE stage=? AND segment_id=? AND unit_id=? AND status='active'
		ORDER BY seq_no DESC LIMIT 1`, stage, segmentID, unitID)
	var c checkpointRow
	err := row.Scan(&c.SeqNo, &c.Stage, &c.SegmentID, &c.UnitID, &c.ContentFP, &c.Version,
		&c.ArtifactDigest, &c.Status, &c.ErrorCode, &c.DetailJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *Service) activeCheckpoint(stage, segmentID, unitID string) (*checkpointRow, error) {
	var out *checkpointRow
	err := s.Store.WithTx(func(tx *sql.Tx) error {
		c, err := getActiveCheckpointTx(tx, stage, segmentID, unitID)
		out = c
		return err
	})
	return out, err
}

func listSegmentRecordsTx(tx *sql.Tx) ([]tm.SegmentRecord, error) {
	rows, err := tx.Query(`SELECT raw_json FROM segments ORDER BY segment_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []tm.SegmentRecord
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var r tm.SegmentRecord
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Service) listSegmentRecords() ([]tm.SegmentRecord, error) {
	var out []tm.SegmentRecord
	err := s.Store.WithTx(func(tx *sql.Tx) error {
		var err error
		out, err = listSegmentRecordsTx(tx)
		return err
	})
	return out, err
}

type segmentRow struct {
	Record    tm.SegmentRecord
	ContentFP string
	Version   int64
}

func listSegmentRowsTx(tx *sql.Tx) ([]segmentRow, error) {
	rows, err := tx.Query(`SELECT content_fp,version,raw_json FROM segments ORDER BY segment_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []segmentRow
	for rows.Next() {
		var row segmentRow
		var raw string
		if err := rows.Scan(&row.ContentFP, &row.Version, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(raw), &row.Record); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Service) listSegmentRows() ([]segmentRow, error) {
	var out []segmentRow
	err := s.Store.WithTx(func(tx *sql.Tx) error {
		var err error
		out, err = listSegmentRowsTx(tx)
		return err
	})
	return out, err
}

func stageDigest(stage string, v any) string {
	d, _, _ := tm.DigestCanonical(map[string]any{"stage": stage, "artifact": v})
	return d
}

func requireStageTx(tx *sql.Tx, segmentID, targetStage, prerequisite string) error {
	c, err := getActiveCheckpointTx(tx, prerequisite, segmentID, "")
	if err != nil {
		return err
	}
	if c == nil {
		return tm.NewMissingPrerequisite(segmentID, targetStage, prerequisite)
	}
	return nil
}

func stageOrderIndex(stage string) int {
	order := map[string]int{
		tm.StageSegmentation: 0,
		tm.StageAlignment:    1,
		tm.StageMerge:        2,
		tm.StageUnit:         3,
		tm.StageInvalidation: 4,
	}
	if i, ok := order[stage]; ok {
		return i
	}
	return -1
}

func validateStageJump(segmentID, from, to string) error {
	fi := stageOrderIndex(from)
	ti := stageOrderIndex(to)
	if fi < 0 || ti < 0 || ti != fi+1 {
		return tm.NewStageJump(segmentID, from, to)
	}
	return nil
}

var _ = fmt.Sprintf
