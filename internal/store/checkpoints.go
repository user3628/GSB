package store

import (
	"context"
	"database/sql"
	"errors"
)

type Checkpoint struct {
	Seq                int64  `json:"seq"`
	Stage              string `json:"stage"`
	UnitID             string `json:"unit_id"`
	SampleID           string `json:"sample_id"`
	ContentFingerprint string `json:"content_fingerprint"`
	ConfigVersion      int64  `json:"config_version"`
	ArtifactDigest     string `json:"artifact_digest"`
	StageFingerprint   string `json:"stage_fingerprint"`
	Status             string `json:"status"`
}

func (s *Store) SaveCheckpoint(ctx context.Context, cp Checkpoint) (int64, error) {
	var seq int64
	err := s.transact(func(tx *sql.Tx) error {
		if cp.Status == "" {
			cp.Status = "valid"
		}
		if _, err := tx.ExecContext(ctx, `UPDATE checkpoints SET status='superseded'
			WHERE stage=? AND unit_id=? AND status='valid'`, cp.Stage, cp.UnitID); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `INSERT INTO checkpoints
			(stage,unit_id,sample_id,content_fingerprint,config_version,artifact_digest,stage_fingerprint,status)
			VALUES(?,?,?,?,?,?,?,?)`,
			cp.Stage, cp.UnitID, cp.SampleID, cp.ContentFingerprint, cp.ConfigVersion,
			cp.ArtifactDigest, cp.StageFingerprint, cp.Status)
		if err != nil {
			return err
		}
		seq, err = res.LastInsertId()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE checkpoints SET superseded_by_seq=?
			WHERE stage=? AND unit_id=? AND status='superseded' AND superseded_by_seq=0`,
			seq, cp.Stage, cp.UnitID)
		return err
	})
	return seq, err
}

func (s *Store) GetCheckpoint(ctx context.Context, stage, unitID string) (*Checkpoint, error) {
	row := s.db.QueryRowContext(ctx, `SELECT seq,stage,unit_id,sample_id,content_fingerprint,config_version,
		artifact_digest,stage_fingerprint,status FROM checkpoints
		WHERE stage=? AND unit_id=? AND status='valid'`, stage, unitID)
	cp, err := scanCheckpoint(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return cp, err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanCheckpoint(row rowScanner) (*Checkpoint, error) {
	var cp Checkpoint
	err := row.Scan(&cp.Seq, &cp.Stage, &cp.UnitID, &cp.SampleID, &cp.ContentFingerprint,
		&cp.ConfigVersion, &cp.ArtifactDigest, &cp.StageFingerprint, &cp.Status)
	if err != nil {
		return nil, err
	}
	return &cp, nil
}

func (s *Store) ListCheckpoints(ctx context.Context) ([]Checkpoint, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT seq,stage,unit_id,sample_id,content_fingerprint,config_version,
		artifact_digest,stage_fingerprint,status FROM checkpoints
		WHERE status='valid' ORDER BY stage, unit_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Checkpoint{}
	for rows.Next() {
		cp, err := scanCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *cp)
	}
	return out, rows.Err()
}

func (s *Store) CheckpointCountsByStage(ctx context.Context) (map[string]int64, int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT stage, COUNT(*) FROM checkpoints WHERE status='valid' GROUP BY stage ORDER BY stage`)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	counts := map[string]int64{}
	var total int64
	for rows.Next() {
		var stage string
		var n int64
		if err := rows.Scan(&stage, &n); err != nil {
			return nil, 0, err
		}
		counts[stage] = n
		total += n
	}
	return counts, total, rows.Err()
}
