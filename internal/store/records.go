package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"featureplatform/internal/model"
)

type RecordCommit struct {
	Record             model.Record
	Task               string
	TaskSequence       int64
	ExpectedValidID    int64
	ContentFingerprint string
}

type RecordCommitResult struct {
	ValidID        int64 `json:"valid_id"`
	CurrentVersion int64 `json:"current_version"`
	TaskSequence   int64 `json:"task_sequence"`
}

// CommitRecord enforces per-task contiguous sequence for first commits and
// optimistic valid-id conditional replacement for rebuilds.
func (s *Store) CommitRecord(ctx context.Context, commit RecordCommit) (RecordCommitResult, error) {
	var result RecordCommitResult
	rec := commit.Record
	canonical, err := model.CanonicalJSON(rec)
	if err != nil {
		return result, err
	}
	err = s.transact(func(tx *sql.Tx) error {
		current, _, err := s.currentVersionTx(tx)
		if err != nil {
			return err
		}
		result.CurrentVersion = current
		if rec.ConfigVersion != current {
			return &model.CodedError{Code: model.CodeStaleVersion, Message: "record config_version does not match current effective version", CurrentVersion: current}
		}

		var sampleTask string
		var sampleSeq int64
		if err := tx.QueryRow(`SELECT task,task_seq FROM samples WHERE sample_id=?`, rec.SampleID).Scan(&sampleTask, &sampleSeq); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return &model.CodedError{Code: model.CodeNotFound, Message: "sample is not imported", SampleID: rec.SampleID}
			}
			return err
		}
		if commit.Task != sampleTask || rec.Task != sampleTask || commit.TaskSequence != sampleSeq {
			return &model.CodedError{
				Code: model.CodeStaleVersion, Message: "cross-task or misaligned sequence commit rejected",
				SampleID: rec.SampleID, Task: commit.Task, CurrentVersion: current,
				CurrentTaskSequence: sampleSeq,
			}
		}

		var validID int64
		var validDigest string
		err = tx.QueryRow(`SELECT id,digest FROM record_versions WHERE sample_id=? AND is_valid=1`, rec.SampleID).Scan(&validID, &validDigest)
		hasValid := !errors.Is(err, sql.ErrNoRows)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if commit.ExpectedValidID > 0 && (!hasValid || commit.ExpectedValidID != validID) {
			return &model.CodedError{Code: model.CodeStaleVersion, Message: "record valid id is behind current version", SampleID: rec.SampleID, CurrentVersion: current}
		}
		if commit.ExpectedValidID == 0 && hasValid {
			return &model.CodedError{Code: model.CodeStaleVersion, Message: "expected no valid record but a current version exists", SampleID: rec.SampleID, CurrentVersion: current}
		}
		if commit.ExpectedValidID >= 0 && commit.ExpectedValidID != 0 && commit.ExpectedValidID != validID {
			return &model.CodedError{Code: model.CodeStaleVersion, Message: "record valid id does not match expected version", SampleID: rec.SampleID, CurrentVersion: current}
		}

		var lastSeq int64
		if err := tx.QueryRow(`SELECT COALESCE(last_sequence,0) FROM task_cursors WHERE task=?`, sampleTask).Scan(&lastSeq); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			lastSeq = 0
		}
		if !hasValid {
			if commit.TaskSequence != lastSeq+1 {
				return &model.CodedError{
					Code: model.CodeStaleVersion, Message: "task sequence must advance contiguously",
					SampleID: rec.SampleID, Task: sampleTask, CurrentVersion: current, CurrentTaskSequence: lastSeq,
				}
			}
			cond, err := tx.Exec(`INSERT INTO task_cursors(task,last_sequence) VALUES(?,?)
				ON CONFLICT(task) DO UPDATE SET last_sequence=excluded.last_sequence
				WHERE task_cursors.last_sequence=?`, sampleTask, commit.TaskSequence, lastSeq)
			if err != nil {
				return err
			}
			if n, _ := cond.RowsAffected(); n != 1 {
				return &model.CodedError{Code: model.CodeStaleVersion, Message: "task sequence conditional update lost", Task: sampleTask, CurrentVersion: current, CurrentTaskSequence: lastSeq}
			}
		}

		if hasValid {
			cond, err := tx.Exec(`UPDATE record_versions SET is_valid=0 WHERE id=? AND sample_id=? AND is_valid=1`, validID, rec.SampleID)
			if err != nil {
				return err
			}
			if n, _ := cond.RowsAffected(); n != 1 {
				return &model.CodedError{Code: model.CodeStaleVersion, Message: "record was concurrently replaced", SampleID: rec.SampleID, CurrentVersion: current}
			}
		}

		applicable := 0
		denominator := 0
		if rec.Coverage != nil {
			denominator = rec.Coverage.Denominator
			if rec.Coverage.Applicable {
				applicable = 1
			}
		}
		res, err := tx.Exec(`INSERT INTO record_versions
			(sample_id,config_version,task,unified_label,mapping_version,status,hit_bits,denominator,applicable,canonical_json,digest,is_valid)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,1)`,
			rec.SampleID, rec.ConfigVersion, rec.Task, rec.UnifiedLabel, rec.MappingVersion, rec.Status,
			rec.ActiveBitCount, denominator, applicable, string(canonical), rec.RecordDigest)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		result.ValidID = id
		result.TaskSequence = commit.TaskSequence
		_ = validDigest

		if _, err := tx.Exec(`UPDATE checkpoints SET status='superseded'
			WHERE stage='record' AND unit_id=? AND status='valid'`, rec.SampleID); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO checkpoints
			(stage,unit_id,sample_id,content_fingerprint,config_version,artifact_digest,stage_fingerprint,status)
			VALUES('record',?,?,?,?,?,?,'valid')`,
			rec.SampleID, rec.SampleID, commit.ContentFingerprint, current, rec.RecordDigest, "record:"+rec.RecordDigest); err != nil {
			return err
		}
		return nil
	})
	return result, err
}

type ValidRecordRow struct {
	ValidID int64
	Record  model.Record
}

func (s *Store) GetValidRecord(ctx context.Context, sampleID string) (*ValidRecordRow, error) {
	var id int64
	var raw, digest string
	err := s.db.QueryRowContext(ctx, `SELECT id,canonical_json,digest FROM record_versions
		WHERE sample_id=? AND is_valid=1`, sampleID).Scan(&id, &raw, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec model.Record
	if err := modelJSONUnmarshal([]byte(raw), &rec); err != nil {
		return nil, fmt.Errorf("decode record %s: %w", sampleID, err)
	}
	return &ValidRecordRow{ValidID: id, Record: rec}, nil
}

func (s *Store) ListValidRecordsSorted(ctx context.Context) ([]ValidRecordRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,canonical_json,digest FROM record_versions
		WHERE is_valid=1 ORDER BY sample_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ValidRecordRow{}
	for rows.Next() {
		var id int64
		var raw, digest string
		if err := rows.Scan(&id, &raw, &digest); err != nil {
			return nil, err
		}
		var rec model.Record
		if err := modelJSONUnmarshal([]byte(raw), &rec); err != nil {
			return nil, err
		}
		out = append(out, ValidRecordRow{ValidID: id, Record: rec})
	}
	return out, rows.Err()
}

func (s *Store) ListInvalidations(ctx context.Context) ([]map[string]any, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,config_version,fragment_id,source,plan_json FROM invalidations ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, cfgVersion int64
		var fragmentID, source, planJSON string
		if err := rows.Scan(&id, &cfgVersion, &fragmentID, &source, &planJSON); err != nil {
			return nil, err
		}
		var plan any
		_ = modelJSONUnmarshal([]byte(planJSON), &plan)
		out = append(out, map[string]any{
			"id": id, "config_version": cfgVersion, "fragment_id": fragmentID,
			"source": source, "plan": plan,
		})
	}
	return out, rows.Err()
}
