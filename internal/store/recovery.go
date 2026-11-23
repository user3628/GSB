package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"h2diag/internal/model"
)

type runningIntent struct {
	ID          int64
	SampleID    string
	Version     int64
	Kind        string
	PayloadHash string
	CreatedAt   string
}

func (s *Store) Recover(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `
SELECT intent_id, sample_id, version, kind, payload_hash, created_at
FROM intents WHERE status='RUNNING' ORDER BY intent_id`)
	if err != nil {
		return err
	}
	var intents []runningIntent
	for rows.Next() {
		var intent runningIntent
		if err := rows.Scan(&intent.ID, &intent.SampleID, &intent.Version, &intent.Kind, &intent.PayloadHash, &intent.CreatedAt); err != nil {
			_ = rows.Close()
			return err
		}
		intents = append(intents, intent)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, intent := range intents {
		if err := s.recoverIntent(ctx, intent); err != nil {
			return err
		}
	}

	rows, err = s.db.QueryContext(ctx, `SELECT sample_id FROM samples WHERE status='RUNNING' ORDER BY sample_id`)
	if err != nil {
		return err
	}
	var runningSamples []string
	for rows.Next() {
		var sampleID string
		if err := rows.Scan(&sampleID); err != nil {
			_ = rows.Close()
			return err
		}
		runningSamples = append(runningSamples, sampleID)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, sampleID := range runningSamples {
		snapshot, _, loadErr := s.LoadSnapshot(ctx, sampleID, false)
		meta, metaExists, metaErr := s.GetSampleMeta(ctx, sampleID)
		if metaErr != nil {
			return metaErr
		}
		detail := map[string]any{
			"sample_id":                sampleID,
			"basis":                    "SQLite committed rows only; volatile in-memory state is never trusted",
			"exit_report_guaranteed":   false,
			"recovered_without_intent": true,
		}
		lastCompleteVersion := int64(0)
		checkpointValid := false
		if loadErr == nil && snapshot != nil {
			lastCompleteVersion = snapshot.Version
			actualDigest := model.SnapshotDigest(*snapshot)
			detail["last_complete_version"] = snapshot.Version
			detail["snapshot_digest"] = actualDigest
			detail["recomputable_snapshot"] = snapshot
			if metaExists {
				checkpointValid = meta.StateDigest == actualDigest
			}
		}
		detail["checkpoint_digest_valid"] = checkpointValid
		outcome := "ABORTED_RUNNING_SAMPLE"
		if metaExists && !checkpointValid {
			outcome = model.CodeCheckpointCorrupt
			detail["error_code"] = model.CodeCheckpointCorrupt
			detail["expected_digest"] = meta.StateDigest
		}
		if err := s.MarkSampleAborted(ctx, sampleID, model.CodeSampleAborted, "sample had no active process at restart"); err != nil {
			return err
		}
		if err := s.insertRecoveryReport(ctx, 0, sampleID, lastCompleteVersion,
			"ABORTED", outcome, detail); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) recoverIntent(ctx context.Context, intent runningIntent) error {
	detail := map[string]any{
		"sample_id":              intent.SampleID,
		"intent_id":              intent.ID,
		"intent_version":         intent.Version,
		"intent_kind":            intent.Kind,
		"intent_payload_hash":    intent.PayloadHash,
		"intent_created_at":      intent.CreatedAt,
		"basis":                  "SQLite committed rows only; intent and commit tables are compared",
		"exit_report_guaranteed": false,
	}
	var commitDigest string
	err := s.db.QueryRowContext(ctx, `SELECT digest FROM commits WHERE sample_id=? AND version=?`, intent.SampleID, intent.Version).Scan(&commitDigest)
	commitPresent := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	detail["commit_present"] = commitPresent
	detail["commit_digest"] = commitDigest
	if intent.Kind == "FINALIZE" {
		sampleMeta, exists, metaErr := s.GetSampleMeta(ctx, intent.SampleID)
		if metaErr != nil {
			return metaErr
		}
		commitPresent = exists && sampleMeta.Status == "DONE"
		detail["commit_present"] = commitPresent
		detail["finalize_commit_rule"] = "FINALIZE is committed only when samples.status=DONE"
	}

	snapshot, _, loadErr := s.LoadSnapshot(ctx, intent.SampleID, false)
	digestValid := false
	lastCompleteVersion := int64(0)
	if loadErr != nil {
		var biz *model.BizError
		if errors.As(loadErr, &biz) && biz.Code == model.CodeNotFound {
			detail["sample_present"] = false
		} else {
			return loadErr
		}
	}
	if snapshot != nil {
		detail["sample_present"] = true
		lastCompleteVersion = snapshot.Version
		detail["last_complete_version"] = snapshot.Version
		actualDigest := model.SnapshotDigest(*snapshot)
		detail["snapshot_digest"] = actualDigest
		meta, exists, metaErr := s.GetSampleMeta(ctx, intent.SampleID)
		if metaErr != nil {
			return metaErr
		}
		if exists {
			digestValid = meta.StateDigest == actualDigest
		}
		detail["recomputable_snapshot"] = snapshot
	}
	detail["checkpoint_digest_valid"] = digestValid
	outcome := "ABORTED_AT_LAST_COMPLETE_BOUNDARY"
	intentStatus := "ABORTED"
	if commitPresent {
		intentStatus = "COMMITTED"
		outcome = "COMMITTED_INTENT_REPAIRED"
	}
	if !digestValid && snapshot != nil {
		outcome = model.CodeCheckpointCorrupt
		detail["error_code"] = model.CodeCheckpointCorrupt
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := s.db.ExecContext(ctx, `UPDATE intents SET status=?, updated_at=? WHERE intent_id=?`, intentStatus, now, intent.ID); err != nil {
		return err
	}
	if err := s.MarkSampleAborted(ctx, intent.SampleID, model.CodeSampleAborted,
		"previous process stopped during sample advancement; recovered to last complete commit"); err != nil {
		return err
	}
	return s.insertRecoveryReport(ctx, intent.ID, intent.SampleID, lastCompleteVersion, intentStatus, outcome, detail)
}

func (s *Store) insertRecoveryReport(ctx context.Context, intentID int64, sampleID string, version int64, intentStatus, outcome string, detail map[string]any) error {
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.db.ExecContext(ctx, `
INSERT INTO recovery_reports(intent_id, sample_id, version, intent_status, outcome, detail_json, created_at)
VALUES(?,?,?,?,?,?,?)`, intentID, sampleID, version, intentStatus, outcome, string(detailJSON), now)
	return err
}

func (s *Store) ListRecoveryReports(ctx context.Context, sampleID string, limit int) ([]model.RecoveryReport, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query := `
SELECT report_id, intent_id, sample_id, version, intent_status, outcome, detail_json, created_at
FROM recovery_reports`
	args := make([]any, 0, 2)
	if sampleID != "" {
		query += ` WHERE sample_id=?`
		args = append(args, sampleID)
	}
	query += ` ORDER BY report_id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var reports []model.RecoveryReport
	for rows.Next() {
		var report model.RecoveryReport
		var ignoredIntentID int64
		var detailJSON string
		if err := rows.Scan(&report.ID, &ignoredIntentID, &report.SampleID, &report.Version,
			&report.IntentStatus, &report.Outcome, &detailJSON, &report.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(detailJSON), &report.Detail); err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}
	return reports, rows.Err()
}

func (s *Store) VerifyCheckpoint(ctx context.Context, sampleID string) *model.BizError {
	snapshot, meta, err := s.LoadSnapshot(ctx, sampleID, true)
	if err == nil {
		committed, commitErr := s.LoadSnapshotAtVersion(ctx, sampleID, meta.Version)
		if commitErr != nil {
			var biz *model.BizError
			if errors.As(commitErr, &biz) && biz.Code == model.CodeCheckpointCorrupt {
				return biz
			}
			return model.Biz(model.CodeCheckpointCorrupt, "latest checkpoint commit is missing or unreadable", map[string]any{
				"sample_id": sampleID, "version": meta.Version, "detail": commitErr.Error(),
			})
		}
		if model.SnapshotDigest(*committed) != model.SnapshotDigest(*snapshot) {
			return model.Biz(model.CodeCheckpointCorrupt, "current tables do not match latest committed checkpoint", map[string]any{
				"sample_id":         sampleID,
				"version":           meta.Version,
				"table_digest":      model.SnapshotDigest(*snapshot),
				"checkpoint_digest": model.SnapshotDigest(*committed),
			})
		}
		return nil
	}
	var biz *model.BizError
	if errors.As(err, &biz) {
		return biz
	}
	return model.Biz(model.CodeCheckpointCorrupt, "snapshot verification failed", map[string]any{
		"sample_id": sampleID,
		"detail":    err.Error(),
	})
}
