package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"

	"tm-sync/internal/tm"
)

type SyncRequest struct {
	Mode      string `json:"mode"`
	SegmentID string `json:"segment_id,omitempty"`
	Verify    bool   `json:"verify"`
}

type SyncReport struct {
	RunID                   int64                   `json:"run_id"`
	Mode                    string                  `json:"mode"`
	Version                 int64                   `json:"version"`
	ProcessedSegments       []string                `json:"processed_segments"`
	SkippedSegments         []string                `json:"skipped_segments"`
	FailedSegments          []tm.SegmentErrorReport `json:"failed_segments"`
	ReconciledUnits         []string                `json:"reconciled_units"`
	PendingUnits            []string                `json:"pending_units"`
	Verified                bool                    `json:"verified"`
	ExportDigest            string                  `json:"export_digest,omitempty"`
	CheckpointSeq           int64                   `json:"checkpoint_seq,omitempty"`
	RecoveryInterruptedRuns int64                   `json:"recovery_interrupted_runs,omitempty"`
}

func (s *Service) Sync(ctx context.Context, req SyncRequest) (*SyncReport, error) {
	if req.Mode == "" {
		req.Mode = "incremental"
	}
	version, err := s.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}
	if version == 0 {
		return nil, &tm.BizError{Code: "E_NO_SOURCE_IMPORTED", Message: "import a segmented source before sync", Stage: "sync", HTTPStatus: 409}
	}
	runID, err := s.startRun(req.Mode, version, req)
	if err != nil {
		return nil, err
	}
	report := &SyncReport{RunID: runID, Mode: req.Mode, Version: version}

	rows, err := s.listSegmentRows()
	if err != nil {
		return nil, s.failRun(runID, err)
	}
	touchedUnits := map[string]bool{}

	for _, row := range rows {
		if req.SegmentID != "" && row.Record.SegmentID != req.SegmentID {
			continue
		}
		complete, err := s.segmentCheckpointComplete(row)
		if err != nil {
			return nil, s.failRun(runID, err)
		}
		if req.Mode == "full" {
			complete = false
		}
		if complete {
			report.SkippedSegments = append(report.SkippedSegments, row.Record.SegmentID)
			continue
		}
		unitIDs, bizErr := s.rebuildSegmentStages(row, version, req.Mode == "full")
		if bizErr != nil {
			report.FailedSegments = append(report.FailedSegments, tm.SegmentErrorReport{SegmentID: row.Record.SegmentID, Error: bizErr})
			continue
		}
		report.ProcessedSegments = append(report.ProcessedSegments, row.Record.SegmentID)
		for _, id := range unitIDs {
			touchedUnits[id] = true
		}
	}

	invalidUnits, err := s.listUnitsByStatus("invalid", "pending")
	if err != nil {
		return nil, s.failRun(runID, err)
	}
	for _, id := range invalidUnits {
		touchedUnits[id] = true
	}
	unitIDs := keys(touchedUnits)
	sort.Strings(unitIDs)
	report.PendingUnits = unitIDs

	for _, unitID := range unitIDs {
		wrote, err := s.reconcileUnit(unitID, version)
		if err != nil {
			return nil, s.failRun(runID, err)
		}
		if wrote {
			report.ReconciledUnits = append(report.ReconciledUnits, unitID)
		}
	}

	for _, segmentID := range report.ProcessedSegments {
		row, err := s.getSegmentRow(segmentID)
		if err != nil {
			return nil, s.failRun(runID, err)
		}
		if row == nil {
			continue
		}
		complete, err := s.invalidationCheckpointExists(segmentID)
		if err != nil {
			return nil, s.failRun(runID, err)
		}
		if !complete {
			if _, err := s.writeInvalidationCheckpoint(*row, version); err != nil {
				return nil, s.failRun(runID, err)
			}
		}
	}

	bundle, b, err := s.ExportBundle(ctx)
	if err != nil {
		return nil, s.failRun(runID, err)
	}
	report.ExportDigest = tm.SHA256Hex(b)
	report.Verified = true

	records, err := s.listSegmentRecords()
	if err != nil {
		return nil, s.failRun(runID, err)
	}
	oracle := tm.BuildCorpus(records, version, s.Config)
	if err := tm.CompareExports(oracle.Export(), bundle); err != nil {
		if biz, ok := tm.AsBizError(err); ok {
			_ = s.finishRun(runID, "mismatch", report)
			return nil, biz
		}
		return nil, s.failRun(runID, err)
	}
	report.Verified = true
	if err := s.finishRun(runID, "completed", report); err != nil {
		return nil, err
	}
	return report, nil
}

func (s *Service) segmentCheckpointComplete(row segmentRow) (bool, error) {
	var complete bool
	err := s.Store.WithTx(func(tx *sql.Tx) error {
		seg, err := getActiveCheckpointTx(tx, tm.StageSegmentation, row.Record.SegmentID, "")
		if err != nil {
			return err
		}
		if seg == nil || seg.ContentFP != row.ContentFP || seg.ArtifactDigest == "" {
			complete = false
			return nil
		}
		align, err := getActiveCheckpointTx(tx, tm.StageAlignment, row.Record.SegmentID, "")
		if err != nil {
			return err
		}
		merge, err := getActiveCheckpointTx(tx, tm.StageMerge, row.Record.SegmentID, "")
		if err != nil {
			return err
		}
		invalid, err := getActiveCheckpointTx(tx, tm.StageInvalidation, row.Record.SegmentID, "")
		if err != nil {
			return err
		}
		complete = align != nil && merge != nil && invalid != nil
		return nil
	})
	return complete, err
}

func (s *Service) invalidationCheckpointExists(segmentID string) (bool, error) {
	c, err := s.activeCheckpoint(tm.StageInvalidation, segmentID, "")
	return c != nil, err
}

func (s *Service) rebuildSegmentStages(row segmentRow, version int64, force bool) ([]string, *tm.BizError) {
	record := row.Record
	fact, bizErr := tm.ProcessSegment(record, s.Config)
	if bizErr != nil {
		stage := bizErr.Stage
		if stage == "" {
			stage = tm.StageSegmentation
		}
		_, _ = s.writeCheckpoint(CheckpointInput{
			Stage: stage, SegmentID: record.SegmentID, ContentFP: row.ContentFP,
			Version: version, Status: "error", ErrorCode: bizErr.Code,
			ArtifactDigest: tm.SHA256Hex([]byte(bizErr.Error())),
			Detail:         bizErr.Details,
		})
		return nil, bizErr
	}

	segCP, err := s.activeCheckpoint(tm.StageSegmentation, record.SegmentID, "")
	if err != nil {
		return nil, &tm.BizError{Code: "E_CHECKPOINT_READ", Message: err.Error(), SegmentID: record.SegmentID, Stage: tm.StageSegmentation, HTTPStatus: 500}
	}
	if force || segCP == nil || segCP.ContentFP != row.ContentFP {
		digest := stageDigest(tm.StageSegmentation, map[string]any{
			"source": fact.Source.ArtifactSum, "target": fact.Target.ArtifactSum,
		})
		if _, err := s.writeCheckpoint(CheckpointInput{
			Stage: tm.StageSegmentation, SegmentID: record.SegmentID, ContentFP: row.ContentFP,
			Version: version, ArtifactDigest: digest,
			Detail: map[string]any{"source_sentences": len(fact.Source.Sentences), "target_sentences": len(fact.Target.Sentences)},
		}); err != nil {
			return nil, storeError(record.SegmentID, tm.StageSegmentation, err)
		}
	}

	alignCP, err := s.activeCheckpoint(tm.StageAlignment, record.SegmentID, "")
	if err != nil {
		return nil, storeError(record.SegmentID, tm.StageAlignment, err)
	}
	if force || alignCP == nil || alignCP.ArtifactDigest != fact.Alignment.Digest {
		if _, err := s.writeCheckpoint(CheckpointInput{
			Stage: tm.StageAlignment, SegmentID: record.SegmentID, ContentFP: row.ContentFP,
			Version: version, ArtifactDigest: fact.Alignment.Digest,
			Detail: map[string]any{
				"pair_count":      len(fact.Alignment.Pairs),
				"candidate_count": len(fact.Alignment.Candidates),
				"alignment":       fact.Alignment,
			},
		}); err != nil {
			return nil, storeError(record.SegmentID, tm.StageAlignment, err)
		}
	}

	mergeCP, err := s.activeCheckpoint(tm.StageMerge, record.SegmentID, "")
	if err != nil {
		return nil, storeError(record.SegmentID, tm.StageMerge, err)
	}
	if force || mergeCP == nil {
		var unitIDs []string
		err := s.Store.WithTx(func(tx *sql.Tx) error {
			var err error
			unitIDs, err = replaceOccurrencesTx(tx, fact)
			if err != nil {
				return err
			}
			_, err = putCheckpointTx(tx, CheckpointInput{
				Stage: tm.StageMerge, SegmentID: record.SegmentID, ContentFP: row.ContentFP,
				Version: version, ArtifactDigest: stageDigest(tm.StageMerge, unitIDs),
				Detail: map[string]any{"unit_ids": unitIDs, "occurrence_count": len(fact.Occurrences)},
			})
			return err
		})
		if err != nil {
			return nil, storeError(record.SegmentID, tm.StageMerge, err)
		}
		s.maybeCrashAfterCheckpoint()
		return unitIDs, nil
	}

	var detail struct {
		UnitIDs []string `json:"unit_ids"`
	}
	_ = json.Unmarshal([]byte(mergeCP.DetailJSON), &detail)
	return detail.UnitIDs, nil
}

func storeError(segmentID, stage string, err error) *tm.BizError {
	if biz, ok := tm.AsBizError(err); ok {
		return biz
	}
	return &tm.BizError{Code: "E_SQLITE_TX", Message: err.Error(), SegmentID: segmentID, Stage: stage, HTTPStatus: 500}
}

func replaceOccurrencesTx(tx *sql.Tx, fact *tm.SegmentFact) ([]string, error) {
	oldRows, err := tx.Query(`SELECT DISTINCT unit_id FROM unit_occurrences WHERE segment_id=?`, fact.Record.SegmentID)
	if err != nil {
		return nil, err
	}
	idSet := map[string]bool{}
	for oldRows.Next() {
		var id string
		if err := oldRows.Scan(&id); err != nil {
			_ = oldRows.Close()
			return nil, err
		}
		idSet[id] = true
	}
	_ = oldRows.Close()

	if _, err := tx.Exec(`DELETE FROM unit_occurrences WHERE segment_id=?`, fact.Record.SegmentID); err != nil {
		return nil, err
	}
	for _, occ := range fact.Occurrences {
		unitID := tm.UnitIDFor(fact.Record.SourceLang, fact.Record.TargetLang, occ.SourceText)
		idSet[unitID] = true
		payload, _ := tm.CanonicalJSON(occ)
		_, err := tx.Exec(`INSERT INTO unit_occurrences
			(unit_id,segment_id,source_index,target_index,source_start,source_end,target_start,target_end,payload_json)
			VALUES(?,?,?,?,?,?,?,?,?)`,
			unitID, occ.SegmentID, occ.SourceIndex, occ.TargetIndex,
			occ.SourceOffset[0], occ.SourceOffset[1], occ.TargetOffset[0], occ.TargetOffset[1], string(payload))
		if err != nil {
			return nil, err
		}
	}
	return keys(idSet), nil
}

func (s *Service) listUnitsByStatus(statuses ...string) ([]string, error) {
	q := `SELECT unit_id FROM units WHERE status IN (`
	args := make([]any, len(statuses))
	for i, st := range statuses {
		if i > 0 {
			q += ","
		}
		q += "?"
		args[i] = st
	}
	q += `) ORDER BY unit_id`
	rows, err := s.Store.DB.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Service) getSegmentRow(segmentID string) (*segmentRow, error) {
	var row segmentRow
	var raw string
	err := s.Store.DB.QueryRow(`SELECT content_fp,version,raw_json FROM segments WHERE segment_id=?`, segmentID).
		Scan(&row.ContentFP, &row.Version, &raw)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, json.Unmarshal([]byte(raw), &row.Record)
}

func (s *Service) writeInvalidationCheckpoint(row segmentRow, version int64) (int64, error) {
	var digest string
	var count int
	err := s.Store.DB.QueryRow(`SELECT count(*), COALESCE(SUM(length(chain_json)),0) FROM invalidations WHERE segment_id=?`, row.Record.SegmentID).Scan(&count, &digest)
	if err != nil {
		return 0, err
	}
	artifact := stageDigest(tm.StageInvalidation, map[string]any{"rows": count, "chain_bytes": digest})
	return s.writeCheckpoint(CheckpointInput{
		Stage: tm.StageInvalidation, SegmentID: row.Record.SegmentID, ContentFP: row.ContentFP,
		Version: version, ArtifactDigest: artifact,
		Detail: map[string]any{"invalidation_rows": count},
	})
}

func (s *Service) startRun(mode string, version int64, req SyncRequest) (int64, error) {
	var id int64
	err := s.Store.WithTx(func(tx *sql.Tx) error {
		options, _ := tm.CanonicalJSON(req)
		res, err := tx.Exec(`INSERT INTO sync_runs(version,mode,status,options_json) VALUES(?,?, 'running',?)`, version, mode, string(options))
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		return err
	})
	return id, err
}

func (s *Service) finishRun(id int64, status string, report *SyncReport) error {
	return s.Store.WithTx(func(tx *sql.Tx) error {
		b, _ := tm.CanonicalJSON(report)
		_, err := tx.Exec(`UPDATE sync_runs SET status=?, report_json=?, finished_at=datetime('now') WHERE id=?`, status, string(b), id)
		return err
	})
}

func (s *Service) failRun(id int64, err error) error {
	_ = s.Store.WithTx(func(tx *sql.Tx) error {
		_, e := tx.Exec(`UPDATE sync_runs SET status='failed', report_json=?, finished_at=datetime('now') WHERE id=?`, err.Error(), id)
		return e
	})
	return err
}

func (s *Service) Recover(ctx context.Context) (*SyncReport, error) {
	var interrupted int64
	err := s.Store.WithTx(func(tx *sql.Tx) error {
		res, err := tx.Exec(`UPDATE sync_runs SET status='interrupted', finished_at=datetime('now') WHERE status='running'`)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		interrupted = n
		return nil
	})
	if err != nil {
		return nil, err
	}
	report, err := s.Sync(ctx, SyncRequest{Mode: "incremental"})
	if err != nil {
		return nil, err
	}
	report.RecoveryInterruptedRuns = interrupted
	return report, nil
}
