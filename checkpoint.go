package abeval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

func (s *Store) CommitStage(ctx context.Context, input StageCheckpoint) (*BatchStatus, error) {
	checkpoint := input
	if checkpoint.BatchID == "" {
		return nil, NewError(CodeInvalidConfig, "batch_id is required").
			WithStage("commit_stage").WithField("batch_id")
	}
	if checkpoint.StageIndex <= 0 {
		return nil, NewError(CodeInvalidConfig, "stage_index must be positive").
			WithStage("commit_stage").WithField("stage_index")
	}

	statsJSON, err := CanonicalJSON(checkpoint.Stats)
	if err != nil {
		return nil, err
	}
	verdictJSON, err := CanonicalJSON(checkpoint.Verdict)
	if err != nil {
		return nil, err
	}
	if checkpoint.StatsHash == "" {
		checkpoint.StatsHash = SHA256Hex(statsJSON)
	}

	conn, err := s.beginImmediate(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			rollbackConn(conn)
		}
	}()

	row, err := s.getBatchRow(ctx, conn, checkpoint.BatchID)
	if err != nil {
		return nil, err
	}
	if checkpoint.Version != 0 && checkpoint.Version != row.Version {
		return nil, staleCheckpointError(row, checkpoint, "version does not match active revision")
	}
	if checkpoint.ContentHash != "" && checkpoint.ContentHash != row.ContentHash {
		return nil, staleCheckpointError(row, checkpoint, "content hash does not match active revision")
	}
	expectedStage := row.CurrentStage + 1
	if checkpoint.StageIndex != expectedStage {
		return nil, staleCheckpointError(row, checkpoint, "stage is not the next contiguous checkpoint")
	}

	result, err := conn.ExecContext(ctx, `
		UPDATE batches
		SET current_stage = ?, status = ?
		WHERE batch_id = ?
		  AND current_stage = ?
		  AND version = ?
		  AND content_hash = ?`,
		checkpoint.StageIndex, StatusRunning, checkpoint.BatchID, row.CurrentStage, row.Version, row.ContentHash)
	if err != nil {
		return nil, mapSQLiteError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if affected != 1 {
		return nil, staleCheckpointError(row, checkpoint, "conditional batch checkpoint update affected no row")
	}

	checkpoint.Version = row.Version
	checkpoint.ContentHash = row.ContentHash
	_, err = conn.ExecContext(ctx, `
		INSERT INTO stage_checkpoints (
			batch_id, version, content_hash, stage_index, status,
			stats_json, verdict_json, stats_hash, writer_id
		) VALUES (?, ?, ?, ?, 'valid', ?, ?, ?, ?)`,
		checkpoint.BatchID, checkpoint.Version, checkpoint.ContentHash, checkpoint.StageIndex,
		string(statsJSON), string(verdictJSON), checkpoint.StatsHash, checkpoint.WriterID)
	if err != nil {
		return nil, mapSQLiteError(err)
	}

	if checkpoint.StageIndex == row.TotalStages {
		row.CurrentStage = checkpoint.StageIndex
		row.Status = StatusCompleted
		report, err := s.buildReport(ctx, conn, &row)
		if err != nil {
			return nil, err
		}
		finalJSON, err := CanonicalJSON(report.FinalVerdict)
		if err != nil {
			return nil, err
		}
		reportJSON, err := CanonicalJSON(report)
		if err != nil {
			return nil, err
		}
		_, err = conn.ExecContext(ctx, `
			UPDATE batches
			SET status = ?, final_decision = ?, final_decision_code = ?,
			    final_verdict_json = ?, report_json = ?
			WHERE batch_id = ?`,
			StatusCompleted, report.Decision, report.DecisionCode,
			string(finalJSON), string(reportJSON), checkpoint.BatchID)
		if err != nil {
			return nil, err
		}
	}

	if err := commitConn(conn); err != nil {
		return nil, err
	}
	committed = true
	status, err := s.GetStatus(ctx, checkpoint.BatchID)
	if err != nil {
		return nil, err
	}
	return status, nil
}

func staleCheckpointError(row batchRow, checkpoint StageCheckpoint, reason string) *Error {
	return NewError(CodeStaleCheckpoint, reason).
		WithStage("commit_stage").
		WithField("stage_index").
		WithDetail("batch_id", checkpoint.BatchID).
		WithDetail("active_version", row.Version).
		WithDetail("active_current_stage", row.CurrentStage).
		WithDetail("expected_stage_index", row.CurrentStage+1).
		WithDetail("actual_stage_index", checkpoint.StageIndex).
		WithDetail("incoming_version", checkpoint.Version).
		WithDetail("writer_id", checkpoint.WriterID)
}

func (s *Store) AdvanceStage(ctx context.Context, batchID, writerID string) (*StageCheckpoint, error) {
	status, err := s.GetStatus(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if status.Status == StatusCompleted || status.CurrentStage >= status.TotalStages {
		return nil, ErrCompleted
	}
	dataset, err := s.LoadDataset(ctx, batchID)
	if err != nil {
		return nil, err
	}

	nextStage := status.CurrentStage + 1
	metrics, err := BuildStageMetrics(dataset.Config, dataset.Events, nextStage)
	if err != nil {
		return nil, err
	}
	verdict := EvaluateStage(metrics, dataset.Config)
	statsJSON, err := CanonicalJSON(metrics)
	if err != nil {
		return nil, err
	}
	checkpoint := StageCheckpoint{
		BatchID:     batchID,
		Version:     status.Version,
		ContentHash: status.ContentHash,
		StageIndex:  nextStage,
		Stats:       metrics,
		Verdict:     verdict,
		StatsHash:   SHA256Hex(statsJSON),
		WriterID:    writerID,
	}
	if _, err := s.CommitStage(ctx, checkpoint); err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

func (s *Store) MarkGracefulStopped(ctx context.Context, batchID string) (*BatchStatus, error) {
	conn, err := s.beginImmediate(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			rollbackConn(conn)
		}
	}()

	row, err := s.getBatchRow(ctx, conn, batchID)
	if err != nil {
		return nil, err
	}
	if row.Status != StatusCompleted {
		_, err = conn.ExecContext(ctx, `
			UPDATE batches
			SET stop_requested = 1, status = ?
			WHERE batch_id = ? AND status != ?`,
			StatusGracefulStopped, batchID, StatusCompleted)
		if err != nil {
			return nil, err
		}
	}
	if err := commitConn(conn); err != nil {
		return nil, err
	}
	committed = true
	return s.GetStatus(ctx, batchID)
}

func (s *Store) buildReport(ctx context.Context, q querier, row *batchRow) (*Report, error) {
	var config Config
	if err := json.Unmarshal([]byte(row.ConfigJSON), &config); err != nil {
		return nil, fmt.Errorf("unmarshal persisted config: %w", err)
	}

	report := &Report{
		SchemaVersion: SchemaVersion,
		BatchID:       row.BatchID,
		ExperimentID:  row.ExperimentID,
		Revision: RevisionInfo{
			Version:     row.Version,
			ConfigHash:  row.ConfigHash,
			SampleHash:  row.SampleHash,
			ContentHash: row.ContentHash,
		},
		Status: row.Status,
		Stages: make([]StageReport, 0, row.TotalStages),
	}

	stageRows, err := q.QueryContext(ctx, `
		SELECT stage_index, stats_json, verdict_json
		FROM stage_checkpoints
		WHERE batch_id = ? AND status = 'valid'
		ORDER BY stage_index ASC`, row.BatchID)
	if err != nil {
		return nil, err
	}
	defer stageRows.Close()

	for stageRows.Next() {
		var stageIndex int
		var statsJSON, verdictJSON string
		if err := stageRows.Scan(&stageIndex, &statsJSON, &verdictJSON); err != nil {
			return nil, err
		}
		var metrics StageMetrics
		if err := json.Unmarshal([]byte(statsJSON), &metrics); err != nil {
			return nil, fmt.Errorf("unmarshal stage %d stats: %w", stageIndex, err)
		}
		var verdict Verdict
		if err := json.Unmarshal([]byte(verdictJSON), &verdict); err != nil {
			return nil, fmt.Errorf("unmarshal stage %d verdict: %w", stageIndex, err)
		}
		if stageIndex < 1 || stageIndex > len(config.Stages) {
			return nil, NewError(CodeConflict, "persisted stage index has no configuration").
				WithStage("build_report").WithDetail("stage_index", stageIndex)
		}
		report.Stages = append(report.Stages, StageReport{
			Stage:   config.Stages[stageIndex-1],
			Metrics: metrics,
			Verdict: verdict,
		})
	}
	if err := stageRows.Err(); err != nil {
		return nil, err
	}

	if row.Status == StatusCompleted {
		report.Decision = row.FinalDecision
		report.DecisionCode = row.FinalDecisionCode
		report.Summary = ""
		if row.FinalVerdictJSON != "" {
			var verdict Verdict
			if err := json.Unmarshal([]byte(row.FinalVerdictJSON), &verdict); err != nil {
				return nil, err
			}
			report.Summary = verdict.Summary
			verdictCopy := verdict
			report.FinalVerdict = &verdictCopy
		}
		if report.FinalVerdict == nil && len(report.Stages) > 0 {
			final := report.Stages[len(report.Stages)-1].Verdict
			report.FinalVerdict = &final
			report.Decision = final.Decision
			report.DecisionCode = final.ReasonCode
			report.Summary = final.Summary
		}
	}
	return report, nil
}

func (s *Store) BuildReport(ctx context.Context, batchID string) (*Report, error) {
	row, err := s.getBatchRow(ctx, s.db, batchID)
	if err != nil {
		return nil, err
	}
	return s.buildReport(ctx, s.db, &row)
}

func (s *Store) ExportReport(ctx context.Context, batchID string) ([]byte, error) {
	report, err := s.BuildReport(ctx, batchID)
	if err != nil {
		return nil, err
	}
	data, err := PrettyJSON(report)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func IsCompleted(err error) bool {
	return errors.Is(err, ErrCompleted)
}
