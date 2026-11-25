package edgeconfig

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
)

func (s *Service) SubmitReport(ctx context.Context, report Report) (*ReportDecision, error) {
	if report.NodeID == "" {
		return nil, NewAPIError(http.StatusBadRequest, CodeValidation, "node_id is required", nil)
	}
	return runWrite(s, ctx, "report", report.ReportID, report, func(job *writeJob[ReportDecision]) (*ReportDecision, *APIError) {
		current, apiErr := currentVersionAPI(job.ctx, s.store)
		if apiErr != nil {
			return nil, apiErr
		}
		topology, err := s.LoadTopology(job.ctx)
		if err != nil {
			return nil, internalError(err)
		}
		policy := withDefaultPolicy(topology.Policy)

		var expectedVersion int64
		var expectedDigest, region string
		err = s.db.QueryRowContext(job.ctx, `
			SELECT expected_version, expected_digest, region
			FROM node_convergence WHERE node_id=?`, report.NodeID).Scan(&expectedVersion, &expectedDigest, &region)
		if err == sql.ErrNoRows {
			return nil, NewAPIError(http.StatusNotFound, CodeNotFound, "unknown node: "+report.NodeID, nil)
		}
		if err != nil {
			return nil, internalError(err)
		}

		decision := &ReportDecision{
			NodeID:             report.NodeID,
			ExpectedVersion:    expectedVersion,
			CurrentViewVersion: current,
			ExpectedDigest:     expectedDigest,
			AppliedDigest:      report.AppliedDigest,
			EffectiveAtMS:      report.EffectiveAtMS,
		}

		if report.ObservedVersion < expectedVersion {
			decision.Decision = StatusStale
			decision.Code = CodeStaleRead
			decision.Converged = false
			decision.UsedVersion = expectedVersion
			apiErr := NewAPIError(http.StatusConflict, CodeStaleRead, "reported observation version is older than expected digest version", map[string]any{
				"node_id":              report.NodeID,
				"expected_version":     expectedVersion,
				"current_view_version": current,
				"observed_version":     report.ObservedVersion,
				"report_id":            job.requestID,
			})
			if persistErr := s.persistReportFact(job.ctx, job.intentID, job.requestID, report, decision, topology.Nodes, policy, current); persistErr != nil {
				return nil, internalError(persistErr)
			}
			return nil, apiErr
		}

		decision.UsedVersion = expectedVersion
		if report.AppliedDigest == expectedDigest && expectedDigest != "" {
			decision.Decision = StatusConverged
			decision.Converged = true
		} else {
			decision.Decision = StatusDiverged
			decision.Converged = false
			decision.Code = CodeValidation // replaced below with digest mismatch code constant
			decision.Code = "E_DIGEST_MISMATCH"
		}

		apiErr = job.Complete(func(tx *sql.Tx) error {
			if err := insertReportTx(job.ctx, tx, job.requestID, report, decision, current, s.NowMS()); err != nil {
				return err
			}
			if _, err := tx.ExecContext(job.ctx, `
				UPDATE node_convergence
				SET observed_version=?, applied_digest=?, effective_at_ms=?, last_report_id=?, updated_at_ms=?
				WHERE node_id=?`,
				report.ObservedVersion, report.AppliedDigest, report.EffectiveAtMS, job.requestID, s.NowMS(), report.NodeID); err != nil {
				return err
			}
			consistency, err := refreshAndRecomputeTx(job.ctx, tx, current, "", topology.Nodes, policy)
			if err != nil {
				return err
			}
			_ = consistency
			return nil
		}, decision, current)
		if apiErr != nil {
			return nil, apiErr
		}
		return decision, nil
	})
}

func (s *Service) persistReportFact(ctx context.Context, intentID, reportID string, report Report, decision *ReportDecision, nodes []Node, policy Policy, current int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := insertReportTx(ctx, tx, reportID, report, decision, current, s.NowMS()); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE node_convergence
		SET observed_version=?, applied_digest=?, effective_at_ms=?, last_report_id=?, updated_at_ms=?
		WHERE node_id=?`,
		report.ObservedVersion, report.AppliedDigest, report.EffectiveAtMS, reportID, s.NowMS(), report.NodeID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := refreshAndRecomputeTx(ctx, tx, current, "", nodes, policy); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	_ = intentID
	return nil
}

func insertReportTx(ctx context.Context, tx *sql.Tx, reportID string, report Report, decision *ReportDecision, current int64, receivedAt int64) error {
	responseJSON, err := json.Marshal(decision)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO node_reports(
			report_id, node_id, observed_version, applied_digest, effective_at_ms,
			received_at_ms, decision, code, expected_version, current_view_version, response_json
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		reportID, report.NodeID, report.ObservedVersion, report.AppliedDigest, report.EffectiveAtMS,
		receivedAt, decision.Decision, decision.Code, decision.ExpectedVersion, current, string(responseJSON))
	return err
}
