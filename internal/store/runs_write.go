package store

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"localinfer/workbench/internal/domain"
)

func (s *Store) CreateRun(runID, optionsHash string, trace *domain.Trace, catalogHash string, plan *domain.Plan) error {
	if plan == nil {
		return domain.NewBizError(domain.CodeInternal, "plan is required", nil)
	}
	created := now()
	reasonJSON, _ := json.Marshal(plan.DeferredReasonCounts)
	return s.withIntent("CREATE_RUN", trace.TraceID, runID, "", map[string]any{
		"trace_id": trace.TraceID, "version": trace.Version, "mode": plan.Mode, "plan_hash": plan.PlanHash,
	}, func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO runs(
			run_id,trace_id,version,content_hash,catalog_hash,mode,mode_label,status,options_json,options_hash,plan_hash,result_hash,
			node_vram_bytes,available_vram_bytes,used_vram_bytes,batch_count,deferred_count,deferred_reason_json,selected_requests,
			total_input_samples,total_planned_samples,sample_conservation_ok,precision_ok,latency_ok,failure_code,created_at,completed_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			runID, trace.TraceID, trace.Version, trace.ContentHash, catalogHash, plan.Mode, plan.ModeLabel, "organizing",
			plan.Options.OptionsJSON(), optionsHash, plan.PlanHash, "", plan.Options.NodeVRAMBytes, plan.Options.AvailableVRAM(),
			0, 0, len(plan.Deferred), string(reasonJSON), plan.SelectedRequests,
			plan.TotalInputSamples, plan.TotalPlannedSamples, boolToInt(plan.SampleConservationOK), boolToInt(plan.PrecisionOK),
			boolToInt(plan.LatencyOK), "", created, "")
		if err != nil {
			return err
		}
		for i, d := range plan.Deferred {
			if _, err := tx.Exec(`INSERT INTO deferred_requests(run_id,ord,request_id,model_id,reason,detail) VALUES(?,?,?,?,?,?)`,
				runID, i, d.RequestID, d.ModelID, d.Reason, d.Detail); err != nil {
				return err
			}
		}
		for i, m := range plan.MixRejections {
			modelsJSON, _ := json.Marshal(m.ExistingModels)
			if _, err := tx.Exec(`INSERT INTO mix_rejections(run_id,ord,request_id,existing_models_json,candidate_model,reason,detail) VALUES(?,?,?,?,?,?,?)`,
				runID, i, m.RequestID, string(modelsJSON), m.CandidateModel, m.Reason, m.Detail); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) InsertBatchBase(runID string, batch *domain.BatchPlan) error {
	if batch == nil {
		return domain.NewBizError(domain.CodeInternal, "batch is required", nil)
	}
	modelsJSON, _ := json.Marshal(batch.Models)
	return s.withIntent("COMMIT_BATCH_BASE", "", runID, batch.BatchID, map[string]any{
		"batch_sequence": batch.Sequence, "total_samples": batch.TotalSamples, "vram_bytes": batch.VRAMBytes,
	}, func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO batches(
			batch_id,run_id,batch_seq,root_seq,split_index,split_reason,split_detail,status,mode,priority,models_json,total_samples,
			vram_bytes,latency_us,compensation_us,latency_limit_us,latency_ok,sealed,late_sealed,seal_reason)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(batch_id) DO UPDATE SET status=excluded.status,models_json=excluded.models_json,total_samples=excluded.total_samples,
			vram_bytes=excluded.vram_bytes,latency_us=excluded.latency_us,compensation_us=excluded.compensation_us,
			latency_limit_us=excluded.latency_limit_us,latency_ok=excluded.latency_ok,seal_reason=excluded.seal_reason`,
			batch.BatchID, runID, batch.Sequence, batch.RootSequence, batch.SplitIndex, batch.SplitReason, batch.SplitDetail,
			"quantile_pending", batch.Mode, batch.Priority, string(modelsJSON), batch.TotalSamples, batch.VRAMBytes,
			batch.LatencyUS, batch.CompensationUS, batch.LatencyLimitUS, boolToInt(batch.LatencyOK), boolToInt(batch.Sealed),
			boolToInt(batch.LateSealed), batch.SealReason)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM batch_members WHERE batch_id=?`, batch.BatchID); err != nil {
			return err
		}
		for i, m := range batch.Members {
			if _, err := tx.Exec(`INSERT INTO batch_members(batch_id,member_ord,request_id,seq,model_id,allocated_samples,original_samples,allocation_reason)
				VALUES(?,?,?,?,?,?,?,?)`, batch.BatchID, i, m.RequestID, m.Sequence, m.ModelID, m.AllocatedSamples, m.OriginalSamples, m.AllocationReason); err != nil {
				return err
			}
		}
		_, err = tx.Exec(`UPDATE runs SET
			used_vram_bytes=(SELECT COALESCE(SUM(vram_bytes),0) FROM batches WHERE run_id=?),
			batch_count=(SELECT COUNT(*) FROM batches WHERE run_id=?) WHERE run_id=?`, runID, runID, runID)
		return err
	})
}

func (s *Store) InsertBatchResults(runID string, batch *domain.BatchPlan) error {
	if batch == nil {
		return domain.NewBizError(domain.CodeInternal, "batch is required", nil)
	}
	return s.withIntent("COMMIT_BATCH_RESULTS", "", runID, batch.BatchID, map[string]any{
		"status": batch.Status, "quantiles": len(batch.Quantiles), "precision_ok": batch.Precision.PrecisionOK,
	}, func(tx *sql.Tx) error {
		for _, q := range batch.Quantiles {
			if _, err := tx.Exec(`INSERT INTO quantile_results(batch_id,quantile,method,available,latency_us,latency_ms,rank,sample_count,min_samples,error_code,reason)
				VALUES(?,?,?,?,?,?,?,?,?,?,?)
				ON CONFLICT(batch_id,quantile) DO UPDATE SET method=excluded.method,available=excluded.available,latency_us=excluded.latency_us,
				latency_ms=excluded.latency_ms,rank=excluded.rank,sample_count=excluded.sample_count,min_samples=excluded.min_samples,
				error_code=excluded.error_code,reason=excluded.reason`,
				batch.BatchID, q.Quantile, q.Method, boolToInt(q.Available), q.LatencyUS, q.LatencyMS, q.Rank, q.SampleCount, q.MinSamples, q.ErrorCode, q.Reason); err != nil {
				return err
			}
		}
		p := batch.Precision
		if _, err := tx.Exec(`INSERT INTO precision_results(batch_id,truth_us,reported_us,reported_ms,rounding_place,internal_fixed_us,
			compensation_us,rounding_error_us,cumulative_rounding_error_us,abs_deviation_us,relative_ppm,max_abs_us,max_rel_ppm,precision_ok,explanation)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(batch_id) DO UPDATE SET truth_us=excluded.truth_us,reported_us=excluded.reported_us,reported_ms=excluded.reported_ms,
			rounding_place=excluded.rounding_place,internal_fixed_us=excluded.internal_fixed_us,compensation_us=excluded.compensation_us,
			rounding_error_us=excluded.rounding_error_us,cumulative_rounding_error_us=excluded.cumulative_rounding_error_us,
			abs_deviation_us=excluded.abs_deviation_us,relative_ppm=excluded.relative_ppm,max_abs_us=excluded.max_abs_us,
			max_rel_ppm=excluded.max_rel_ppm,precision_ok=excluded.precision_ok,explanation=excluded.explanation`,
			batch.BatchID, p.TruthUS, p.ReportedUS, p.ReportedMS, p.RoundingPlace, p.InternalFixedUS, p.CompensationUS,
			p.RoundingErrorUS, p.CumulativeRoundingErrUS, p.AbsoluteDeviationUS, p.RelativeDeviationPPM, p.MaxAbsDeviationUS,
			p.MaxRelativeDeviationPPM, boolToInt(p.PrecisionOK), p.Explanation); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE batches SET status=? WHERE batch_id=?`, batch.Status, batch.BatchID)
		return err
	})
}

func (s *Store) CompleteRun(runID, status, failureCode string, plan *domain.Plan) error {
	if plan == nil {
		return domain.NewBizError(domain.CodeInternal, "plan is required", nil)
	}
	reasonJSON, _ := json.Marshal(plan.DeferredReasonCounts)
	return s.withIntent("COMPLETE_RUN", "", runID, "", map[string]any{"status": status, "plan_hash": plan.PlanHash}, func(tx *sql.Tx) error {
		res, err := tx.Exec(`UPDATE runs SET status=?,result_hash=?,used_vram_bytes=?,batch_count=?,deferred_count=?,deferred_reason_json=?,
			selected_requests=?,total_input_samples=?,total_planned_samples=?,sample_conservation_ok=?,precision_ok=?,latency_ok=?,
			failure_code=?,completed_at=? WHERE run_id=?`,
			status, plan.PlanHash, plan.TotalVRAMBytes, len(plan.Batches), len(plan.Deferred), string(reasonJSON),
			plan.SelectedRequests, plan.TotalInputSamples, plan.TotalPlannedSamples, boolToInt(plan.SampleConservationOK),
			boolToInt(plan.PrecisionOK), boolToInt(plan.LatencyOK), failureCode, now(), runID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("run %s not found", runID)
		}
		return nil
	})
}
