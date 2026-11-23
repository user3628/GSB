package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"localinfer/workbench/internal/domain"
)

type RunMeta struct {
	RunID         string `json:"run_id"`
	TraceID       string `json:"trace_id"`
	Version       string `json:"version"`
	Mode          string `json:"mode"`
	ModeLabel     string `json:"mode_label"`
	Status        string `json:"status"`
	CreatedAt     string `json:"created_at"`
	CompletedAt   string `json:"completed_at"`
	BatchCount    int    `json:"batch_count"`
	DeferredCount int    `json:"deferred_request_count"`
	PlanHash      string `json:"plan_hash"`
	ResultHash    string `json:"result_hash"`
	FailureCode   string `json:"failure_code,omitempty"`
}

const runColumns = `run_id,trace_id,version,content_hash,catalog_hash,mode,mode_label,status,options_json,options_hash,plan_hash,result_hash,
node_vram_bytes,available_vram_bytes,used_vram_bytes,batch_count,deferred_count,deferred_reason_json,selected_requests,
total_input_samples,total_planned_samples,sample_conservation_ok,precision_ok,latency_ok,failure_code,created_at,completed_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRun(scanner rowScanner) (*domain.RunResult, error) {
	var r domain.RunResult
	var optionsJSON, deferredReasonJSON string
	var conservation, precisionOK, latencyOK int64
	err := scanner.Scan(&r.RunID, &r.TraceID, &r.Version, &r.ContentHash, &r.CatalogHash, &r.Mode, &r.ModeLabel, &r.Status,
		&optionsJSON, &r.OptionsHash, &r.PlanHash, &r.ResultHash, &r.NodeVRAMBytes, &r.AvailableVRAMBytes, &r.UsedVRAMBytes,
		&r.BatchCount, &r.DeferredCount, &deferredReasonJSON, &r.SelectedRequests, &r.TotalInputSamples, &r.TotalPlannedSamples,
		&conservation, &precisionOK, &latencyOK, &r.FailureCode, &r.CreatedAt, &r.CompletedAt)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(optionsJSON), &r.Options); err != nil {
		return nil, fmt.Errorf("decode options for %s: %w", r.RunID, err)
	}
	if deferredReasonJSON != "" {
		if err := json.Unmarshal([]byte(deferredReasonJSON), &r.DeferredReasonCounts); err != nil {
			return nil, err
		}
	}
	r.SampleConservationOK = intToBool(conservation)
	r.PrecisionOK = intToBool(precisionOK)
	r.LatencyOK = intToBool(latencyOK)
	return &r, nil
}

func (s *Store) GetRun(runID string) (*domain.RunResult, error) {
	r, err := scanRun(s.db.QueryRow(`SELECT `+runColumns+` FROM runs WHERE run_id=?`, runID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.NewBizError(domain.CodeNotFound, "run not found", map[string]any{"run_id": runID})
	}
	if err != nil {
		return nil, err
	}
	batches, err := s.loadBatches(runID)
	if err != nil {
		return nil, err
	}
	r.Batches = batches
	r.BatchCount = len(batches)
	r.UsedVRAMBytes = sumCommittedVRAM(batches)
	deferred, err := s.loadDeferred(runID)
	if err != nil {
		return nil, err
	}
	r.Deferred = deferred
	r.DeferredCount = len(deferred)
	if r.DeferredReasonCounts == nil {
		r.DeferredReasonCounts = map[string]int{}
	}
	mixes, err := s.loadMixRejections(runID)
	if err != nil {
		return nil, err
	}
	r.MixRejections = mixes
	return r, nil
}

func (s *Store) loadBatches(runID string) ([]domain.BatchPlan, error) {
	rows, err := s.db.Query(`SELECT batch_id,run_id,batch_seq,root_seq,split_index,split_reason,split_detail,status,mode,priority,models_json,
		total_samples,vram_bytes,latency_us,compensation_us,latency_limit_us,latency_ok,sealed,late_sealed,seal_reason
		FROM batches WHERE run_id=? ORDER BY batch_seq`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	batches := make([]domain.BatchPlan, 0)
	for rows.Next() {
		var b domain.BatchPlan
		var modelsJSON, sealReason string
		var latencyOK, sealed, lateSealed int64
		if err := rows.Scan(&b.BatchID, &b.RunID, &b.Sequence, &b.RootSequence, &b.SplitIndex, &b.SplitReason, &b.SplitDetail,
			&b.Status, &b.Mode, &b.Priority, &modelsJSON, &b.TotalSamples, &b.VRAMBytes, &b.LatencyUS, &b.CompensationUS,
			&b.LatencyLimitUS, &latencyOK, &sealed, &lateSealed, &sealReason); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(modelsJSON), &b.Models)
		b.LatencyOK = intToBool(latencyOK)
		b.Sealed = intToBool(sealed)
		b.LateSealed = intToBool(lateSealed)
		b.SealReason = sealReason
		batches = append(batches, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range batches {
		members, err := s.loadMembers(batches[i].BatchID)
		if err != nil {
			return nil, err
		}
		batches[i].Members = members
		quantiles, err := s.loadQuantiles(batches[i].BatchID)
		if err != nil {
			return nil, err
		}
		batches[i].Quantiles = quantiles
		precision, has, err := s.loadPrecision(batches[i].BatchID)
		if err != nil {
			return nil, err
		}
		if has {
			batches[i].Precision = precision
		}
	}
	return batches, nil
}

func (s *Store) loadMembers(batchID string) ([]domain.PlannedMember, error) {
	rows, err := s.db.Query(`SELECT request_id,seq,model_id,allocated_samples,original_samples,allocation_reason
		FROM batch_members WHERE batch_id=? ORDER BY member_ord`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	members := make([]domain.PlannedMember, 0)
	for rows.Next() {
		var m domain.PlannedMember
		if err := rows.Scan(&m.RequestID, &m.Sequence, &m.ModelID, &m.AllocatedSamples, &m.OriginalSamples, &m.AllocationReason); err != nil {
			return nil, err
		}
		members = append(members, m)
	}
	return members, rows.Err()
}

func (s *Store) loadQuantiles(batchID string) ([]domain.QuantileResult, error) {
	rows, err := s.db.Query(`SELECT quantile,method,available,latency_us,latency_ms,rank,sample_count,min_samples,error_code,reason
		FROM quantile_results WHERE batch_id=? ORDER BY quantile`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.QuantileResult, 0)
	for rows.Next() {
		var q domain.QuantileResult
		var available int64
		if err := rows.Scan(&q.Quantile, &q.Method, &available, &q.LatencyUS, &q.LatencyMS, &q.Rank, &q.SampleCount, &q.MinSamples, &q.ErrorCode, &q.Reason); err != nil {
			return nil, err
		}
		q.Available = intToBool(available)
		out = append(out, q)
	}
	return out, rows.Err()
}

func (s *Store) loadPrecision(batchID string) (domain.PrecisionResult, bool, error) {
	var p domain.PrecisionResult
	var ok int64
	err := s.db.QueryRow(`SELECT truth_us,reported_us,reported_ms,rounding_place,internal_fixed_us,compensation_us,rounding_error_us,
		cumulative_rounding_error_us,abs_deviation_us,relative_ppm,max_abs_us,max_rel_ppm,precision_ok,explanation
		FROM precision_results WHERE batch_id=?`, batchID).Scan(&p.TruthUS, &p.ReportedUS, &p.ReportedMS, &p.RoundingPlace,
		&p.InternalFixedUS, &p.CompensationUS, &p.RoundingErrorUS, &p.CumulativeRoundingErrUS, &p.AbsoluteDeviationUS,
		&p.RelativeDeviationPPM, &p.MaxAbsDeviationUS, &p.MaxRelativeDeviationPPM, &ok, &p.Explanation)
	if errors.Is(err, sql.ErrNoRows) {
		return p, false, nil
	}
	if err != nil {
		return p, false, err
	}
	p.PrecisionOK = intToBool(ok)
	return p, true, nil
}

func (s *Store) loadDeferred(runID string) ([]domain.DeferredRequest, error) {
	rows, err := s.db.Query(`SELECT request_id,model_id,reason,detail FROM deferred_requests WHERE run_id=? ORDER BY ord`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.DeferredRequest, 0)
	for rows.Next() {
		var d domain.DeferredRequest
		if err := rows.Scan(&d.RequestID, &d.ModelID, &d.Reason, &d.Detail); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) loadMixRejections(runID string) ([]domain.MixRejection, error) {
	rows, err := s.db.Query(`SELECT request_id,existing_models_json,candidate_model,reason,detail FROM mix_rejections WHERE run_id=? ORDER BY ord`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.MixRejection, 0)
	for rows.Next() {
		var m domain.MixRejection
		var existing string
		if err := rows.Scan(&m.RequestID, &existing, &m.CandidateModel, &m.Reason, &m.Detail); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(existing), &m.ExistingModels)
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) GetBatch(batchID string) (*domain.BatchEnvelope, error) {
	var runID string
	err := s.db.QueryRow(`SELECT run_id FROM batches WHERE batch_id=?`, batchID).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.NewBizError(domain.CodeNotFound, "batch not found", map[string]any{"batch_id": batchID})
	}
	if err != nil {
		return nil, err
	}
	run, err := s.GetRun(runID)
	if err != nil {
		return nil, err
	}
	var found *domain.BatchPlan
	var usedThrough int64
	for i := range run.Batches {
		usedThrough += run.Batches[i].VRAMBytes
		if run.Batches[i].BatchID == batchID {
			found = &run.Batches[i]
			break
		}
	}
	if found == nil {
		return nil, domain.NewBizError(domain.CodeNotFound, "batch not found in run", map[string]any{"batch_id": batchID})
	}
	return &domain.BatchEnvelope{
		Batch: *found,
		Budget: domain.VRAMBudget{
			NodeVRAMBytes: run.NodeVRAMBytes, InjectedShortfall: run.NodeVRAMBytes - run.AvailableVRAMBytes,
			AvailableVRAMBytes: run.AvailableVRAMBytes, BatchVRAMBytes: found.VRAMBytes,
			UsedThroughBatch: usedThrough, RemainingAfter: run.AvailableVRAMBytes - usedThrough,
		},
		TraceID: run.TraceID, Version: run.Version, RunStatus: run.Status, Mode: run.Mode, ModeLabel: run.ModeLabel, NodeVRAM: run.NodeVRAMBytes,
	}, nil
}

func (s *Store) ListRuns(traceID, version string) ([]RunMeta, error) {
	query := `SELECT run_id,trace_id,version,mode,mode_label,status,created_at,completed_at,batch_count,deferred_count,plan_hash,result_hash,failure_code FROM runs`
	args := []any{}
	if traceID != "" {
		query += ` WHERE trace_id=?`
		args = append(args, traceID)
		if version != "" {
			query += ` AND version=?`
			args = append(args, version)
		}
	}
	query += ` ORDER BY created_at DESC,run_id DESC`
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]RunMeta, 0)
	for rows.Next() {
		var m RunMeta
		if err := rows.Scan(&m.RunID, &m.TraceID, &m.Version, &m.Mode, &m.ModeLabel, &m.Status, &m.CreatedAt, &m.CompletedAt,
			&m.BatchCount, &m.DeferredCount, &m.PlanHash, &m.ResultHash, &m.FailureCode); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) LatestRun(traceID, version, mode string) (*domain.RunResult, error) {
	query := `SELECT ` + runColumns + ` FROM runs WHERE trace_id=? AND version=?`
	args := []any{traceID, version}
	if mode != "" {
		query += ` AND mode=?`
		args = append(args, mode)
	}
	query += ` AND status IN ('complete','precision_failed','latency_failed') ORDER BY completed_at DESC, created_at DESC LIMIT 1`
	r, err := scanRun(s.db.QueryRow(query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.NewBizError(domain.CodeNotFound, "completed run not found", map[string]any{"trace_id": traceID, "version": version, "mode": mode})
	}
	if err != nil {
		return nil, err
	}
	batches, err := s.loadBatches(r.RunID)
	if err != nil {
		return nil, err
	}
	r.Batches = batches
	r.BatchCount = len(batches)
	r.UsedVRAMBytes = sumCommittedVRAM(batches)
	deferred, err := s.loadDeferred(r.RunID)
	if err != nil {
		return nil, err
	}
	r.Deferred = deferred
	mixes, err := s.loadMixRejections(r.RunID)
	if err != nil {
		return nil, err
	}
	r.MixRejections = mixes
	return r, nil
}

func sumCommittedVRAM(batches []domain.BatchPlan) int64 {
	var total int64
	for _, batch := range batches {
		if batch.Status == "aborted" {
			continue
		}
		total += batch.VRAMBytes
	}
	return total
}
