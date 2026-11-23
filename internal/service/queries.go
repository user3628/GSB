package service

import (
	"fmt"
	"sort"
	"strings"

	"localinfer/workbench/internal/domain"
	"localinfer/workbench/internal/store"
)

func (s *Service) GetRun(runID string) (*domain.RunResult, error) { return s.store.GetRun(runID) }

func (s *Service) GetBatch(batchID string) (*domain.BatchEnvelope, error) {
	return s.store.GetBatch(batchID)
}

func (s *Service) ListRuns(traceID, version string) ([]store.RunMeta, error) {
	return s.store.ListRuns(traceID, version)
}

func (s *Service) Quantiles(batchID string, p int) ([]domain.QuantileResult, error) {
	batch, err := s.store.GetBatch(batchID)
	if err != nil {
		return nil, err
	}
	if p == 0 {
		return batch.Batch.Quantiles, nil
	}
	switch p {
	case 50, 95, 99:
	default:
		return nil, domain.NewBizError(domain.CodeBadRequest, "supported quantiles are 50, 95 and 99", map[string]any{"requested_quantile": p})
	}
	for _, q := range batch.Batch.Quantiles {
		if q.Quantile != p {
			continue
		}
		if !q.Available {
			return nil, domain.NewBizError(domain.CodeInsufficientSample, "sample size is insufficient for the requested quantile", map[string]any{
				"batch_id": batchID, "quantile": p, "sample_count": q.SampleCount, "min_samples": q.MinSamples,
				"method": q.Method, "reason": q.Reason,
			})
		}
		return []domain.QuantileResult{q}, nil
	}
	return nil, domain.NewBizError(domain.CodeNotFound, "quantile fact is not committed for this batch", map[string]any{"batch_id": batchID, "quantile": p})
}

type Verification struct {
	BatchID             string   `json:"batch_id"`
	RunID               string   `json:"run_id"`
	Conclusion          string   `json:"conclusion"`
	PrecisionOK         bool     `json:"precision_ok"`
	LatencyOK           bool     `json:"latency_ok"`
	TruthUS             int64    `json:"truth_latency_us"`
	ReportedUS          int64    `json:"reported_latency_us"`
	AbsoluteDeviation   int64    `json:"absolute_deviation_us"`
	RelativePPM         int64    `json:"relative_deviation_ppm"`
	QuantileAvailable   []int    `json:"quantiles_available"`
	QuantileUnavailable []int    `json:"quantiles_unavailable"`
	Notes               []string `json:"notes"`
}

func (s *Service) VerifyBatch(batchID string) (*Verification, error) {
	batch, err := s.store.GetBatch(batchID)
	if err != nil {
		return nil, err
	}
	v := &Verification{
		BatchID: batchID, RunID: batch.Batch.RunID, PrecisionOK: batch.Batch.Precision.PrecisionOK,
		LatencyOK: batch.Batch.LatencyOK, TruthUS: batch.Batch.Precision.TruthUS, ReportedUS: batch.Batch.Precision.ReportedUS,
		AbsoluteDeviation: batch.Batch.Precision.AbsoluteDeviationUS, RelativePPM: batch.Batch.Precision.RelativeDeviationPPM,
		QuantileAvailable: []int{}, QuantileUnavailable: []int{}, Notes: []string{},
	}
	for _, q := range batch.Batch.Quantiles {
		if q.Available {
			v.QuantileAvailable = append(v.QuantileAvailable, q.Quantile)
		} else {
			v.QuantileUnavailable = append(v.QuantileUnavailable, q.Quantile)
			v.Notes = append(v.Notes, fmt.Sprintf("P%d unavailable: %s", q.Quantile, q.Reason))
		}
	}
	if v.PrecisionOK {
		v.Conclusion = "TRUTH_CHECK_PASS"
	} else {
		v.Conclusion = "TRUTH_CHECK_FAIL_PRECISION_BREACH"
	}
	if !v.LatencyOK {
		v.Conclusion += "_AND_LATENCY_LIMIT_EXCEEDED"
	}
	return v, nil
}

func (s *Service) LatestRecoveryReport() (*domain.RecoveryReport, error) {
	return s.store.LatestRecoveryReport()
}

func (s *Service) RecoveryReports() ([]domain.RecoveryReport, error) {
	return s.store.ListRecoveryReports()
}

func (s *Service) CompareRuns(leftID, rightID string) (*domain.Comparison, error) {
	left, err := s.store.GetRun(leftID)
	if err != nil {
		return nil, err
	}
	right, err := s.store.GetRun(rightID)
	if err != nil {
		return nil, err
	}
	if left.TraceID != right.TraceID || left.Version != right.Version {
		return nil, domain.NewBizError(domain.CodeBadRequest, "comparison requires runs from the same trace/version", map[string]any{
			"left":  map[string]string{"trace_id": left.TraceID, "version": left.Version},
			"right": map[string]string{"trace_id": right.TraceID, "version": right.Version},
		})
	}
	count := len(left.Batches)
	if len(right.Batches) > count {
		count = len(right.Batches)
	}
	pairs := make([]domain.ComparisonPair, 0, count)
	same := 0
	for i := 0; i < count; i++ {
		pair := domain.ComparisonPair{Index: i}
		if i < len(left.Batches) {
			b := left.Batches[i]
			pair.LeftBatchID = b.BatchID
			pair.LeftSamples = b.TotalSamples
			pair.LeftVRAMBytes = b.VRAMBytes
			pair.LeftLatencyUS = b.LatencyUS
			pair.LeftModels = b.Models
			pair.LeftStatus = b.Status
		}
		if i < len(right.Batches) {
			b := right.Batches[i]
			pair.RightBatchID = b.BatchID
			pair.RightSamples = b.TotalSamples
			pair.RightVRAMBytes = b.VRAMBytes
			pair.RightLatencyUS = b.LatencyUS
			pair.RightModels = b.Models
			pair.RightStatus = b.Status
		}
		pair.SameComposition = pair.LeftSamples == pair.RightSamples && pair.LeftVRAMBytes == pair.RightVRAMBytes &&
			strings.Join(pair.LeftModels, ",") == strings.Join(pair.RightModels, ",")
		if pair.SameComposition {
			same++
		}
		pairs = append(pairs, pair)
	}
	comparison := &domain.Comparison{
		TraceID: left.TraceID, Version: left.Version, LeftRunID: left.RunID, RightRunID: right.RunID,
		LeftMode: left.Mode, RightMode: right.Mode, LeftHash: left.ResultHash, RightHash: right.ResultHash, Pairs: pairs,
	}
	comparison.Summary = map[string]any{
		"left_batch_count": len(left.Batches), "right_batch_count": len(right.Batches),
		"pairs": len(pairs), "same_composition_pairs": same,
		"left_deferred": left.DeferredCount, "right_deferred": right.DeferredCount,
		"left_planned_samples": left.TotalPlannedSamples, "right_planned_samples": right.TotalPlannedSamples,
		"left_selected_requests": left.SelectedRequests, "right_selected_requests": right.SelectedRequests,
	}
	sort.SliceStable(pairs, func(i, j int) bool { return pairs[i].Index < pairs[j].Index })
	return comparison, nil
}

func (s *Service) CompareTraceModes(traceID, version string) (*domain.Comparison, error) {
	fast, err := s.store.LatestRun(traceID, version, domain.ModeFast)
	if err != nil {
		return nil, err
	}
	full, err := s.store.LatestRun(traceID, version, domain.ModeFull)
	if err != nil {
		return nil, err
	}
	return s.CompareRuns(fast.RunID, full.RunID)
}
