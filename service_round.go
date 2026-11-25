package feedbackstream

import (
	"context"
	"encoding/json"
	"path/filepath"
)

type RoundReport struct {
	Plan    *RoundPlan     `json:"plan"`
	Results []*BatchResult `json:"results"`
}

func (s *Service) RunNextRound(ctx context.Context, roundID string) (*RoundReport, error) {
	s.roundMu.Lock()
	defer s.roundMu.Unlock()
	s.policyMu.RLock()
	defer s.policyMu.RUnlock()

	record, err := s.store.ActivePolicy(ctx)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, NewError(CodeNotFound, "no active policy")
	}
	policy := record.Snapshot
	plan, err := PlanFusion(policy)
	if err != nil {
		return nil, err
	}
	batches, err := s.store.ListBatchesByStatus(ctx, StatusQueued)
	if err != nil {
		return nil, err
	}
	candidates := make([]Candidate, 0, len(batches))
	byID := map[string]*Batch{}
	for _, batch := range batches {
		candidates = append(candidates, CandidateFromBatch(batch, policy))
		byID[batch.ID] = batch
	}
	activeCounts, err := s.store.ActivePartitionCounts(ctx)
	if err != nil {
		return nil, err
	}
	roundPlan := PlanRound(roundID, candidates, policy, activeCounts, 0)
	if err := s.store.SaveRoundPlan(ctx, roundPlan, policy.Version); err != nil {
		return nil, err
	}
	report := &RoundReport{Plan: roundPlan, Results: []*BatchResult{}}
	for _, admission := range roundPlan.Admissions {
		if !admission.Admitted {
			continue
		}
		batch := byID[admission.BatchID]
		if batch == nil {
			var gErr error
			batch, gErr = s.store.GetBatch(ctx, admission.BatchID)
			if gErr != nil {
				return report, gErr
			}
		}
		if batch == nil {
			continue
		}
		unlock := s.partitions.Lock(batch.PartitionID)
		if _, err := s.beginBatchIntent(ctx, batch.ID); err != nil {
			unlock()
			return report, err
		}
		result, runErr := s.advance(ctx, batch.ID, policy, plan)
		unlock()
		s.finishBatchIntent(batch.ID, runErr)
		if runErr != nil {
			_ = s.store.UpdateBatchStatus(ctx, batch.ID, StatusFailed, "", 0)
			return report, runErr
		}
		report.Results = append(report.Results, result)
		if request, _ := s.store.GetBatchRequest(ctx, batch.ID); request != nil {
			data, _ := json.Marshal(result)
			_ = s.store.FinishBatchRequest(ctx, BatchRequestRecord{
				BatchID: batch.ID, Status: result.Status, ResultJSON: string(data),
			})
		}
	}
	return report, nil
}

func (s *Service) EnqueueSampleData(ctx context.Context, dataDir string) ([]*Batch, error) {
	policy, _, err := s.ActivePlan(ctx)
	if err != nil {
		return nil, err
	}
	events, err := ReadEventFile(filepath.Join(dataDir, "events.jsonl"))
	if err != nil {
		return nil, err
	}
	batches, err := BuildWindowBatches(events, policy.Window)
	if err != nil {
		return nil, err
	}
	for _, batch := range batches {
		if err := s.store.SaveBatch(ctx, batch); err != nil {
			return nil, err
		}
	}
	return batches, nil
}

func (s *Service) RequeueShed(ctx context.Context) (int64, error) {
	return s.store.RequeueShedBatches(ctx)
}
