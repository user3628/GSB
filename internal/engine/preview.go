package engine

import (
	"context"

	"local/rollout-guard/internal/domain"
	"local/rollout-guard/internal/risk"
	"local/rollout-guard/internal/store"
)

type CooldownStatus struct {
	Service       string `json:"service"`
	BatchID       string `json:"batch_id,omitempty"`
	Active        bool   `json:"active"`
	TriggeredAtMS int64  `json:"triggered_at_ms,omitempty"`
	ExpiresAtMS   int64  `json:"expires_at_ms,omitempty"`
	RemainingMS   int64  `json:"remaining_ms"`
	CheckedAtMS   int64  `json:"checked_at_ms"`
}

type PreviewResult struct {
	PolicyVersion       int64                `json:"policy_version"`
	PolicyChecksum      string               `json:"policy_checksum"`
	PolicyGeneratedAtMS int64                `json:"policy_generated_at_ms"`
	AsOfCommitSeq       int64                `json:"as_of_commit_seq"`
	ObservationRevision int64                `json:"observation_revision"`
	Batch               domain.Batch         `json:"batch"`
	Policy              domain.Policy        `json:"policy"`
	Mode                string               `json:"mode"`
	Evaluation          *risk.Evaluation     `json:"evaluation,omitempty"`
	Comparison          *risk.Comparison     `json:"comparison,omitempty"`
	Buckets             []risk.BucketSummary `json:"buckets"`
	Cooldown            CooldownStatus       `json:"cooldown"`
}

func (e *Engine) Preview(ctx context.Context, batchID, mode string) (PreviewResult, error) {
	if mode != risk.ModeFast && mode != risk.ModeFull && mode != risk.ModeCompare {
		return PreviewResult{}, domain.NewBizError("E_INVALID_MODE", "mode 必须为 fast、full 或 compare", map[string]any{"mode": mode})
	}
	tx, err := e.store.Begin(ctx)
	if err != nil {
		return PreviewResult{}, err
	}
	defer tx.Rollback()
	policy, _, err := store.CurrentPolicy(ctx, tx)
	if err != nil {
		return PreviewResult{}, err
	}
	batch, err := store.GetBatch(ctx, tx, batchID)
	if err != nil {
		return PreviewResult{}, err
	}
	records, err := store.LoadObservations(ctx, tx, batchID)
	if err != nil {
		return PreviewResult{}, err
	}
	asOf, err := store.CurrentCommit(ctx, tx)
	if err != nil {
		return PreviewResult{}, err
	}
	meta := risk.SnapshotMeta{AsOfCommitSeq: asOf, ObservationRevision: batch.Revision}
	riskObservations := toRiskObservations(records)
	result := PreviewResult{
		PolicyVersion: policy.Version, PolicyChecksum: policy.Checksum, PolicyGeneratedAtMS: policy.GeneratedAtMS,
		AsOfCommitSeq: asOf, ObservationRevision: batch.Revision, Batch: batch, Policy: policy, Mode: mode,
		Buckets: risk.SummarizeBuckets(riskObservations, policy),
	}
	cooldown, err := store.GetCooldown(ctx, tx, batch.Service)
	if err != nil {
		return PreviewResult{}, err
	}
	result.Cooldown = cooldownStatus(batch.Service, cooldown, e.now())
	if mode == risk.ModeCompare {
		comparison, err := risk.Compare(policy, batch, riskObservations, meta)
		if err != nil {
			return PreviewResult{}, err
		}
		comparison.Fast.Status = batch.Status
		comparison.Full.Status = batch.Status
		result.Comparison = &comparison
	} else {
		evaluation, err := risk.Compute(policy, batch, riskObservations, meta, mode)
		if err != nil {
			return PreviewResult{}, err
		}
		evaluation.Status = batch.Status
		result.Evaluation = &evaluation
	}
	return result, nil
}

func (e *Engine) Cooldown(ctx context.Context, batchID string) (CooldownStatus, error) {
	batch, err := store.GetBatch(ctx, e.store.DB(), batchID)
	if err != nil {
		return CooldownStatus{}, err
	}
	cooldown, err := store.GetCooldown(ctx, e.store.DB(), batch.Service)
	if err != nil {
		return CooldownStatus{}, err
	}
	return cooldownStatus(batch.Service, cooldown, e.now()), nil
}

func (e *Engine) ListCooldowns(ctx context.Context) ([]CooldownStatus, error) {
	items, err := store.ListCooldowns(ctx, e.store.DB())
	if err != nil {
		return nil, err
	}
	now := e.now()
	result := make([]CooldownStatus, 0, len(items))
	for _, item := range items {
		result = append(result, cooldownStatus(item.Service, &item, now))
	}
	return result, nil
}

func cooldownStatus(service string, cooldown *store.Cooldown, now int64) CooldownStatus {
	status := CooldownStatus{Service: service, CheckedAtMS: now}
	if cooldown == nil {
		return status
	}
	status.BatchID = cooldown.BatchID
	status.TriggeredAtMS = cooldown.TriggeredAtMS
	status.ExpiresAtMS = cooldown.ExpiresAtMS
	if cooldown.ExpiresAtMS > now {
		status.Active = true
		status.RemainingMS = cooldown.ExpiresAtMS - now
	}
	return status
}

func toRiskObservations(records []store.ObservationRecord) []risk.StoreObservation {
	result := make([]risk.StoreObservation, 0, len(records))
	for _, record := range records {
		result = append(result, risk.StoreObservation{
			Observation: record.Observation, BucketStartMS: record.BucketStartMS,
			PolicyVersion: record.PolicyVersion, CommitSeq: record.CommitSeq,
		})
	}
	return result
}
