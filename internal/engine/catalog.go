package engine

import (
	"context"

	"local/rollout-guard/internal/domain"
	"local/rollout-guard/internal/store"
)

type SubmitPolicyResult struct {
	Policy    domain.Policy `json:"policy"`
	CommitSeq int64         `json:"commit_seq"`
	IntentID  int64         `json:"intent_id"`
}

type SubmitBatchResult struct {
	Batch     domain.Batch `json:"batch"`
	CommitSeq int64        `json:"commit_seq"`
	IntentID  int64        `json:"intent_id"`
}

func (e *Engine) SubmitPolicy(ctx context.Context, policy domain.Policy) (SubmitPolicyResult, error) {
	if policy.Version <= 0 {
		return SubmitPolicyResult{}, domain.NewBizError("E_POLICY_INVALID", "策略版本号必须为正整数", map[string]any{"field": "version"})
	}
	intentID, err := store.CreateIntent(ctx, e.store, "policy_submit", "", map[string]any{"version": policy.Version})
	if err != nil {
		return SubmitPolicyResult{}, err
	}
	tx, err := e.store.Begin(ctx)
	if err != nil {
		return SubmitPolicyResult{}, err
	}
	defer tx.Rollback()
	current, err := store.CurrentPolicyVersion(ctx, tx)
	if err != nil {
		return SubmitPolicyResult{}, err
	}
	if policy.Version <= current {
		_ = store.SetIntentTx(ctx, tx, intentID, "FAILED", "stale snapshot rejected", map[string]any{"current_version": current})
		_ = tx.Commit()
		return SubmitPolicyResult{}, domain.NewBizError("E_STALE_SNAPSHOT", "策略版本必须大于当前已提交版本", map[string]any{
			"submitted_version": policy.Version, "current_version": current,
		})
	}
	if err := policy.Validate(); err != nil {
		_ = failIntentTx(ctx, tx, intentID, "policy validation failed")
		return SubmitPolicyResult{}, err
	}
	now := e.now()
	commit, err := store.InsertPolicyTx(ctx, tx, policy, now)
	if err != nil {
		return SubmitPolicyResult{}, err
	}
	if err := store.SetIntentTx(ctx, tx, intentID, "COMPLETED", "policy snapshot applied", map[string]any{"commit_seq": commit}); err != nil {
		return SubmitPolicyResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return SubmitPolicyResult{}, err
	}
	return SubmitPolicyResult{Policy: policy, CommitSeq: commit, IntentID: intentID}, nil
}

func (e *Engine) SubmitBatch(ctx context.Context, batch domain.Batch) (SubmitBatchResult, error) {
	if err := batch.Validate(); err != nil {
		return SubmitBatchResult{}, err
	}
	intentID, err := store.CreateIntent(ctx, e.store, "batch_submit", batch.ID, map[string]any{"service": batch.Service})
	if err != nil {
		return SubmitBatchResult{}, err
	}
	tx, err := e.store.Begin(ctx)
	if err != nil {
		return SubmitBatchResult{}, err
	}
	defer tx.Rollback()
	if _, err := store.GetBatch(ctx, tx, batch.ID); err == nil {
		_ = failIntentTx(ctx, tx, intentID, "duplicate batch id")
		return SubmitBatchResult{}, domain.NewBizError("E_BATCH_EXISTS", "批次已存在", map[string]any{"batch_id": batch.ID})
	} else if biz, ok := domain.AsBizError(err); !ok || biz.Code != "E_NOT_FOUND" {
		return SubmitBatchResult{}, err
	}
	now := e.now()
	batch.Status = domain.StatusObserving
	commit, err := store.InsertBatchTx(ctx, tx, batch, now)
	if err != nil {
		return SubmitBatchResult{}, err
	}
	batch.CreatedAtMS = now
	batch.UpdatedAtMS = now
	batch.Revision = 0
	if err := store.SetIntentTx(ctx, tx, intentID, "COMPLETED", "batch created", map[string]any{"commit_seq": commit}); err != nil {
		return SubmitBatchResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return SubmitBatchResult{}, err
	}
	return SubmitBatchResult{Batch: batch, CommitSeq: commit, IntentID: intentID}, nil
}
