package engine

import (
	"context"
	"encoding/json"

	"local/rollout-guard/internal/domain"
	"local/rollout-guard/internal/risk"
	"local/rollout-guard/internal/store"
)

type RollbackDispatch struct {
	TargetURL     string `json:"target_url"`
	Attempted     bool   `json:"attempted"`
	Success       bool   `json:"success"`
	StatusCode    int    `json:"status_code,omitempty"`
	LatencyMS     int64  `json:"latency_ms"`
	Error         string `json:"error,omitempty"`
	RequestedAtMS int64  `json:"requested_at_ms"`
}

type EvaluateResult struct {
	Evaluation risk.Evaluation      `json:"evaluation"`
	Batch      domain.Batch         `json:"batch"`
	Policy     domain.Policy        `json:"policy"`
	Buckets    []risk.BucketSummary `json:"buckets"`
	Cooldown   CooldownStatus       `json:"cooldown"`
	Rollback   *RollbackDispatch    `json:"rollback,omitempty"`
	IntentID   int64                `json:"intent_id"`
}

func (e *Engine) Evaluate(ctx context.Context, batchID, mode string, persist bool) (EvaluateResult, error) {
	if mode != risk.ModeFast && mode != risk.ModeFull {
		return EvaluateResult{}, domain.NewBizError("E_INVALID_MODE", "裁决模式必须为 fast 或 full", map[string]any{"mode": mode})
	}
	intentID, err := store.CreateIntent(ctx, e.store, "risk_decision", batchID, map[string]any{"mode": mode, "persist": persist})
	if err != nil {
		return EvaluateResult{}, err
	}
	tx, err := e.store.Begin(ctx)
	if err != nil {
		return EvaluateResult{}, err
	}
	defer tx.Rollback()
	policy, _, err := store.CurrentPolicy(ctx, tx)
	if err != nil {
		_ = failIntentTx(ctx, tx, intentID, "policy missing")
		return EvaluateResult{}, err
	}
	batch, err := store.GetBatch(ctx, tx, batchID)
	if err != nil {
		_ = failIntentTx(ctx, tx, intentID, "batch missing")
		return EvaluateResult{}, err
	}
	records, err := store.LoadObservations(ctx, tx, batchID)
	if err != nil {
		return EvaluateResult{}, err
	}
	asOf, err := store.CurrentCommit(ctx, tx)
	if err != nil {
		return EvaluateResult{}, err
	}
	riskObservations := toRiskObservations(records)
	meta := risk.SnapshotMeta{AsOfCommitSeq: asOf, ObservationRevision: batch.Revision}
	evaluation, err := risk.Compute(policy, batch, riskObservations, meta, mode)
	if err != nil {
		_ = failIntentTx(ctx, tx, intentID, "risk computation rejected")
		return EvaluateResult{}, err
	}
	evaluation.Status = batch.Status
	buckets := risk.SummarizeBuckets(riskObservations, policy)
	cooldownExisting, err := store.GetCooldown(ctx, tx, batch.Service)
	if err != nil {
		return EvaluateResult{}, err
	}
	currentCooldown := cooldownStatus(batch.Service, cooldownExisting, e.now())
	if !persist {
		_ = failIntentTx(ctx, tx, intentID, "preview only")
		return EvaluateResult{
			Evaluation: evaluation, Batch: batch, Policy: policy, Buckets: buckets,
			Cooldown: currentCooldown, IntentID: intentID,
		}, nil
	}

	statusBefore := batch.Status
	statusAfter := statusBefore
	rollbackNeeded := false
	blockedByCooldown := false
	blockingCooldown := currentCooldown
	if statusBefore == domain.StatusObserving {
		switch evaluation.Decision.Action {
		case risk.ActionPass:
			statusAfter = domain.StatusPassed
		case risk.ActionEscalate:
			statusAfter = domain.StatusEscalated
		case risk.ActionRollback:
			if currentCooldown.Active {
				blockedByCooldown = true
			} else {
				statusAfter = domain.StatusRollingBack
				rollbackNeeded = true
			}
		default:
			statusAfter = statusBefore
		}
	}
	applied := rollbackNeeded || statusAfter != statusBefore
	evaluation.Status = statusAfter

	now := e.now()
	commit, err := store.NextCommit(ctx, tx, "risk_decision", now)
	if err != nil {
		return EvaluateResult{}, err
	}
	cooldown := currentCooldown
	if rollbackNeeded {
		expires := now + policy.CooldownMS
		if err := store.UpsertCooldownTx(ctx, tx, store.Cooldown{
			Service: batch.Service, BatchID: batch.ID, TriggeredAtMS: now, ExpiresAtMS: expires, CommitSeq: commit,
		}); err != nil {
			return EvaluateResult{}, err
		}
		cooldown = CooldownStatus{
			Service: batch.Service, BatchID: batch.ID, Active: expires > now, TriggeredAtMS: now,
			ExpiresAtMS: expires, RemainingMS: max64(0, expires-now), CheckedAtMS: now,
		}
	}
	if statusAfter != statusBefore {
		if err := store.UpdateBatchStatusTx(ctx, tx, batch.ID, statusAfter, now); err != nil {
			return EvaluateResult{}, err
		}
	}
	evaluation.Applied = applied
	evaluationJSON, err := json.Marshal(evaluation)
	if err != nil {
		return EvaluateResult{}, err
	}
	action := evaluation.Decision.Action
	intentStatus := "COMPLETED"
	intentNote := "risk evaluation committed"
	if blockedByCooldown {
		action = "ROLLBACK_BLOCKED_COOLDOWN"
		intentStatus = "FAILED"
		intentNote = "rollback blocked by service cooldown"
	}
	if rollbackNeeded {
		intentStatus = "APPLIED"
		intentNote = "ROLLING_BACK and cooldown committed; webhook pending"
	}
	if err := store.InsertRiskEvaluationTx(ctx, tx, batch.ID, statusBefore, statusAfter, evaluationJSON, mode,
		evaluation.ScoreBP, evaluation.Level, action, policy.Version, policy.Checksum, asOf, batch.Revision,
		evaluation.Window.LatestObservationMS, evaluation.Window.StartMS, evaluation.Window.EndMS,
		mode == risk.ModeFast, commit, now); err != nil {
		return EvaluateResult{}, err
	}
	if err := store.SetIntentTx(ctx, tx, intentID, intentStatus, intentNote, map[string]any{
		"commit_seq": commit, "status_before": statusBefore, "status_after": statusAfter,
		"score_bp": evaluation.ScoreBP, "action": action,
	}); err != nil {
		return EvaluateResult{}, err
	}
	e.maybeCrash("risk_eval_before_commit", intentID, batch.ID)
	if err := tx.Commit(); err != nil {
		return EvaluateResult{}, err
	}
	if rollbackNeeded {
		e.maybeCrash("rollback_after_commit", intentID, batch.ID)
	}
	if blockedByCooldown {
		return EvaluateResult{}, domain.NewBizError("E_ROLLBACK_COOLDOWN", "自动回滚被服务冷却期拦截", map[string]any{
			"batch_id": batch.ID, "service": batch.Service, "remaining_ms": blockingCooldown.RemainingMS,
			"expires_at_ms": blockingCooldown.ExpiresAtMS, "blocking_batch_id": blockingCooldown.BatchID,
			"evaluation": evaluation,
		})
	}
	result := EvaluateResult{
		Evaluation: evaluation, Batch: batch, Policy: policy, Buckets: buckets, Cooldown: cooldown, IntentID: intentID,
	}
	if rollbackNeeded {
		dispatch := e.dispatchRollback(ctx, batch, policy, evaluation, "auto_rollback")
		finalStatus := domain.StatusRolledBack
		finalIntentStatus := "COMPLETED"
		finalNote := "rollback webhook succeeded"
		if dispatch == nil || !dispatch.Success {
			finalStatus = domain.StatusEscalated
			finalIntentStatus = "FAILED"
			finalNote = "rollback webhook failed; batch escalated"
		}
		if err := e.applyFinalStatus(ctx, batch.ID, finalStatus, intentID, finalIntentStatus, finalNote, dispatch); err != nil {
			return result, err
		}
		result.Batch.Status = finalStatus
		result.Evaluation.Status = finalStatus
		result.Rollback = dispatch
	} else {
		result.Batch.Status = statusAfter
	}
	return result, nil
}

func (e *Engine) TriggerManualRollback(ctx context.Context, batchID, reason string) (EvaluateResult, error) {
	intentID, err := store.CreateIntent(ctx, e.store, "manual_rollback", batchID, map[string]any{"reason": reason})
	if err != nil {
		return EvaluateResult{}, err
	}
	tx, err := e.store.Begin(ctx)
	if err != nil {
		return EvaluateResult{}, err
	}
	defer tx.Rollback()
	batch, err := store.GetBatch(ctx, tx, batchID)
	if err != nil {
		_ = failIntentTx(ctx, tx, intentID, "batch missing")
		return EvaluateResult{}, err
	}
	policy, _, err := store.CurrentPolicy(ctx, tx)
	if err != nil {
		_ = failIntentTx(ctx, tx, intentID, "policy missing")
		return EvaluateResult{}, err
	}
	if !domain.ValidTransition(batch.Status, domain.StatusRollingBack) {
		_ = failIntentTx(ctx, tx, intentID, "invalid transition")
		return EvaluateResult{}, domain.NewBizError("E_INVALID_TRANSITION", "当前状态不允许进入 ROLLING_BACK", map[string]any{
			"batch_id": batchID, "from": batch.Status, "to": domain.StatusRollingBack,
		})
	}
	now := e.now()
	existing, err := store.GetCooldown(ctx, tx, batch.Service)
	if err != nil {
		return EvaluateResult{}, err
	}
	current := cooldownStatus(batch.Service, existing, now)
	if current.Active {
		_ = failIntentTx(ctx, tx, intentID, "rollback blocked by cooldown")
		return EvaluateResult{}, domain.NewBizError("E_ROLLBACK_COOLDOWN", "冷却期内禁止重复回滚", map[string]any{
			"batch_id": batchID, "service": batch.Service, "remaining_ms": current.RemainingMS,
			"expires_at_ms": current.ExpiresAtMS, "blocking_batch_id": current.BatchID,
		})
	}
	commit, err := store.NextCommit(ctx, tx, "manual_rollback", now)
	if err != nil {
		return EvaluateResult{}, err
	}
	expires := now + policy.CooldownMS
	if err := store.UpsertCooldownTx(ctx, tx, store.Cooldown{
		Service: batch.Service, BatchID: batch.ID, TriggeredAtMS: now, ExpiresAtMS: expires, CommitSeq: commit,
	}); err != nil {
		return EvaluateResult{}, err
	}
	if err := store.UpdateBatchStatusTx(ctx, tx, batch.ID, domain.StatusRollingBack, now); err != nil {
		return EvaluateResult{}, err
	}
	if err := store.SetIntentTx(ctx, tx, intentID, "APPLIED", "manual rollback committed; webhook pending", map[string]any{
		"commit_seq": commit, "reason": reason,
	}); err != nil {
		return EvaluateResult{}, err
	}
	e.maybeCrash("manual_rollback_before_commit", intentID, batch.ID)
	if err := tx.Commit(); err != nil {
		return EvaluateResult{}, err
	}
	dispatch := e.dispatchRollback(ctx, batch, policy, risk.Evaluation{
		ScoreBP: 0, Level: "MANUAL", PolicyVersion: policy.Version,
		Decision: risk.Decision{Reason: reason, Action: risk.ActionRollback},
	}, "manual_rollback")
	finalStatus := domain.StatusRolledBack
	intentStatus := "COMPLETED"
	note := "manual rollback webhook succeeded"
	if dispatch == nil || !dispatch.Success {
		finalStatus = domain.StatusEscalated
		intentStatus = "FAILED"
		note = "manual rollback webhook failed; escalated"
	}
	if err := e.applyFinalStatus(ctx, batch.ID, finalStatus, intentID, intentStatus, note, dispatch); err != nil {
		return EvaluateResult{}, err
	}
	batch.Status = finalStatus
	return EvaluateResult{
		Batch: batch, Policy: policy, IntentID: intentID, Rollback: dispatch,
		Cooldown: CooldownStatus{Service: batch.Service, BatchID: batch.ID, Active: true, TriggeredAtMS: now,
			ExpiresAtMS: expires, RemainingMS: max64(0, expires-e.now()), CheckedAtMS: e.now()},
	}, nil
}

func (e *Engine) Transition(ctx context.Context, batchID, to, reason string) (domain.Batch, error) {
	intentID, err := store.CreateIntent(ctx, e.store, "manual_transition", batchID, map[string]any{"to": to, "reason": reason})
	if err != nil {
		return domain.Batch{}, err
	}
	tx, err := e.store.Begin(ctx)
	if err != nil {
		return domain.Batch{}, err
	}
	defer tx.Rollback()
	batch, err := store.GetBatch(ctx, tx, batchID)
	if err != nil {
		_ = failIntentTx(ctx, tx, intentID, "batch missing")
		return domain.Batch{}, err
	}
	if !domain.ValidTransition(batch.Status, to) {
		_ = failIntentTx(ctx, tx, intentID, "invalid transition")
		return domain.Batch{}, domain.NewBizError("E_INVALID_TRANSITION", "非法状态迁移", map[string]any{
			"batch_id": batchID, "from": batch.Status, "to": to,
		})
	}
	if to == domain.StatusRollingBack {
		_ = failIntentTx(ctx, tx, intentID, "use rollback endpoint for cooldown-safe transition")
		return domain.Batch{}, domain.NewBizError("E_INVALID_TRANSITION", "进入 ROLLING_BACK 必须经过回滚裁决端点", map[string]any{
			"batch_id": batchID, "from": batch.Status, "to": to,
		})
	}
	now := e.now()
	commit, err := store.NextCommit(ctx, tx, "manual_transition", now)
	if err != nil {
		return domain.Batch{}, err
	}
	if err := store.UpdateBatchStatusTx(ctx, tx, batch.ID, to, now); err != nil {
		return domain.Batch{}, err
	}
	if err := store.SetIntentTx(ctx, tx, intentID, "COMPLETED", "manual transition committed", map[string]any{"commit_seq": commit}); err != nil {
		return domain.Batch{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Batch{}, err
	}
	batch.Status = to
	batch.Revision++
	batch.UpdatedAtMS = now
	return batch, nil
}
