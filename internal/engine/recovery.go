package engine

import (
	"context"
	"encoding/json"

	"local/rollout-guard/internal/domain"
	"local/rollout-guard/internal/risk"
	"local/rollout-guard/internal/store"
)

type RecoveryReport struct {
	GeneratedAtMS    int64            `json:"generated_at_ms"`
	PendingFound     int              `json:"pending_found"`
	AbortedIntents   []store.Intent   `json:"aborted_intents"`
	ResumedRollbacks []map[string]any `json:"resumed_rollbacks"`
	RollingBackFound int              `json:"rolling_back_found"`
	Batches          []domain.Batch   `json:"batches"`
	LatestCommitSeq  int64            `json:"latest_commit_seq"`
	Clean            bool             `json:"clean"`
	FactsOnly        bool             `json:"facts_only"`
}

func (e *Engine) RecoverOnStartup(ctx context.Context) (RecoveryReport, error) {
	e.recoveryMu.Lock()
	defer e.recoveryMu.Unlock()
	pending, err := store.ListIntents(ctx, e.store.DB(), "status = ?", "PENDING")
	if err != nil {
		return RecoveryReport{}, err
	}
	aborted := make([]store.Intent, 0, len(pending))
	for _, intent := range pending {
		if err := store.SetIntentDirect(ctx, e.store, intent.ID, "ABORTED", "startup recovery: uncommitted intent discarded"); err != nil {
			return RecoveryReport{}, err
		}
		intent.Status = "ABORTED"
		intent.Note = "startup recovery: uncommitted intent discarded"
		aborted = append(aborted, intent)
	}
	applied, err := store.ListIntents(ctx, e.store.DB(), "status = ? AND kind IN ('risk_decision','manual_rollback')", "APPLIED")
	if err != nil {
		return RecoveryReport{}, err
	}
	resumed := make([]map[string]any, 0)
	for _, intent := range applied {
		batch, err := store.GetBatch(ctx, e.store.DB(), intent.BatchID)
		if err != nil {
			_ = store.SetIntentDirect(ctx, e.store, intent.ID, "ABORTED", "startup recovery: batch missing")
			continue
		}
		if batch.Status != domain.StatusRollingBack {
			_ = store.SetIntentDirect(ctx, e.store, intent.ID, "COMPLETED", "startup recovery: state already settled")
			resumed = append(resumed, map[string]any{"batch_id": batch.ID, "result": "already_settled", "status": batch.Status})
			continue
		}
		policy, _, policyErr := store.CurrentPolicy(ctx, e.store.DB())
		if policyErr != nil {
			_ = store.SetIntentDirect(ctx, e.store, intent.ID, "FAILED", "startup recovery: policy unavailable")
			continue
		}
		dispatch := e.dispatchRollback(ctx, batch, policy, riskEvaluationRecovery(), "rollback_recovery")
		finalStatus := domain.StatusRolledBack
		status := "COMPLETED"
		if dispatch == nil || !dispatch.Success {
			finalStatus = domain.StatusEscalated
			status = "FAILED"
		}
		if err := e.applyFinalStatus(ctx, batch.ID, finalStatus, intent.ID, status, "startup recovery finalized rollback", dispatch); err != nil {
			return RecoveryReport{}, err
		}
		resumed = append(resumed, map[string]any{
			"batch_id": batch.ID, "result": status, "status": finalStatus, "webhook": dispatch,
		})
	}
	batches, err := store.ListBatches(ctx, e.store.DB())
	if err != nil {
		return RecoveryReport{}, err
	}
	latestCommit, err := store.CurrentCommit(ctx, e.store.DB())
	if err != nil {
		return RecoveryReport{}, err
	}
	rollingBack := 0
	for _, batch := range batches {
		if batch.Status == domain.StatusRollingBack {
			rollingBack++
		}
	}
	report := RecoveryReport{
		GeneratedAtMS: e.now(), PendingFound: len(pending), AbortedIntents: aborted, ResumedRollbacks: resumed,
		RollingBackFound: rollingBack, Batches: batches, LatestCommitSeq: latestCommit,
		Clean: len(pending) == 0 && len(applied) == 0 && rollingBack == 0, FactsOnly: true,
	}
	reportMap := map[string]any{}
	raw, _ := json.Marshal(report)
	_ = json.Unmarshal(raw, &reportMap)
	if _, err := store.SaveRecoveryRun(ctx, e.store, reportMap); err != nil {
		return RecoveryReport{}, err
	}
	return report, nil
}

func (e *Engine) Recovery(ctx context.Context) (store.RecoveryRun, error) {
	return store.LatestRecoveryRun(ctx, e.store.DB())
}

func riskEvaluationRecovery() risk.Evaluation {
	return risk.Evaluation{Decision: risk.Decision{Reason: "startup recovery", Action: risk.ActionRollback}}
}
