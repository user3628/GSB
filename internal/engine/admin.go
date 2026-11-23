package engine

import (
	"context"

	"local/rollout-guard/internal/domain"
	"local/rollout-guard/internal/store"
)

func (e *Engine) Snapshot(ctx context.Context) (map[string]any, error) {
	tx, err := e.store.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var policy any
	currentPolicy, _, policyErr := store.CurrentPolicy(ctx, tx)
	if policyErr == nil {
		policy = currentPolicy
	} else if biz, ok := domain.AsBizError(policyErr); !ok || biz.Code != "E_POLICY_MISSING" {
		return nil, policyErr
	}
	batches, err := store.ListBatches(ctx, tx)
	if err != nil {
		return nil, err
	}
	cooldowns, err := store.ListCooldowns(ctx, tx)
	if err != nil {
		return nil, err
	}
	asOf, err := store.CurrentCommit(ctx, tx)
	if err != nil {
		return nil, err
	}
	now := e.now()
	activeCooldowns := make([]CooldownStatus, 0)
	for _, item := range cooldowns {
		status := cooldownStatus(item.Service, &item, now)
		if status.Active {
			activeCooldowns = append(activeCooldowns, status)
		}
	}
	var recovery any
	latestRecovery, recoveryErr := store.LatestRecoveryRun(ctx, tx)
	if recoveryErr == nil {
		recovery = latestRecovery
	}
	return map[string]any{
		"policy": policy, "batches": batches, "cooldowns": activeCooldowns,
		"as_of_commit_seq": asOf, "recovery": recovery, "now_ms": now,
	}, nil
}

func (e *Engine) LatestIntent(ctx context.Context, id int64) (store.Intent, error) {
	return store.GetIntent(ctx, e.store.DB(), id)
}

func (e *Engine) ListIntents(ctx context.Context, limit int) ([]store.Intent, error) {
	intents, err := store.ListIntents(ctx, e.store.DB(), "")
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(intents) > limit {
		intents = intents[len(intents)-limit:]
	}
	return intents, nil
}
