package engine

import (
	"fmt"
	"sort"

	"motionlab/internal/model"
	"motionlab/internal/store"
)

func BuildPlan(spec model.SequenceSpec, config model.RunConfig, policy model.PolicySnapshot) ([]model.BranchPlan, error) {
	ids := make([]int, 0, spec.Branches)
	if len(config.BranchFilter) > 0 {
		for _, id := range model.SortedUniqueInts(config.BranchFilter) {
			if id < 0 || id >= spec.Branches {
				return nil, model.NewError(model.CodeValidation, fmt.Sprintf("branch filter %d outside sequence branch range", id))
			}
			ids = append(ids, id)
		}
	} else {
		for i := 0; i < spec.Branches; i++ {
			ids = append(ids, i)
		}
	}
	if len(ids) == 0 {
		return nil, model.NewError(model.CodeValidation, "branch plan is empty")
	}
	concurrency := config.Concurrency
	if policy.Concurrency > 0 && policy.Concurrency < concurrency {
		concurrency = policy.Concurrency
	}
	if concurrency < 1 {
		concurrency = 1
	}
	target := config.EffectiveStepsForBranch(policy.BranchQuota)
	cost := target*model.VirtualStepCost + 2
	perWave := concurrency
	if cost <= 0 || config.RoundBudgetMS < cost {
		perWave = 0
	} else if byBudget := config.RoundBudgetMS / cost; byBudget < perWave {
		perWave = byBudget
	}
	if perWave < 0 {
		perWave = 0
	}
	plans := make([]model.BranchPlan, 0, len(ids))
	admittedIndex := 0
	for _, id := range ids {
		p := model.BranchPlan{
			BranchID:        id,
			RequestedSteps:  config.RequestedSteps,
			TargetSteps:     target,
			EstimatedWorkMS: cost,
			Admitted:        perWave > 0,
		}
		if target < 1 {
			p.Admitted = false
			p.Reason = "E_BRANCH_QUOTA: effective step quota is zero"
		} else if perWave == 0 {
			p.Reason = fmt.Sprintf("E_TIME_BUDGET: branch needs %d virtual-ms, round budget is %d ms", cost, config.RoundBudgetMS)
		} else {
			p.Wave = 1 + admittedIndex/perWave
			admittedIndex++
		}
		plans = append(plans, p)
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].BranchID < plans[j].BranchID })
	return plans, nil
}

func BranchSeeds(plans []model.BranchPlan) []store.BranchSeed {
	out := make([]store.BranchSeed, 0, len(plans))
	for _, p := range plans {
		status := model.BranchPending
		reason := ""
		if !p.Admitted {
			status = model.BranchNotStarted
			reason = p.Reason
		}
		out = append(out, store.BranchSeed{
			BranchID:       p.BranchID,
			Status:         status,
			Reason:         reason,
			Wave:           p.Wave,
			RequestedSteps: p.RequestedSteps,
			TargetSteps:    p.TargetSteps,
		})
	}
	return out
}

func WithPolicyDefaults(config model.RunConfig, policy model.PolicySnapshot) model.RunConfig {
	config.Concurrency = policy.Concurrency
	config.BranchQuota = policy.BranchQuota
	config.RoundBudgetMS = policy.RoundBudgetMS
	config.PruneThreshold = policy.PruneThreshold
	config.VirtualTimeMode = true
	return config
}
