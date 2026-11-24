package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"motionlab/internal/model"
	"motionlab/internal/store"
)

type PolicyDecision struct {
	Classification string `json:"classification"`
	Reason         string `json:"reason"`
	Invalidated    bool   `json:"invalidated"`
	RunID          string `json:"run_id"`
}

func ClassifyPolicy(current, next model.PolicySnapshot) (string, string, error) {
	if current.Hash() == next.Hash() {
		return "advance", "identical policy snapshot; checkpoint may advance normally", nil
	}
	if next.Version < current.Version {
		e := model.NewError(model.CodePolicyRollback,
			fmt.Sprintf("policy rollback from version %d to %d is rejected", current.Version, next.Version))
		e.Details = map[string]any{
			"current_version": current.Version, "candidate_version": next.Version,
			"current_hash": current.Hash(), "candidate_hash": next.Hash(),
		}
		return "", "", e
	}
	parentMatch := next.ParentVersion == current.Version && next.ParentHash == current.Hash()
	sameOps := sameOperationalPolicy(current, next)
	if next.Version > current.Version && parentMatch && sameOps {
		return "advance", "parent-linked metadata advance; branch facts remain valid", nil
	}
	reason := "policy snapshot was wholly replaced"
	if next.Version == current.Version {
		reason = "policy content changed without a version advance"
	} else if !parentMatch {
		reason = "policy version chain does not point at the current snapshot"
	} else if !sameOps {
		reason = "operational policy fields changed; branch facts require rebuild"
	}
	return "replacement", reason, nil
}

func sameOperationalPolicy(a, b model.PolicySnapshot) bool {
	return a.Concurrency == b.Concurrency && a.BranchQuota == b.BranchQuota &&
		a.RoundBudgetMS == b.RoundBudgetMS && a.PruneThreshold == b.PruneThreshold
}

func (e *Engine) ApplyPolicy(ctx context.Context, runID string, next model.PolicySnapshot) (*PolicyDecision, error) {
	if err := next.Validate(); err != nil {
		return nil, err
	}
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if e.IsActive(runID) {
		return nil, model.NewError(model.CodeRunBusy, "stop the execution before replacing its policy")
	}
	var current model.PolicySnapshot
	if err := json.Unmarshal([]byte(run.PolicyJSON), &current); err != nil {
		return nil, model.NewError(model.CodeInternal, "stored policy snapshot is corrupt")
	}
	classification, reason, err := ClassifyPolicy(current, next)
	if err != nil {
		return nil, err
	}
	nextJSON, _ := json.Marshal(next)
	decision := &PolicyDecision{Classification: classification, Reason: reason, RunID: runID}
	switch classification {
	case "advance":
		if current.Hash() != next.Hash() {
			if err := e.store.AdvancePolicy(ctx, runID, next, string(nextJSON), classification, reason); err != nil {
				return nil, err
			}
		}
		return decision, nil
	case "replacement":
		seq, appErr := e.Catalog().Sequence(run.SequenceID)
		if appErr != nil {
			return nil, appErr
		}
		var config model.RunConfig
		if err := json.Unmarshal([]byte(run.ConfigJSON), &config); err != nil {
			return nil, model.NewError(model.CodeInternal, "stored config is corrupt")
		}
		config = WithPolicyDefaults(config, next)
		normalized, err := config.Normalize(seq.Spec, next)
		if err != nil {
			return nil, err
		}
		plans, err := BuildPlan(seq.Spec, normalized, next)
		if err != nil {
			return nil, err
		}
		configJSON, _ := json.Marshal(normalized)
		if err := e.store.ApplyPolicy(ctx, runID, next, string(nextJSON), string(configJSON), model.Digest(normalized), BranchSeeds(plans), classification, reason); err != nil {
			return nil, err
		}
		decision.Invalidated = true
		return decision, nil
	default:
		return nil, model.NewError(model.CodeInternal, "unknown policy classification")
	}
}

func (e *Engine) PolicyEvents(ctx context.Context, runID string) ([]store.PolicyEvent, error) {
	return e.store.PolicyEvents(ctx, runID)
}
