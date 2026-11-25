package pipeline

import (
	"fmt"

	"headprune/internal/model"
)

// ValidateDataset performs stage-independent structural validation and maps
// every defect to a stable business code with head/probe identifiers.
func ValidateDataset(ds model.Dataset) error {
	if len(ds.Heads) == 0 {
		return &model.BizError{Code: model.CodeHeadInvalid, Message: "no attention head shards provided"}
	}
	seen := map[model.HeadID]bool{}
	for _, h := range ds.Heads {
		id := h.ID()
		if seen[id] {
			return (&model.BizError{Code: model.CodeHeadInvalid, Message: "duplicate head shard"}).WithHead(h.Layer, h.Head)
		}
		seen[id] = true
		if h.Dim <= 0 {
			return (&model.BizError{Code: model.CodeHeadInvalid, Message: "head dim must be positive"}).WithHead(h.Layer, h.Head)
		}
		if len(h.Weight) != h.Dim {
			return (&model.BizError{Code: model.CodeHeadInvalid,
				Message: fmt.Sprintf("weight length %d mismatches head dim %d", len(h.Weight), h.Dim)}).WithHead(h.Layer, h.Head)
		}
		if len(h.Activations) != h.Dim {
			return (&model.BizError{Code: model.CodeHeadInvalid,
				Message: fmt.Sprintf("activation length %d mismatches head dim %d", len(h.Activations), h.Dim)}).WithHead(h.Layer, h.Head)
		}
		if len(h.SynergyWeight) != 0 && len(h.SynergyWeight) != h.Dim {
			return (&model.BizError{Code: model.CodeHeadInvalid,
				Message: "synergy weight length mismatches head dim"}).WithHead(h.Layer, h.Head)
		}
		for i, w := range h.Weight {
			if !model.Finite(w) {
				return (&model.BizError{Code: model.CodeHeadInvalid,
					Message: fmt.Sprintf("weight[%d] is NaN/Inf", i)}).WithHead(h.Layer, h.Head)
			}
		}
	}
	if len(ds.Sentences) == 0 {
		return &model.BizError{Code: model.CodeSentenceInvalid, Message: "no masked sentences provided"}
	}
	for _, s := range ds.Sentences {
		if s.ID == "" {
			return &model.BizError{Code: model.CodeSentenceInvalid, Message: "sentence id is empty"}
		}
		if len(s.Candidates) == 0 {
			return (&model.BizError{Code: model.CodeSentenceInvalid, Message: "sentence has no truth candidates"}).WithProbe(s.ID)
		}
		trueCount := 0
		for ci, c := range s.Candidates {
			if len(c.Feat) != ds.Heads[0].Dim {
				return (&model.BizError{Code: model.CodeSentenceInvalid,
					Message: fmt.Sprintf("candidate[%d] feature length mismatches head dim", ci)}).WithProbe(s.ID)
			}
			if c.IsTrue {
				trueCount++
			}
		}
		if trueCount == 0 {
			return (&model.BizError{Code: model.CodeSentenceInvalid, Message: "sentence missing ground-truth candidate"}).WithProbe(s.ID)
		}
	}
	return ValidatePolicy(ds.Policy)
}

// ValidatePolicy checks the strategy snapshot independently so setting a bad
// policy fails before any replay work starts.
func ValidatePolicy(p model.Policy) error {
	if p.ProbeLimit == 0 {
		return &model.BizError{Code: model.CodePolicyInvalid, Message: "probe sample limit must not be zero"}
	}
	if p.ProbeLimit < 0 {
		return &model.BizError{Code: model.CodePolicyInvalid, Message: "probe sample limit must be positive"}
	}
	if p.MinProbes < 0 {
		return &model.BizError{Code: model.CodePolicyInvalid, Message: "min probes must not be negative"}
	}
	if p.Tolerance < 0 {
		return &model.BizError{Code: model.CodePolicyInvalid, Message: "reconstruction tolerance must not be negative"}
	}
	if p.QuantBits < 2 {
		return &model.BizError{Code: model.CodePolicyInvalid, Message: "quant bits must be >= 2"}
	}
	if p.MaxPruneHeads <= 0 {
		return &model.BizError{Code: model.CodePolicyInvalid, Message: "max prune heads must be positive"}
	}
	if len(p.CandidateRatios) == 0 {
		return &model.BizError{Code: model.CodePolicyInvalid, Message: "candidate ratios must not be empty"}
	}
	for _, r := range p.CandidateRatios {
		if r <= 0 || r > 1 {
			return &model.BizError{Code: model.CodePolicyInvalid, Message: "candidate ratio must be within (0,1]"}
		}
	}
	if p.CouplingThreshold < 0 {
		return &model.BizError{Code: model.CodePolicyInvalid, Message: "coupling deviation threshold must not be negative"}
	}
	return nil
}
