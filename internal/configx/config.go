package configx

import (
	"fmt"
	"reflect"

	"multirecall/internal/model"
)

type RouteDepths struct {
	Lexical  int `json:"lexical"`
	Category int `json:"category"`
	Vector   int `json:"vector"`
}

type PermilleBudgets struct {
	Lexical  int `json:"lexical_permille"`
	Category int `json:"category_permille"`
	Vector   int `json:"vector_permille"`
}

type RouteWeights struct {
	Lexical  float64 `json:"lexical"`
	Category float64 `json:"category"`
	Vector   float64 `json:"vector"`
}

type TruncationRule struct {
	TopK          int    `json:"top_k"`
	MaxCandidates int    `json:"max_candidates"`
	RecordClamp   bool   `json:"record_clamp"`
	DedupByItemID bool   `json:"dedup_by_item_id"`
	TieBreak      string `json:"tie_break"`
}

type FusionConfig struct {
	Method              string         `json:"method"`
	Weights             RouteWeights   `json:"weights"`
	RRFK                int            `json:"rrf_k"`
	LogBase             float64        `json:"log_base"`
	Truncation          TruncationRule `json:"truncation"`
	SimilarityTolerance float64        `json:"similarity_tolerance"`
	NumericTolerance    float64        `json:"numeric_tolerance"`
	SamplingPositions   []int          `json:"sampling_positions"`
}

type BudgetConfig struct {
	GlobalEvaluationLimit int              `json:"global_evaluation_limit"`
	QueryConcurrency      int              `json:"query_concurrency"`
	RoundTimeBudgetMS     int64            `json:"round_time_budget_ms"`
	ReductionOrder        []string         `json:"reduction_order"`
	RouteCostMicroseconds map[string]int64 `json:"route_cost_microseconds"`
}

type Snapshot struct {
	ConfigVersion  int64           `json:"config_version"`
	Kind           string          `json:"kind"`
	ContentDigest  string          `json:"content_digest"`
	CandidateDepth RouteDepths     `json:"candidate_depth"`
	PermilleBudget PermilleBudgets `json:"permille_budget"`
	Fusion         FusionConfig    `json:"fusion"`
	Budget         BudgetConfig    `json:"budget"`
}

type Content struct {
	CandidateDepth RouteDepths     `json:"candidate_depth"`
	PermilleBudget PermilleBudgets `json:"permille_budget"`
	Fusion         FusionConfig    `json:"fusion"`
	Budget         BudgetConfig    `json:"budget"`
}

type Fragment struct {
	BaseVersion    int64            `json:"base_version"`
	CandidateDepth *RouteDepths     `json:"candidate_depth,omitempty"`
	PermilleBudget *PermilleBudgets `json:"permille_budget,omitempty"`
	FusionWeights  *RouteWeights    `json:"fusion_weights,omitempty"`
	Truncation     *TruncationRule  `json:"truncation,omitempty"`
}

func ContentOf(s Snapshot) Content {
	return Content{CandidateDepth: s.CandidateDepth, PermilleBudget: s.PermilleBudget, Fusion: s.Fusion, Budget: s.Budget}
}

func RecalcDigest(s *Snapshot) string {
	s.ContentDigest = model.Digest(ContentOf(*s))
	return s.ContentDigest
}

func DefaultSnapshot() Snapshot {
	s := Snapshot{
		ConfigVersion:  1,
		Kind:           "seed",
		CandidateDepth: RouteDepths{Lexical: 8, Category: 6, Vector: 6},
		PermilleBudget: PermilleBudgets{Lexical: 1000, Category: 850, Vector: 750},
		Fusion: FusionConfig{
			Method:              "weighted_sum",
			Weights:             RouteWeights{Lexical: 1.15, Category: 0.9, Vector: 1.35},
			RRFK:                60,
			LogBase:             2,
			Truncation:          TruncationRule{TopK: 5, MaxCandidates: 24, RecordClamp: true, DedupByItemID: true, TieBreak: "fusion_score_desc,item_id_asc"},
			SimilarityTolerance: 1e-6,
			NumericTolerance:    2e-4,
			SamplingPositions:   []int{1, 2, 3, 5},
		},
		Budget: BudgetConfig{
			GlobalEvaluationLimit: 260,
			QueryConcurrency:      4,
			RoundTimeBudgetMS:     5000,
			ReductionOrder:        []string{"vector", "category", "lexical"},
			RouteCostMicroseconds: map[string]int64{"lexical": 2, "category": 1, "vector": 5},
		},
	}
	RecalcDigest(&s)
	return s
}

func ValidateWeights(w RouteWeights) error {
	if w.Lexical == 0 && w.Category == 0 && w.Vector == 0 {
		return model.NewError(model.CodeNoWeight, "lexical, category and vector fusion weights are all zero")
	}
	return nil
}

func ApplyFragment(current Snapshot, f Fragment) (Snapshot, string, error) {
	if f.BaseVersion != current.ConfigVersion {
		return Snapshot{}, "E_STALE_BASE", &model.APIError{
			Code: model.CodeStaleBase, Message: fmt.Sprintf("fragment base_version=%d does not match current=%d", f.BaseVersion, current.ConfigVersion),
			BaseVersion: f.BaseVersion, CurrentVersion: current.ConfigVersion,
		}
	}
	next := current
	next.ConfigVersion++
	next.Kind = "incremental_fragment"
	if f.CandidateDepth != nil {
		next.CandidateDepth = *f.CandidateDepth
	}
	if f.PermilleBudget != nil {
		next.PermilleBudget = *f.PermilleBudget
	}
	if f.FusionWeights != nil {
		next.Fusion.Weights = *f.FusionWeights
	}
	if f.Truncation != nil {
		next.Fusion.Truncation = *f.Truncation
	}
	RecalcDigest(&next)
	return next, "incremental_fragment", nil
}

func ClassifyReplacement(current, incoming Snapshot) (Snapshot, string, error) {
	if incoming.ConfigVersion < current.ConfigVersion {
		return Snapshot{}, "rollback", &model.APIError{
			Code: model.CodeStaleVersion, Message: "rejected configuration rollback",
			CurrentVersion: current.ConfigVersion, RequestedVersion: incoming.ConfigVersion,
			Digest: current.ContentDigest,
		}
	}
	RecalcDigest(&incoming)
	if incoming.ConfigVersion == current.ConfigVersion {
		if incoming.ContentDigest != current.ContentDigest {
			return Snapshot{}, "conflicting_same_version", &model.APIError{
				Code: model.CodeStaleVersion, Message: "same config version has a different content digest",
				CurrentVersion: current.ConfigVersion, RequestedVersion: incoming.ConfigVersion,
				Digest: incoming.ContentDigest,
			}
		}
		incoming.Kind = current.Kind
		return current, "unchanged", nil
	}
	kind := "normal_advance"
	if incoming.Kind == "whole_replace" || incoming.ContentDigest != current.ContentDigest {
		kind = "whole_replace"
	}
	incoming.Kind = kind
	return incoming, kind, nil
}

func EarliestAffectedStage(before, after Snapshot) string {
	if before.CandidateDepth != after.CandidateDepth || before.PermilleBudget != after.PermilleBudget || !reflect.DeepEqual(before.Budget, after.Budget) {
		return "budget"
	}
	return "fuse"
}
