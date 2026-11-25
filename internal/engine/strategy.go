package engine

import (
	"fmt"

	"csrouter/internal/domain"
)

const (
	StrategyExactFull     = "exact_full"
	StrategyVocabPriority = "vocab_priority_truncated"
	StrategyHashBucket    = "hash_bucket_budget"
)

type StrategyMetrics struct {
	ExactOps                     int64
	VocabOps                     int64
	HashOps                      int64
	VocabRetainedTermOccurrences int64
	HashDistinctBucketUses       int64
}

func EstimateStrategyMetrics(model *Model, norms []domain.NormalizeArtifact) StrategyMetrics {
	metrics := StrategyMetrics{}
	for _, norm := range norms {
		occurrences, _, _ := aggregateTerms(norm)
		metrics.ExactOps += int64(len(norm.Tokens)) * int64(len(model.Intents))

		retained := 0
		buckets := map[int]bool{}
		for _, occ := range occurrences {
			if model.VocabTopSet[occ.term] {
				retained += occ.n
			}
			feature := occ.term
			if _, known := model.TermTotal[feature]; !known {
				feature = UnknownToken
			}
			buckets[HashBucket(feature, model.Policy.HashDimensions)] = true
		}
		metrics.VocabOps += int64(retained) * int64(len(model.Intents))
		metrics.HashOps += int64(len(buckets)) * int64(len(model.Intents))
		metrics.HashDistinctBucketUses += int64(len(buckets))
	}
	return metrics
}

func PlanStrategy(scale domain.ScaleObservation, metrics StrategyMetrics) domain.StrategyDecision {
	intents := int64(scale.IntentCount)
	if intents == 0 {
		intents = 1
	}
	budget := int64(scale.RoundBudget)
	if budget <= 0 {
		budget = 1
	}

	exactOps := metrics.ExactOps
	if exactOps == 0 {
		exactOps = int64(scale.ObservedTotalTerms) * intents
	}
	vocabOps := metrics.VocabOps
	if vocabOps == 0 {
		vocabOps = exactOps
	}
	hashOps := metrics.HashOps
	if hashOps == 0 {
		hashOps = exactOps
	}

	candidates := []domain.StrategyCandidate{
		{
			Strategy:     StrategyExactFull,
			EstimatedOps: exactOps,
			FitsBudget:   exactOps <= budget,
			Reason:       "uses every observed token and full conditional count table; zero approximation",
		},
		{
			Strategy:     StrategyVocabPriority,
			EstimatedOps: vocabOps,
			FitsBudget:   vocabOps <= budget,
			Reason:       fmt.Sprintf("retains top %d discriminative vocabulary entries and folds/drops the long tail", scaleBucketK(scale)),
		},
		{
			Strategy:         StrategyHashBucket,
			EstimatedOps:     hashOps,
			EstimatedBuckets: scale.HashDimensions,
			FitsBudget:       hashOps <= budget && int64(scale.HashDimensions)*intents <= budget*4,
			Reason:           fmt.Sprintf("hashes features into %d deterministic buckets to bound vocabulary memory", scale.HashDimensions),
		},
	}

	decision := domain.StrategyDecision{Tier: scale.Tier, Scale: scale, Candidates: candidates}

	switch scale.Tier {
	case "small":
		decision.SelectedStrategy = StrategyExactFull
		decision.Reason = fmt.Sprintf(
			"tier=%s declares %d tickets/%d intents; observed %d terms and exact estimate %d ops <= budget %d. Small tier uses the simple exact strategy and does not adopt large-tier hash machinery.",
			scale.Tier, scale.DeclaredTickets, scale.IntentCount, scale.ObservedTotalTerms, exactOps, budget,
		)
	case "medium":
		switch {
		case exactOps <= budget:
			decision.SelectedStrategy = StrategyExactFull
			decision.Reason = fmt.Sprintf("medium exact estimate %d ops fits budget %d; exact scoring is preferred", exactOps, budget)
		case hashOps <= budget && int64(scale.HashDimensions) >= 2*int64(scale.ObservedUniqueTerms):
			decision.SelectedStrategy = StrategyHashBucket
			decision.Reason = fmt.Sprintf(
				"medium exact estimate %d exceeds budget %d; hash estimate %d fits and dim=%d is >=2x unique_terms=%d, giving low-collision bounded scoring",
				exactOps, budget, hashOps, scale.HashDimensions, scale.ObservedUniqueTerms,
			)
		case vocabOps <= budget:
			decision.SelectedStrategy = StrategyVocabPriority
			decision.Reason = fmt.Sprintf(
				"medium observation: exact estimate %d ops exceeds budget %d; vocab-priority estimate %d ops fits. It preserves high-discrimination terms without hash collisions.",
				exactOps, budget, vocabOps,
			)
		case hashOps <= budget:
			decision.SelectedStrategy = StrategyHashBucket
			decision.Reason = fmt.Sprintf(
				"exact=%d and vocab=%d exceed budget=%d; hash estimate=%d fits with dim=%d",
				exactOps, vocabOps, budget, hashOps, scale.HashDimensions,
			)
		default:
			decision.SelectedStrategy = StrategyHashBucket
			decision.Reason = fmt.Sprintf(
				"all full-feature estimates exceed budget=%d; hash-bucket strategy bounds state to dim=%d and processes budgeted feature rounds",
				budget, scale.HashDimensions,
			)
		}
	default:
		switch {
		case vocabOps <= budget:
			decision.SelectedStrategy = StrategyVocabPriority
		case hashOps <= budget:
			decision.SelectedStrategy = StrategyHashBucket
		default:
			decision.SelectedStrategy = StrategyHashBucket
		}
		decision.Reason = fmt.Sprintf("tier=%s selected %s from observed scale and budget", scale.Tier, decision.SelectedStrategy)
	}

	for i := range decision.Candidates {
		candidate := &decision.Candidates[i]
		candidate.Selected = candidate.Strategy == decision.SelectedStrategy
		if candidate.Selected {
			continue
		}
		switch candidate.Strategy {
		case StrategyExactFull:
			if !candidate.FitsBudget {
				candidate.EliminationReasons = append(candidate.EliminationReasons,
					fmt.Sprintf("estimated_ops=%d > round_budget=%d", candidate.EstimatedOps, budget))
			}
			if scale.Tier == "small" {
				candidate.EliminationReasons = append(candidate.EliminationReasons,
					"not selected because another exact candidate is the intended small-tier path")
			}
		case StrategyVocabPriority:
			if scale.Tier == "small" {
				candidate.EliminationReasons = append(candidate.EliminationReasons,
					"large/medium-tier optimization is unnecessary at the declared small scale")
			}
			if !candidate.FitsBudget {
				candidate.EliminationReasons = append(candidate.EliminationReasons,
					fmt.Sprintf("estimated_ops=%d > round_budget=%d", candidate.EstimatedOps, budget))
			}
			if decision.SelectedStrategy == StrategyHashBucket {
				if !candidate.FitsBudget {
					candidate.EliminationReasons = append(candidate.EliminationReasons, "retained vocabulary exceeds the current budget")
				} else {
					candidate.EliminationReasons = append(candidate.EliminationReasons,
						"top-K truncation discards long-tail signals; low-collision hashing preserves all features within the same budget")
				}
			}
			if decision.SelectedStrategy == StrategyExactFull && scale.Tier != "small" {
				candidate.EliminationReasons = append(candidate.EliminationReasons, "approximation is unnecessary when exact scoring fits")
			}
		case StrategyHashBucket:
			if scale.Tier == "small" {
				candidate.EliminationReasons = append(candidate.EliminationReasons,
					"hash dimension/collision machinery is designed for larger bounded-budget tiers")
			}
			if decision.SelectedStrategy == StrategyVocabPriority {
				candidate.EliminationReasons = append(candidate.EliminationReasons,
					"hash collisions are avoidable because vocab-priority truncation already fits budget")
			}
			if !candidate.FitsBudget {
				candidate.EliminationReasons = append(candidate.EliminationReasons,
					fmt.Sprintf("estimated_ops=%d or model cells exceed round_budget=%d", candidate.EstimatedOps, budget))
			}
		}
		if len(candidate.EliminationReasons) == 0 {
			candidate.EliminationReasons = append(candidate.EliminationReasons, "lower-priority trade-off for this tier")
		}
	}

	if budget > 0 {
		selectedOps := candidates[0].EstimatedOps
		for _, candidate := range candidates {
			if candidate.Selected {
				selectedOps = candidate.EstimatedOps
			}
		}
		decision.BudgetUtilization = float64(selectedOps) / float64(budget)
	}
	return decision
}

func scaleBucketK(scale domain.ScaleObservation) int {
	if scale.IntentCount >= 24 {
		return 320
	}
	return 96
}
