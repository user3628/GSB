package engine

import (
	"fmt"
	"math"

	"ocrlab/internal/config"
	"ocrlab/internal/model"
)

type SelectionInput struct {
	GroupClass      string
	ShardCount      int
	TotalCharacters int
	ExpectedBoxes   int
	BudgetUnits     int
}

func GroupClass(cfg config.StrategyConfig, shardCount int) string {
	if shardCount <= cfg.SmallMaxShards {
		return "small"
	}
	if shardCount <= cfg.MediumMaxShards {
		return "medium"
	}
	return "large"
}

func ChooseDetection(cfg config.StrategyConfig, totalBudget int, input SelectionInput) model.StrategyDecision {
	return chooseStrategy("detection", cfg.DetectionTiers, cfg, totalBudget, input)
}

func ChooseRecognition(cfg config.StrategyConfig, totalBudget int, input SelectionInput) model.StrategyDecision {
	return chooseStrategy("recognition", cfg.RecognitionTiers, cfg, totalBudget, input)
}

func chooseStrategy(stage string, tiers []config.StrategyTier, cfg config.StrategyConfig, totalBudget int, input SelectionInput) model.StrategyDecision {
	shards := maxInt(1, input.ShardCount)
	budgetPerShard := totalBudget / shards
	if budgetPerShard < 1 {
		budgetPerShard = 1
	}
	perShardChars := (input.TotalCharacters + shards - 1) / shards
	evaluations := make([]model.StrategyEvaluation, 0, len(tiers))
	bestIndex := -1
	bestScore := math.Inf(-1)
	for i, tier := range tiers {
		cost := tier.BaseCost + tier.PerCharCost*perShardChars + tier.PerBoxCost*input.ExpectedBoxes
		utilization := float64(cost) / float64(budgetPerShard)
		score := round9(tier.Quality*cfg.QualityWeight - cfg.CostWeight*utilization)
		evaluation := model.StrategyEvaluation{
			Code: tier.Code, Quality: tier.Quality, EstimatedCost: cost,
			BudgetUtilization: round9(utilization), Score: score, Admissible: true,
		}
		if input.GroupClass == "small" && cfg.SmallForbidHeavy && tier.Heavy {
			evaluation.Admissible = false
			evaluation.EliminatedReason = "small tier forbids heavy strategies"
		} else if cost > budgetPerShard {
			evaluation.Admissible = false
			evaluation.EliminatedReason = fmt.Sprintf("estimated cost %d exceeds per-shard budget %d", cost, budgetPerShard)
		}
		if evaluation.Admissible && score > bestScore+1e-9 {
			bestScore = score
			bestIndex = i
		}
		evaluations = append(evaluations, evaluation)
	}
	if bestIndex < 0 {
		// A very small compute budget must still yield a deterministic, explicit
		// choice. The cheapest tier is marked selected and diagnostics show the
		// over-budget utilization rather than silently dropping the stage.
		bestIndex = 0
		for i := 1; i < len(evaluations); i++ {
			if evaluations[i].EstimatedCost < evaluations[bestIndex].EstimatedCost {
				bestIndex = i
			}
		}
		evaluations[bestIndex].Admissible = true
		evaluations[bestIndex].EliminatedReason = ""
	}
	for i := range evaluations {
		if i == bestIndex {
			evaluations[i].Selected = true
			evaluations[i].EliminatedReason = ""
			continue
		}
		if evaluations[i].EliminatedReason == "" {
			evaluations[i].EliminatedReason = fmt.Sprintf("deterministic score %.6f is below selected %s", evaluations[i].Score, evaluations[bestIndex].Code)
		}
	}
	chosen := evaluations[bestIndex]
	reason := fmt.Sprintf("%s tier with %d shards, %d characters and %d candidate boxes; selected %s (score %.6f, cost %d, per-shard budget %d)",
		input.GroupClass, input.ShardCount, input.TotalCharacters, input.ExpectedBoxes, chosen.Code, chosen.Score, chosen.EstimatedCost, budgetPerShard)
	return model.StrategyDecision{Stage: stage, Chosen: chosen.Code, Reason: reason, Evaluations: evaluations}
}

func round9(value float64) float64 {
	return math.Round(value*1e9) / 1e9
}
