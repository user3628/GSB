package engine

import (
	"math"
	"testing"

	"csrouter/internal/domain"
)

func TestPlanStrategyScaleChoices(t *testing.T) {
	smallScale := domain.ScaleObservation{Tier: "small", DeclaredTickets: 400, IntentCount: 8, ObservedTotalTerms: 9000, HashDimensions: 1024, RoundBudget: 120000}
	small := PlanStrategy(smallScale, StrategyMetrics{ExactOps: 72000, VocabOps: 30000, HashOps: 25000})
	if small.SelectedStrategy != StrategyExactFull {
		t.Fatalf("small must use exact_full, got %s", small.SelectedStrategy)
	}

	mediumScale := domain.ScaleObservation{Tier: "medium", DeclaredTickets: 4000, IntentCount: 24, ObservedTotalTerms: 150000, HashDimensions: 4096, RoundBudget: 3000000}
	medium := PlanStrategy(mediumScale, StrategyMetrics{ExactOps: 3625392, VocabOps: 2645760, HashOps: 3400000})
	if medium.SelectedStrategy != StrategyVocabPriority {
		t.Fatalf("medium should use vocab priority, got %s", medium.SelectedStrategy)
	}

	tightScale := mediumScale
	tightScale.RoundBudget = 900000
	hashChoice := PlanStrategy(tightScale, StrategyMetrics{ExactOps: 3600000, VocabOps: 2200000, HashOps: 800000})
	if hashChoice.SelectedStrategy != StrategyHashBucket {
		t.Fatalf("tight budget should use hash bucket, got %s", hashChoice.SelectedStrategy)
	}
}

func TestScoreTicketRejectsZeroAndNaNSmoothing(t *testing.T) {
	policy := domain.Policy{
		IntentNames:    []string{"a", "b"},
		SmoothingAlpha: 0,
		HashDimensions: 16,
	}
	policy.Version = 3
	model := TrainModel(policy, []TrainingExample{{
		Ticket:    domain.Ticket{TicketID: "T", Intent: "a"},
		Normalize: domain.NormalizeArtifact{Tokens: []domain.Token{{Text: "x"}}},
	}})
	_, err := model.ScoreTicket(domain.Ticket{TicketID: "T"}, domain.NormalizeArtifact{Tokens: []domain.Token{{Text: "x"}}}, StrategyExactFull)
	if err == nil || err.Code != domain.ErrInvalidSmoothing || err.TicketID != "T" || err.Intent == "" || err.ConfigVersion != 3 {
		t.Fatalf("unexpected zero smoothing error: %+v", err)
	}

	policy.SmoothingAlpha = math.NaN()
	model = TrainModel(policy, []TrainingExample{{Ticket: domain.Ticket{TicketID: "T", Intent: "a"}, Normalize: domain.NormalizeArtifact{Tokens: []domain.Token{{Text: "x"}}}}})
	_, err = model.ScoreTicket(domain.Ticket{TicketID: "T"}, domain.NormalizeArtifact{Tokens: []domain.Token{{Text: "x"}}}, StrategyExactFull)
	if err == nil || err.Code != domain.ErrNonFiniteScore {
		t.Fatalf("expected non-finite score error, got %+v", err)
	}
}
