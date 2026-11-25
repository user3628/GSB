package budget

import (
	"testing"

	"localsearch/multirecall/internal/model"
)

func budgetConfig() model.Config {
	return model.Config{
		Version: "budget-test", Seed: 1, VectorDimension: 4,
		Routes: model.RoutesConfig{
			Lexical:  model.RouteConfig{CandidateDepth: 2, EvaluationQuota: 20},
			Category: model.RouteConfig{CandidateDepth: 2, EvaluationQuota: 20},
			Vector:   model.RouteConfig{CandidateDepth: 2, EvaluationQuota: 20},
		},
		Fusion:     model.FusionConfig{Strategy: "weighted", Weights: model.RouteWeights{Lexical: 1, Category: 1, Vector: 1}, TopK: 10, DedupeByItemID: true},
		Budget:     model.BudgetConfig{GlobalCandidateEvaluations: 3, PerRoundTimeBudgetMS: 1000, EstimatedCandidateMicros: 1},
		Evaluation: model.EvaluationConfig{FusedTopK: 10},
		Execution:  model.ExecutionConfig{MaxConcurrentQueries: 2, WorkerProcesses: 2},
	}
}

func queries(count int) []model.Query {
	out := make([]model.Query, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, model.Query{QueryID: string(rune('a' + i)), Text: "x"})
	}
	return out
}

func TestBudgetExhaustionAndQuota(t *testing.T) {
	config := budgetConfig()
	plan := Build(config, 10, queries(5))
	if plan.Predicted > config.Budget.GlobalCandidateEvaluations {
		t.Fatalf("predicted %d exceeds global budget %d", plan.Predicted, config.Budget.GlobalCandidateEvaluations)
	}
	if len(plan.Unprocessed) != 2 {
		t.Fatalf("expected two unprocessed queries, got %+v", plan.Unprocessed)
	}
	if plan.Unprocessed[0].Reason != ReasonGlobalExhausted {
		t.Fatalf("unexpected unprocessed reason: %+v", plan.Unprocessed)
	}
	for _, decision := range plan.Decisions {
		for _, route := range decision.Routes {
			if route.PredictedEvaluations > config.Routes.Get(route.Route).EvaluationQuota {
				t.Fatalf("route quota exceeded: %+v", route)
			}
		}
	}
}

func TestBudgetPerRoundTimeCap(t *testing.T) {
	config := budgetConfig()
	config.Budget.GlobalCandidateEvaluations = 1000
	config.Budget.PerRoundTimeBudgetMS = 2
	config.Budget.EstimatedCandidateMicros = 1000
	plan := Build(config, 10, queries(4))
	if len(plan.Unprocessed) != 0 {
		t.Fatalf("time-capped queries should roll into later rounds, not remain unprocessed: %+v", plan.Unprocessed)
	}
	byRound := map[int]int{}
	for _, decision := range plan.Decisions {
		byRound[decision.Round] += decision.PredictedEvaluations
	}
	for round, predicted := range byRound {
		if predicted > 2 {
			t.Fatalf("round %d predicted %d evaluations, cap was 2", round, predicted)
		}
	}
	if len(byRound) < 2 {
		t.Fatalf("expected queries to be redistributed across rounds: %+v", byRound)
	}
}

func TestBudgetRouteQuotaReasons(t *testing.T) {
	config := budgetConfig()
	config.Budget.GlobalCandidateEvaluations = 1000
	config.Routes.Lexical.EvaluationQuota = 1
	plan := Build(config, 10, queries(1))
	decision := plan.Decisions[0]
	if decision.Routes[0].AppliedDepth != 1 || decision.Routes[0].RemainingRouteQuota != 1 {
		t.Fatalf("lexical quota was not applied: %+v", decision.Routes[0])
	}
	if !decision.Processed || decision.Reason != "adaptive_route_plan_selected" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}
