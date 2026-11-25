package model

import "testing"

func baseConfig() Config {
	return Config{
		Version: "v1", Seed: 7, VectorDimension: 4,
		Routes: RoutesConfig{
			Lexical:  RouteConfig{CandidateDepth: 3, EvaluationQuota: 10},
			Category: RouteConfig{CandidateDepth: 3, EvaluationQuota: 10},
			Vector:   RouteConfig{CandidateDepth: 3, EvaluationQuota: 10},
		},
		Fusion: FusionConfig{
			Strategy: "weighted", Weights: RouteWeights{Lexical: 1, Category: 1, Vector: 1},
			RRFK: 60, TopK: 5, DedupeByItemID: true,
		},
		Budget:     BudgetConfig{GlobalCandidateEvaluations: 100, PerRoundTimeBudgetMS: 100, EstimatedCandidateMicros: 10},
		Evaluation: EvaluationConfig{FusedTopK: 10},
		Execution:  ExecutionConfig{MaxConcurrentQueries: 2, WorkerProcesses: 2},
	}
}

func TestConfigValidationAndFingerprints(t *testing.T) {
	config := baseConfig()
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	negative := config
	negative.Fusion.Weights.Vector = -0.1
	if err := negative.Validate(); AsAppError(err).Code != CodeInvalidWeight {
		t.Fatalf("expected E_INVALID_WEIGHT, got %v", err)
	}
	baseFingerprint, _ := ConfigContentFingerprint(config)
	differentWorkers := config
	differentWorkers.Execution.WorkerProcesses = 99
	workersFingerprint, _ := ConfigContentFingerprint(differentWorkers)
	if baseFingerprint != workersFingerprint {
		t.Fatal("worker process count must not change the experiment content fingerprint")
	}
	differentWeight := config
	differentWeight.Fusion.Weights.Lexical = 0.5
	weightFingerprint, _ := ConfigContentFingerprint(differentWeight)
	if baseFingerprint == weightFingerprint {
		t.Fatal("fusion weight change must change config fingerprint")
	}
	recallA, _ := RecallConfigDigest(config)
	recallB, _ := RecallConfigDigest(differentWeight)
	if recallA != recallB {
		t.Fatal("fusion weight change must not invalidate recall stage fingerprint")
	}
}
