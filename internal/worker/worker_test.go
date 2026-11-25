package worker

import (
	"bytes"
	"encoding/json"
	"testing"

	"localsearch/multirecall/internal/model"
)

func TestRunnerPreservesStructuredFailureContext(t *testing.T) {
	runner := NewRunner(Snapshot{
		Items: []model.Item{{ItemID: "i1", Category: "c", Vector: []float64{1}, Title: "coffee"}},
		Config: model.Config{
			Version: "v", Seed: 1, VectorDimension: 1,
			Routes: model.RoutesConfig{
				Lexical:  model.RouteConfig{CandidateDepth: 1, EvaluationQuota: 1},
				Category: model.RouteConfig{CandidateDepth: 1, EvaluationQuota: 1},
				Vector:   model.RouteConfig{CandidateDepth: 1, EvaluationQuota: 1},
			},
			Fusion:     model.FusionConfig{Strategy: "weighted", Weights: model.RouteWeights{Lexical: 1}, TopK: 1, DedupeByItemID: true},
			Evaluation: model.EvaluationConfig{FusedTopK: 10},
		},
	})
	result := runner.Execute(Task{
		Shard: 9, Query: model.Query{QueryID: "q-error", Text: "coffee"},
		StartStage: "not-a-stage",
	})
	if result.Error == nil || result.Error.Code != model.CodeIllegalStageTransition || result.Error.Shard != 9 || result.Error.QueryID != "q-error" {
		t.Fatalf("unexpected structured worker result: %+v", result)
	}
}

func TestJSONLWorkerProtocol(t *testing.T) {
	snapshot := Snapshot{
		Items: []model.Item{{ItemID: "i1", Category: "coffee", Vector: []float64{1}, Title: "coffee latte"}},
		Config: model.Config{
			Version: "v", Seed: 1, VectorDimension: 1,
			Routes: model.RoutesConfig{
				Lexical:  model.RouteConfig{CandidateDepth: 1, EvaluationQuota: 1},
				Category: model.RouteConfig{CandidateDepth: 1, EvaluationQuota: 1},
				Vector:   model.RouteConfig{CandidateDepth: 1, EvaluationQuota: 1},
			},
			Fusion:     model.FusionConfig{Strategy: "weighted", Weights: model.RouteWeights{Lexical: 1}, TopK: 1, DedupeByItemID: true},
			Evaluation: model.EvaluationConfig{FusedTopK: 10},
		},
	}
	task := Task{
		Shard: 0, Query: model.Query{QueryID: "q1", Text: "coffee"},
		Judgments: []model.Judgment{{QueryID: "q1", ItemID: "i1", Relevance: 1}},
		Plan: model.BudgetDecision{QueryID: "q1", Processed: true, Routes: []model.RouteDecision{
			{Route: model.RouteLexical, Enabled: true, AppliedDepth: 1},
			{Route: model.RouteCategory, Enabled: true, AppliedDepth: 1},
			{Route: model.RouteVector, Enabled: true, AppliedDepth: 1},
		}},
		StartStage: model.StageRecall,
	}
	line, _ := json.Marshal(task)
	input := bytes.NewReader(append(line, '\n'))
	var output bytes.Buffer
	if err := Run(input, &output, snapshot); err != nil {
		t.Fatal(err)
	}
	var result Result
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &result); err != nil {
		t.Fatal(err)
	}
	if result.Error != nil || result.Recall == nil || result.Fusion == nil || result.Evaluation == nil {
		t.Fatalf("worker protocol returned incomplete result: %+v", result)
	}
}
