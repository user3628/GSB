package recall

import (
	"math"
	"testing"

	"localsearch/multirecall/internal/model"
)

func testItems() []model.Item {
	return []model.Item{
		{ItemID: "i4", Category: "tea", Vector: []float64{0, 1}, Title: "tea house"},
		{ItemID: "i3", Category: "coffee", Vector: []float64{1, 0}, Title: "coffee latte"},
		{ItemID: "i2", Category: "dessert", Vector: []float64{1, 1}, Title: "coffee cake"},
		{ItemID: "i1", Category: "coffee", Vector: []float64{1, 0}, Title: "coffee latte"},
	}
}

func TestRecallRoutesOrderingAndEmptyInput(t *testing.T) {
	index := NewIndex(testItems())
	query := model.Query{QueryID: "q1", Text: "coffee latte", Locale: "zh-CN", ClickedCategories: []string{"coffee", "coffee", "dessert"}}
	bundle, err := index.RecallAll(query, map[string]RoutePlan{
		model.RouteLexical:  {Enabled: true, Depth: 10},
		model.RouteCategory: {Enabled: true, Depth: 10},
		model.RouteVector:   {Enabled: true, Depth: 10},
	})
	if err != nil {
		t.Fatalf("recall failed: %v", err)
	}
	if len(bundle.Routes) != 3 {
		t.Fatalf("expected 3 routes, got %d", len(bundle.Routes))
	}
	lexical := bundle.Routes[0]
	if lexical.Route != model.RouteLexical || len(lexical.Candidates) != 3 {
		t.Fatalf("unexpected lexical result: %+v", lexical)
	}
	if lexical.Candidates[0].ItemID != "i1" || lexical.Candidates[1].ItemID != "i3" {
		t.Fatalf("lexical tie-break must use item_id: %+v", lexical.Candidates)
	}
	if lexical.Candidates[0].Score != 1 || lexical.Candidates[2].Score >= lexical.Candidates[0].Score {
		t.Fatalf("lexical scores not descending: %+v", lexical.Candidates)
	}
	for _, route := range bundle.Routes {
		for i := 1; i < len(route.Candidates); i++ {
			left, right := route.Candidates[i-1], route.Candidates[i]
			if left.Score < right.Score || (left.Score == right.Score && left.ItemID > right.ItemID) {
				t.Fatalf("route %s is not deterministically sorted: %+v", route.Route, route.Candidates)
			}
		}
	}
	if _, err := index.RecallRoute(model.Query{QueryID: "empty"}, model.RouteLexical, RoutePlan{Enabled: true, Depth: 1}); errorCode(err) != model.CodeEmptyInput {
		t.Fatalf("expected E_EMPTY_INPUT for empty query, got %v", err)
	}
	if _, err := index.RecallRoute(model.Query{QueryID: "punctuation", Text: "!!!"}, model.RouteLexical, RoutePlan{Enabled: true, Depth: 1}); errorCode(err) != model.CodeEmptyInput {
		t.Fatalf("expected E_EMPTY_INPUT for token-less query, got %v", err)
	}
	emptyIndex := NewIndex(nil)
	if _, err := emptyIndex.RecallRoute(query, model.RouteLexical, RoutePlan{Enabled: true, Depth: 1}); errorCode(err) != model.CodeEmptyInput {
		t.Fatalf("expected E_EMPTY_INPUT for empty items, got %v", err)
	}
}

func TestQueryVectorDeterministic(t *testing.T) {
	query := model.Query{QueryID: "q", Text: "coffee 咖啡", Locale: "zh-CN", ClickedCategories: []string{"coffee"}}
	first := QueryVector(query, 16)
	second := QueryVector(query, 16)
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("query vector is not deterministic at %d", i)
		}
	}
	query.Locale = "en-US"
	third := QueryVector(query, 16)
	if equalVector(first, third) {
		t.Fatal("locale must participate in query features")
	}
}

func TestFusionWeightedRRFDedupeAndTruncation(t *testing.T) {
	bundle := model.RecallBundle{QueryID: "q1", Routes: []model.RouteResult{
		{Route: model.RouteLexical, Enabled: true, Candidates: []model.Candidate{{ItemID: "A", Score: 0.5}, {ItemID: "B", Score: 0.4}}},
		{Route: model.RouteCategory, Enabled: true, Candidates: []model.Candidate{{ItemID: "B", Score: 0.8}, {ItemID: "C", Score: 0.2}}},
		{Route: model.RouteVector, Enabled: true, Candidates: []model.Candidate{{ItemID: "A", Score: 0.9}, {ItemID: "C", Score: 0.1}}},
	}}
	config := model.FusionConfig{
		Strategy: "weighted", Weights: model.RouteWeights{Lexical: 1, Category: 1, Vector: 1},
		RRFK: 60, TopK: 2, DedupeByItemID: true,
	}
	fused, err := Fuse(bundle, config)
	if err != nil {
		t.Fatal(err)
	}
	if fused.ReturnedCount != 2 || !fused.Truncated || fused.TruncationReason != "top_k_reached" {
		t.Fatalf("unexpected truncation: %+v", fused)
	}
	if len(fused.Results) != 2 || fused.Results[0].ItemID != "A" || fused.Results[1].ItemID != "B" {
		t.Fatalf("unexpected fused order: %+v", fused.Results)
	}
	if math.Abs(fused.Results[0].Score-1.4) > 1e-12 || len(fused.Results[0].Sources) != 2 {
		t.Fatalf("dedupe or contribution merge failed: %+v", fused.Results[0])
	}
	seen := map[string]bool{}
	for _, item := range fused.Results {
		if seen[item.ItemID] {
			t.Fatalf("duplicate item_id in fused result: %s", item.ItemID)
		}
		seen[item.ItemID] = true
	}
	wantDetails := map[string][]string{model.RouteLexical: {"A", "B"}, model.RouteCategory: {"B", "C"}, model.RouteVector: {"A", "C"}}
	for _, detail := range fused.RouteDetails {
		if !equalStrings(detail.ItemIDs, wantDetails[detail.Route]) {
			t.Fatalf("route detail is not sorted by item_id: %+v", detail)
		}
	}
	if !VerifyFusion(bundle, config, fused) {
		t.Fatal("fusion self-consistency check failed")
	}

	config.TopK = 10
	fused, err = Fuse(bundle, config)
	if err != nil {
		t.Fatal(err)
	}
	if !fused.TopKExceedsCandidates || fused.ReturnedCount != 3 || fused.TruncationReason != "top_k_exceeds_candidates" {
		t.Fatalf("top-k larger than candidate count was not recorded: %+v", fused)
	}

	config.Strategy = "rrf"
	config.TopK = 3
	fused, err = Fuse(bundle, config)
	if err != nil {
		t.Fatal(err)
	}
	wantA := 1.0/61.0 + 1.0/61.0
	if math.Abs(fused.Results[0].Score-wantA) > 1e-12 {
		t.Fatalf("rrf contribution mismatch: got %v want %v", fused.Results[0].Score, wantA)
	}
}

func TestEvaluationMetricsAndNotApplicable(t *testing.T) {
	bundle := model.RecallBundle{QueryID: "q", Routes: []model.RouteResult{
		{Route: model.RouteLexical, Enabled: true, Candidates: []model.Candidate{{ItemID: "A", Score: 1}, {ItemID: "B", Score: 0.5}}},
		{Route: model.RouteCategory, Enabled: true, Candidates: []model.Candidate{{ItemID: "C", Score: 1}}},
		{Route: model.RouteVector, Enabled: true, Candidates: []model.Candidate{}},
	}}
	fused := model.FusionResult{QueryID: "q", Results: []model.FusedItem{{ItemID: "A"}, {ItemID: "C"}, {ItemID: "D"}}}
	judgments := []model.Judgment{
		{QueryID: "q", ItemID: "A", Relevance: 2}, {QueryID: "q", ItemID: "C", Relevance: 1},
	}
	result := Evaluate(bundle, fused, judgments, model.EvaluationConfig{FusedTopK: 10})
	if !result.FusedTop10Overlap.Applicable || result.FusedTop10Overlap.Numerator != 2 || result.FusedTop10Overlap.Denominator != 2 {
		t.Fatalf("unexpected overlap metric: %+v", result.FusedTop10Overlap)
	}
	if !result.CandidateCoverage.Applicable || result.CandidateCoverage.Value != 1 {
		t.Fatalf("unexpected coverage: %+v", result.CandidateCoverage)
	}
	if !result.AverageRankDeviation.Applicable || result.AverageRankDeviation.Denominator != 2 {
		t.Fatalf("unexpected rank deviation: %+v", result.AverageRankDeviation)
	}
	empty := Evaluate(bundle, fused, nil, model.EvaluationConfig{FusedTopK: 10})
	for name, metric := range map[string]model.Metric{
		"overlap": empty.FusedTop10Overlap, "coverage": empty.CandidateCoverage, "rank": empty.AverageRankDeviation,
	} {
		if metric.Applicable || metric.NotApplicableReason == "" {
			t.Fatalf("%s must be explicitly not-applicable: %+v", name, metric)
		}
	}
}

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	return model.AsAppError(err).Code
}

func equalVector(left, right []float64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
