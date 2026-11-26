package compiler_test

import (
	"testing"

	"netpolcompiler/internal/compiler"
	"netpolcompiler/internal/model"
	"netpolcompiler/internal/sample"
)

func compileDemo(t *testing.T) *compiler.CompiledNamespace {
	t.Helper()
	snapshot := sample.GenerateSnapshot(sample.DefaultSeed)
	view := compiler.CompileNamespace(snapshot.Version, "demo", snapshot.Rules)
	if view.RuleCount != 9 {
		t.Fatalf("expected 9 demo rules, got %d", view.RuleCount)
	}
	return view
}

func TestPortMergeAndContainment(t *testing.T) {
	view := compileDemo(t)
	if view.PortView.TotalRawCount != 9 {
		t.Fatalf("expected 9 raw demo port ranges, got %d", view.PortView.TotalRawCount)
	}

	var tcp *compiler.ProtocolPortView
	for i := range view.PortView.Protocols {
		if view.PortView.Protocols[i].Protocol == "TCP" {
			tcp = &view.PortView.Protocols[i]
		}
	}
	if tcp == nil {
		t.Fatal("missing TCP port view")
	}
	if tcp.RawCount != 7 {
		t.Fatalf("expected 7 raw TCP ranges, got %d", tcp.RawCount)
	}
	if tcp.MergedCount != 4 {
		t.Fatalf("expected 4 merged TCP ranges, got %d: %+v", tcp.MergedCount, tcp.Intervals)
	}
	if len(tcp.MergeEvents) == 0 {
		t.Fatal("expected explicit overlap/adjacency merge events")
	}

	foundContainment := false
	for _, relation := range view.SelectorIndex.Containments {
		if relation.Kind == "destination" &&
			relation.SpecificRuleID == "demo-allow-edge" &&
			relation.GeneralRuleID == "demo-allow-web-broad" &&
			relation.Relation == "strict_subset" {
			foundContainment = true
		}
	}
	if !foundContainment {
		t.Fatalf("expected preserved destination containment, got %+v", view.SelectorIndex.Containments)
	}
}

func TestPerPortAdjudication(t *testing.T) {
	view := compileDemo(t)
	result := compiler.Evaluate(view, compiler.EvaluationRequest{
		Namespace:         "demo",
		SourceLabels:      map[string]string{"role": "edge", "env": "prod"},
		DestinationLabels: map[string]string{"app": "web", "tier": "frontend"},
		Protocol:          model.ProtocolTCP,
		Ports:             []int{8083, 8087, 8092},
	})
	if len(result.PortDecisions) != 3 {
		t.Fatalf("expected 3 port decisions, got %d", len(result.PortDecisions))
	}
	expected := map[int]string{8083: "allow", 8087: "allow", 8092: "deny"}
	for _, decision := range result.PortDecisions {
		if decision.Action != expected[decision.Port] {
			t.Fatalf("port %d action=%s want %s full=%+v", decision.Port, decision.Action, expected[decision.Port], decision)
		}
		if len(decision.MatchedRuleIDs) == 0 {
			t.Fatalf("port %d missing matched rule ids", decision.Port)
		}
	}
	port8087 := result.PortDecisions[1]
	if port8087.WinnerRuleID != "demo-allow-edge" {
		t.Fatalf("same priority should preserve declaration order winner, got %s", port8087.WinnerRuleID)
	}
	if len(port8087.MatchedRuleIDs) != 2 {
		t.Fatalf("expected both overlapping rules in hit list, got %v", port8087.MatchedRuleIDs)
	}
}

func TestCompilationIsOrderIndependentAndStable(t *testing.T) {
	snapshot := sample.GenerateSnapshot(sample.DefaultSeed)
	first := compiler.CompileNamespace(1, "demo", snapshot.Rules)
	shuffled := sample.ShuffleRulesForTest(snapshot.Rules, sample.DefaultSeed)
	second := compiler.CompileNamespace(1, "demo", shuffled)
	if first.CompiledDigest != second.CompiledDigest {
		t.Fatalf("compiled digest changed after rule reorder: %s != %s", first.CompiledDigest, second.CompiledDigest)
	}
	if len(first.Rules) != len(second.Rules) {
		t.Fatalf("rule count changed: %d != %d", len(first.Rules), len(second.Rules))
	}

	request := compiler.EvaluationRequest{
		Namespace:         "demo",
		SourceLabels:      map[string]string{"role": "edge", "env": "prod"},
		DestinationLabels: map[string]string{"app": "web", "tier": "frontend"},
		Protocol:          model.ProtocolTCP,
		Ports:             []int{8083, 8087, 8092},
	}
	firstDecision := compiler.Evaluate(first, request)
	secondDecision := compiler.Evaluate(second, request)
	if firstDecision.DecisionDigest != secondDecision.DecisionDigest {
		t.Fatalf("decision digest changed after submission reorder: %s != %s", firstDecision.DecisionDigest, secondDecision.DecisionDigest)
	}
}
