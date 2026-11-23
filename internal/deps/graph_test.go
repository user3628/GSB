package deps_test

import (
	"testing"

	"secverify/internal/config"
	"secverify/internal/deps"
	"secverify/internal/model"
	"secverify/internal/sample"
)

func account(id string, refs ...string) model.Item {
	return model.Item{
		ID: id, Kind: model.KindAccount, Refs: refs,
		Account: &model.AccountPolicy{Name: "n", Value: floatPtr(10), Unit: "count", Objective: "at_least"},
	}
}

func floatPtr(v float64) *float64 { return &v }

func TestMissingReferenceReturnsStableCodeAndEdges(t *testing.T) {
	graph := deps.Build([]model.Item{account("a", "missing")})
	bizErr := graph.Validate()
	if bizErr == nil || bizErr.Code != model.CodeDepMissing {
		t.Fatalf("unexpected result: %+v", bizErr)
	}
	if len(bizErr.Items) != 2 || len(bizErr.Edges) != 1 {
		t.Fatalf("missing evidence: items=%v edges=%v", bizErr.Items, bizErr.Edges)
	}
	if bizErr.Edges[0].From != "a" || bizErr.Edges[0].To != "missing" {
		t.Fatalf("unexpected edge: %+v", bizErr.Edges[0])
	}
}

func TestCycleReturnsMinimalCycleNotTail(t *testing.T) {
	graph := deps.Build([]model.Item{
		account("a", "b"),
		account("b", "c"),
		account("c", "a"),
		account("tail", "a"),
	})
	bizErr := graph.Validate()
	if bizErr == nil || bizErr.Code != model.CodeDepCycle {
		t.Fatalf("unexpected result: %+v", bizErr)
	}
	if len(bizErr.Items) != 3 {
		t.Fatalf("cycle items = %v, want a,b,c", bizErr.Items)
	}
	if model.ContainsString(bizErr.Items, "tail") {
		t.Fatalf("tail should not participate in minimal cycle: %v", bizErr.Items)
	}
	if len(bizErr.Edges) != 3 {
		t.Fatalf("cycle edges = %v, want three", bizErr.Edges)
	}
}

func TestSelfCycle(t *testing.T) {
	graph := deps.Build([]model.Item{account("self", "self")})
	bizErr := graph.Validate()
	if bizErr == nil || bizErr.Code != model.CodeDepCycle || len(bizErr.Items) != 1 || len(bizErr.Edges) != 1 {
		t.Fatalf("unexpected self cycle: %+v", bizErr)
	}
}

func TestInvalidationClosureAndTraceability(t *testing.T) {
	baseline, bizErr := config.ParseBaseline(sample.BaselineJSON())
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	graph := deps.Build(baseline.Items)
	if bizErr := graph.Validate(); bizErr != nil {
		t.Fatal(bizErr)
	}
	result := deps.ComputeInvalidation([]deps.TriggerSource{{ID: "acct.base.min_length", Graph: graph}})
	if len(result.Items) != 8 {
		t.Fatalf("affected items = %v, want 8", result.Items)
	}
	if len(result.Traces) != 8 {
		t.Fatalf("traces = %d, want 8", len(result.Traces))
	}
	foundDeep := false
	for _, trace := range result.Traces {
		if trace.ItemID == "reg.hklm.defender" {
			foundDeep = true
			if trace.Trigger != "acct.base.min_length" || trace.Depth != 5 || len(trace.Chain) != 5 {
				t.Fatalf("unexpected deep trace: %+v", trace)
			}
			if trace.Chain[len(trace.Chain)-1].To != "acct.base.min_length" {
				t.Fatalf("trace does not terminate at trigger: %+v", trace.Chain)
			}
		}
	}
	if !foundDeep {
		t.Fatal("deep trace for reg.hklm.defender was not emitted")
	}
}
