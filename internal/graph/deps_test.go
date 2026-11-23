package graph

import (
	"testing"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/model"
	"protocolfuzzlab/internal/protocol"
)

func fixture() model.CaseSet {
	seed := model.Seed{ID: "seed-a", Type: protocol.MsgHandshake, Payload: []byte("a")}
	op := model.MutationOperator{ID: "op-a", SeedID: "seed-a", Kind: "append", Arg: "x"}
	assertion := model.PathAssertion{ID: "assert-a", CaseID: "case-a", TemplateID: "seed-a", From: protocol.StateHandshake, To: protocol.StateAuth}
	tc := model.TestCase{ID: "case-a", Steps: []model.CaseStep{{SeedID: "seed-a", OperatorIDs: []string{"op-a"}}}}
	return model.NewCaseSet([]model.Seed{seed}, []model.MutationOperator{op}, []model.PathAssertion{assertion}, []model.TestCase{tc})
}

func TestDeletedAndModifiedSeedInvalidateClosure(t *testing.T) {
	cs := fixture()
	for _, action := range []string{model.ActionDelete, model.ActionModify} {
		analysis := Analyze(cs, 2, []model.Change{{Entity: model.EntitySeed, Action: action, ID: "seed-a"}})
		for _, id := range []string{"op-a", "assert-a", "case-a"} {
			kind := model.EntityOperator
			if id == "assert-a" {
				kind = model.EntityAssertion
			}
			if id == "case-a" {
				kind = model.EntityCase
			}
			diag, ok := analysis.diagnosticFor(kind, id)
			if !ok || diag.Code != codes.DepInvalidated {
				t.Fatalf("action=%s entity=%s not invalidated: %#v", action, id, diag)
			}
			if diag.Version != 2 || len(diag.RootChain) == 0 || diag.RootChain[0] != "seed:seed-a" {
				t.Fatalf("bad root cause: %#v", diag)
			}
		}
	}
}

func TestOperatorCycleIsDetectedAndPropagates(t *testing.T) {
	cs := fixture()
	cs.Operators = append(cs.Operators,
		model.MutationOperator{ID: "op-cycle-a", SeedID: "seed-a", Kind: "append", Arg: "a", DependsOn: []string{"op-cycle-b"}},
		model.MutationOperator{ID: "op-cycle-b", SeedID: "seed-a", Kind: "append", Arg: "b", DependsOn: []string{"op-cycle-a"}},
	)
	cs.Cases[0].Steps[0].OperatorIDs = append(cs.Cases[0].Steps[0].OperatorIDs, "op-cycle-a")
	cs.Normalize()
	analysis := Analyze(cs, 4, nil)
	for _, id := range []string{"op-cycle-a", "op-cycle-b"} {
		diag, ok := analysis.InvalidOperators[id]
		if !ok || diag.Code != codes.DepCycle {
			t.Fatalf("operator %s missing cycle diagnosis: %#v", id, diag)
		}
	}
	diag, ok := analysis.InvalidCases["case-a"]
	if !ok || diag.Code != codes.DepCycle {
		t.Fatalf("case did not inherit cycle: %#v", diag)
	}
}

func TestMissingReferencesAndTrace(t *testing.T) {
	cs := fixture()
	cs.Seeds = nil
	cs.Normalize()
	analysis := Analyze(cs, 3, nil)
	if diag := analysis.InvalidOperators["op-a"]; diag.Code != codes.DepInvalidated {
		t.Fatalf("missing seed diagnosis: %#v", diag)
	}
	if diag := analysis.InvalidCases["case-a"]; diag.Code != codes.DepInvalidated {
		t.Fatalf("case diagnosis: %#v", diag)
	}
	links := TraceCase(cs, "case-a")
	if len(links) == 0 {
		t.Fatal("empty dependency trace")
	}
	foundMissingSeed := false
	for _, link := range links {
		if link.FromType == model.EntityCase && link.ToType == model.EntitySeed && link.ToID == "seed-a" {
			foundMissingSeed = true
		}
	}
	if !foundMissingSeed {
		t.Fatalf("trace did not include missing seed step: %#v", links)
	}
}

func TestAssertionChangeAffectsAssociatedCase(t *testing.T) {
	before := fixture()
	after := fixture()
	after.Assertions = nil
	after.Normalize()
	cases, _ := Affected(before, after, []model.Change{{
		Entity: model.EntityAssertion, Action: model.ActionDelete, ID: "assert-a",
	}})
	if len(cases) != 1 || cases[0] != "case-a" {
		t.Fatalf("affected cases=%v", cases)
	}
}
