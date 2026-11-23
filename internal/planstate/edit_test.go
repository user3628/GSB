package planstate_test

import (
	"testing"
	"time"

	"example.com/logicplan/editor/internal/fixtures"
	"example.com/logicplan/editor/internal/model"
	"example.com/logicplan/editor/internal/planstate"
)

func TestIncrementalEditOnlyTouchesAffectedStructures(t *testing.T) {
	state, err := planstate.NewFromSnapshot(fixtures.Current(2, 200), "jsonl", "accept", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	oldScanSummary := state.Derived.TreeSummary["scan_orders"]
	oldScanCost := state.Derived.CostEstimate.ByNode["scan_orders"]
	oldPredicateJoin := append([]string(nil), state.Derived.PredicateIndex.ByOperator["join_orders_customers"]...)
	prepared, err := planstate.PrepareEdit(state, []planstate.Operation{
		{
			Op: "set_predicate", NodeID: "filter_high_value",
			Predicate: &model.Predicate{Expr: "orders.amount > 500", Columns: []model.ColumnRef{{Qualifier: "orders", Name: "amount"}}},
		},
		{
			Op: "set_stats", NodeID: "filter_high_value",
			Stats: &model.StatsBinding{OperatorID: "filter_high_value", RowCount: 1800, Distinct: map[string]int64{"orders.amount": 1600}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	newState, err := prepared.BuildState(state, 201, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if newState.Derived.TreeSummary["scan_orders"] != oldScanSummary {
		t.Fatal("unaffected scan summary was rebuilt differently")
	}
	if newState.Derived.CostEstimate.ByNode["scan_orders"] != oldScanCost {
		t.Fatal("unaffected scan cost changed")
	}
	if got := newState.Derived.PredicateIndex.ByOperator["join_orders_customers"]; !equalStrings(got, oldPredicateJoin) {
		t.Fatalf("unaffected join predicate index changed: %#v", got)
	}
	if newState.Derived.RootHash == state.Derived.RootHash {
		t.Fatal("root summary should change after editing a predicate")
	}
	if newState.Derived.CostEstimate.ByNode["filter_high_value"].Rows != 1800 {
		t.Fatalf("filter rows = %v, want stats override 1800", newState.Derived.CostEstimate.ByNode["filter_high_value"].Rows)
	}
	if newState.Derived.PredicateIndex.ByOperator["filter_high_value"] == nil {
		t.Fatal("edited predicate is missing from predicate index")
	}
}

func TestAddAndDeletePromotesChild(t *testing.T) {
	state, err := planstate.NewFromSnapshot(fixtures.Current(2, 200), "jsonl", "accept", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := planstate.PrepareEdit(state, []planstate.Operation{
		{
			Op: "add_node", ParentID: "filter_high_value",
			Node: &model.Node{
				ID: "filter_region", Type: model.TypeFilter, Children: []string{"join_orders_customers"},
				Columns:   []model.ColumnRef{{Qualifier: "customers", Name: "region"}},
				Predicate: &model.Predicate{Expr: "customers.region = 'EU'", Columns: []model.ColumnRef{{Qualifier: "customers", Name: "region"}}},
				Stats:     &model.StatsBinding{OperatorID: "filter_region", RowCount: 3000},
			},
		},
		{Op: "set_children", NodeID: "filter_high_value", Children: []string{"filter_region"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	added, err := prepared.BuildState(state, 201, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if added.Nodes["filter_region"] == nil {
		t.Fatal("added filter missing")
	}
	if got := added.Nodes["filter_high_value"].Children; len(got) != 1 || got[0] != "filter_region" {
		t.Fatalf("filter children = %#v", got)
	}
	preparedDelete, err := planstate.PrepareEdit(added, []planstate.Operation{{Op: "delete_node", NodeID: "filter_region"}})
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := preparedDelete.BuildState(added, 202, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Nodes["filter_region"] != nil {
		t.Fatal("deleted filter still present")
	}
	if got := deleted.Nodes["filter_high_value"].Children; len(got) != 1 || got[0] != "join_orders_customers" {
		t.Fatalf("promoted child = %#v", got)
	}
	if _, exists := deleted.Derived.ColumnIndex.References["customers.region"]; exists {
		// customers.region is still referenced by the project, but filter_region's
		// reference must be gone from the values.
		for _, id := range deleted.Derived.ColumnIndex.References["customers.region"] {
			if id == "filter_region" {
				t.Fatal("column index still references deleted operator")
			}
		}
	}
	if err := planstate.DerivedInvariantError(deleted); err != nil {
		t.Fatal(err)
	}
}

func TestColumnEditRebuildsColumnIndexWithoutPredicateOrCost(t *testing.T) {
	state, err := planstate.NewFromSnapshot(fixtures.Current(2, 200), "jsonl", "accept", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	oldCost := state.Derived.CostEstimate
	oldPredicates := state.Derived.PredicateIndex
	prepared, err := planstate.PrepareEdit(state, []planstate.Operation{{
		Op: "set_columns", NodeID: "filter_high_value",
		Columns: []model.ColumnRef{{Qualifier: "orders", Name: "id"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !prepared.Kinds.ColumnIndex || prepared.Kinds.PredicateIndex || prepared.Kinds.CostEstimate || !prepared.Kinds.TreeSummary {
		t.Fatalf("unexpected selective rebuild kinds: %+v", prepared.Kinds)
	}
	next, err := prepared.BuildState(state, 201, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !contains(next.Derived.ColumnIndex.References["orders.id"], "filter_high_value") {
		t.Fatal("new column reference missing from column index")
	}
	for _, id := range next.Derived.ColumnIndex.References["orders.amount"] {
		if id == "filter_high_value" {
			t.Fatal("old column reference remains in column index")
		}
	}
	if next.Derived.CostEstimate.Total != oldCost.Total {
		t.Fatal("column-only edit unexpectedly rebuilt cost estimate")
	}
	if next.Derived.PredicateIndex.ByOperator["filter_high_value"][0] != oldPredicates.ByOperator["filter_high_value"][0] {
		t.Fatal("column-only edit unexpectedly changed predicate index")
	}
	if err := planstate.DerivedInvariantError(next); err != nil {
		t.Fatal(err)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
