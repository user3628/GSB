package model_test

import (
	"testing"

	"example.com/logicplan/editor/internal/bizerr"
	"example.com/logicplan/editor/internal/fixtures"
	"example.com/logicplan/editor/internal/model"
)

func TestAnalyzeFixture(t *testing.T) {
	plan, err := model.Analyze(fixtures.Current(2, 200).Nodes)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Root != "sort_amount" {
		t.Fatalf("root = %q", plan.Root)
	}
	if got := len(plan.Outputs["join_orders_customers"]); got != 5 {
		t.Fatalf("join outputs = %d, want 5", got)
	}
	if got := len(plan.Outputs["project_output"]); got != 3 {
		t.Fatalf("project outputs = %d, want 3", got)
	}
}

func TestInvalidUpstreamColumnRejected(t *testing.T) {
	nodes := fixtures.Current(2, 200).Nodes
	for _, node := range nodes {
		if node.ID == "filter_high_value" {
			node.Columns = []model.ColumnRef{{Qualifier: "missing", Name: "column"}}
		}
	}
	assertCode(t, func() error { _, err := model.Analyze(nodes); return err }, bizerr.CodePlanInvalid)
}

func TestDuplicateIDAndCycle(t *testing.T) {
	nodes := fixtures.Current(2, 200).Nodes
	nodes[1].ID = nodes[0].ID
	assertCode(t, func() error { _, err := model.Analyze(nodes); return err }, bizerr.CodePlanInvalid)

	cycle := []*model.Node{
		{ID: "a", Type: model.TypeFilter, Children: []string{"b"}, Predicate: &model.Predicate{Expr: "true"}},
		{ID: "b", Type: model.TypeFilter, Children: []string{"a"}, Predicate: &model.Predicate{Expr: "true"}},
	}
	assertCode(t, func() error { _, err := model.Analyze(cycle); return err }, bizerr.CodePlanInvalid)
}

func assertCode(t *testing.T, fn func() error, want string) {
	t.Helper()
	err := fn()
	if err == nil {
		t.Fatalf("expected %s", want)
	}
	be, ok := bizerr.As(err)
	if !ok || be.Code != want {
		t.Fatalf("error = %v, want code %s", err, want)
	}
}
