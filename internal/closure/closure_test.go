package closure

import (
	"testing"

	"depverify/internal/model"
)

func testGraph() *Graph {
	comps := map[string]model.Component{
		"a": {ID: "a", Version: []int{1, 0, 0}},
		"b": {ID: "b", Version: []int{2, 0, 0}},
		"c": {ID: "c", Version: []int{3, 0, 0}},
	}
	edges := []model.Edge{
		{Line: 1, Parent: "a", Child: "b", Constraint: []int{2, 0, 0}},
		{Line: 2, Parent: "b", Child: "c", Constraint: []int{4, 0, 0}}, // 降级：c 实际 3.0.0
		{Line: 3, Parent: "c", Child: "a", Constraint: []int{1, 0, 0}}, // 环
	}
	return NewGraph(edges, comps)
}

func TestBuildCycleSafe(t *testing.T) {
	g := testGraph()
	nodes, issues := g.Build([]string{"a"}, 0)
	if len(issues) != 0 {
		t.Fatalf("unexpected build issues: %+v", issues)
	}
	if len(nodes) != 3 {
		t.Fatalf("closure must contain a,b,c and terminate on cycle: %v", nodes)
	}
}

func TestDowngradeDetail(t *testing.T) {
	g := testGraph()
	nodes, _ := g.Build([]string{"a"}, 0)
	issues := g.Downgrades(nodes)
	if len(issues) != 1 || issues[0].Code != model.EVersionDowngrade {
		t.Fatalf("want single downgrade, got %+v", issues)
	}
	if issues[0].Parent != "b" || issues[0].Child != "c" {
		t.Fatalf("downgrade edge wrong: %+v", issues[0])
	}
}

func TestCycleCanonical(t *testing.T) {
	g := testGraph()
	nodes, _ := g.Build([]string{"a"}, 0)
	issues := g.Cycles(nodes)
	if len(issues) != 1 || issues[0].Code != model.ECycle {
		t.Fatalf("want single cycle, got %+v", issues)
	}
	wantNodes := []string{"a", "b", "c"}
	if len(issues[0].CycleNodes) != 3 {
		t.Fatalf("cycle nodes wrong: %v", issues[0].CycleNodes)
	}
	for i, n := range wantNodes {
		if issues[0].CycleNodes[i] != n {
			t.Fatalf("canonical cycle must start at smallest key a: %v", issues[0].CycleNodes)
		}
	}
	if len(issues[0].CycleEdges) != 3 {
		t.Fatalf("cycle edges must close the loop: %+v", issues[0].CycleEdges)
	}
}

func TestCompareVersion(t *testing.T) {
	cases := []struct {
		a, b []int
		want int
	}{
		{[]int{1, 2}, []int{1, 2, 0}, -1},
		{[]int{2, 0}, []int{1, 9, 9}, 1},
		{[]int{3, 1}, []int{3, 1}, 0},
		{[]int{1}, []int{1, 0, 0}, -1},
	}
	for _, c := range cases {
		if got := model.CompareVersion(c.a, c.b); got != c.want {
			t.Fatalf("CompareVersion(%v,%v)=%d want %d", c.a, c.b, got, c.want)
		}
	}
}
