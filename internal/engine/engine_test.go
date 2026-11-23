package engine_test

import (
	"bytes"
	"encoding/json"

	"testing"

	"depverify/internal/closure"
	"depverify/internal/edges"
	"depverify/internal/engine"
	"depverify/internal/manifest"
	"depverify/internal/model"
	"depverify/internal/samples"
)

func buildGraph(t *testing.T, b samples.Bundle) (*closure.Graph, []model.ParseIssue) {
	t.Helper()
	components, mErr := manifest.Parse(bytes.NewReader(b.Manifest))
	if mErr != nil && b.Kind != samples.KindTruncated && b.Kind != samples.KindBadMagic && b.Kind != samples.KindLengthOverflow {
		t.Fatalf("manifest parse: %v", mErr)
	}
	edgeList, edgeIssues := edges.Parse(bytes.NewReader(b.Edges))
	known := make(map[string]struct{}, len(components))
	compMap := make(map[string]model.Component, len(components))
	for _, c := range components {
		known[c.ID] = struct{}{}
		compMap[c.ID] = c
	}
	graphIssues, unique := edges.CheckGraph(edgeList, known)
	allIssues := append(append([]model.ParseIssue{}, edgeIssues...), graphIssues...)
	uniqueEdges := make([]model.Edge, 0, len(unique))
	for _, e := range unique {
		uniqueEdges = append(uniqueEdges, e)
	}
	return closure.NewGraph(uniqueEdges, compMap), allIssues
}

func verifyBundle(t *testing.T, b samples.Bundle, threshold int) model.ClosureResult {
	t.Helper()
	g, issues := buildGraph(t, b)
	return engine.New(g, threshold).Verify(b.Roots, 0, issues)
}

func hasCode(result model.ClosureResult, code string) bool {
	for _, c := range result.CodeSequence {
		if c == code {
			return true
		}
	}
	return false
}

func TestHealthyClosure(t *testing.T) {
	b := samples.All()[0]
	if b.Kind != samples.KindValid {
		t.Fatalf("first bundle must be valid, got %s", b.Kind)
	}
	result := verifyBundle(t, b, 32)
	if len(result.Issues) != 0 {
		t.Fatalf("healthy sample must have no issues: %+v", result.Issues)
	}
	if result.ClosureSize != 5 || result.Path != model.PathPrecise {
		t.Fatalf("closure/path wrong: %+v", result)
	}
	for _, d := range result.DigestSources {
		if !d.Match {
			t.Fatalf("digest mismatch on healthy sample: %+v", d)
		}
	}
}

func TestDowngrade(t *testing.T) {
	var b samples.Bundle
	for _, x := range samples.All() {
		if x.Kind == samples.KindDowngrade {
			b = x
		}
	}
	result := verifyBundle(t, b, 32)
	if !hasCode(result, model.EVersionDowngrade) {
		t.Fatalf("want E_VERSION_DOWNGRADE, got %v", result.CodeSequence)
	}
	for _, is := range result.Issues {
		if is.Code == model.EVersionDowngrade {
			if is.Parent != "acme/app" || is.Child != "acme/net" {
				t.Fatalf("downgrade edge wrong: %+v", is)
			}
			if model.CompareVersion(is.Actual, is.Constraint) >= 0 {
				t.Fatalf("actual must be below constraint: %v vs %v", is.Actual, is.Constraint)
			}
		}
	}
}

func TestCycle(t *testing.T) {
	var b samples.Bundle
	for _, x := range samples.All() {
		if x.Kind == samples.KindCycle {
			b = x
		}
	}
	result := verifyBundle(t, b, 32)
	if !hasCode(result, model.ECycle) {
		t.Fatalf("want E_CYCLE, got %v", result.CodeSequence)
	}
	for _, is := range result.Issues {
		if is.Code == model.ECycle {
			if len(is.CycleNodes) < 3 || len(is.CycleEdges) != len(is.CycleNodes) {
				t.Fatalf("cycle description wrong: %+v", is)
			}
		}
	}
}

func TestConstraintConflict(t *testing.T) {
	var b samples.Bundle
	for _, x := range samples.All() {
		if x.Kind == samples.KindConflict {
			b = x
		}
	}
	result := verifyBundle(t, b, 32)
	if !hasCode(result, model.EConstraintConflict) {
		t.Fatalf("want E_CONSTRAINT_CONFLICT, got %v", result.CodeSequence)
	}
	for _, is := range result.Issues {
		if is.Code == model.EConstraintConflict {
			if is.Child != "acme/util" || len(is.Constraints) != 2 || len(is.Proof) != 2 {
				t.Fatalf("conflict detail wrong: %+v", is)
			}
		}
	}
}

func TestDanglingDoesNotPolluteClosure(t *testing.T) {
	var b samples.Bundle
	for _, x := range samples.All() {
		if x.Kind == samples.KindDangling {
			b = x
		}
	}
	result := verifyBundle(t, b, 32)
	if !hasCode(result, model.EEdgeDangling) {
		t.Fatalf("want E_EDGE_DANGLING, got %v", result.CodeSequence)
	}
	for _, node := range result.Closure {
		if node == "acme/ghost" {
			t.Fatal("dangling component must not enter the closure")
		}
	}
}

func TestDualPathByteIdentical(t *testing.T) {
	for _, n := range []int{5, 33, 64, 128} {
		b := samples.Large(n)
		g, issues := buildGraph(t, b)
		// 相同阈值配置下，强制走两条内部路径，业务结论必须逐字节一致。
		precise := engine.New(g, 2).VerifyWithPath(b.Roots, 0, issues, model.PathPrecise)
		batched := engine.New(g, 2).VerifyWithPath(b.Roots, 0, issues, model.PathBatched)
		if precise.Path != model.PathPrecise || batched.Path != model.PathBatched {
			t.Fatalf("n=%d paths wrong: %s / %s", n, precise.Path, batched.Path)
		}
		strip := func(r model.ClosureResult) model.ClosureResult {
			r.Path = ""
			r.Threshold = 0
			return r
		}
		pb, _ := json.Marshal(strip(precise))
		bb, _ := json.Marshal(strip(batched))
		if string(pb) != string(bb) {
			t.Fatalf("n=%d dual path divergence:\n%s\n%s", n, pb, bb)
		}
		// 自动切换：小规模走精算，达到阈值走批量。
		autoSmall := engine.New(g, n+1).Verify(b.Roots, 0, issues)
		autoLarge := engine.New(g, 1).Verify(b.Roots, 0, issues)
		if autoSmall.Path != model.PathPrecise || autoLarge.Path != model.PathBatched {
			t.Fatalf("n=%d auto switch wrong: %s / %s", n, autoSmall.Path, autoLarge.Path)
		}
	}
}

func TestOrderIndependence(t *testing.T) {
	b := samples.All()[0]
	g, issues := buildGraph(t, b)
	r1 := engine.New(g, 32).Verify(b.Roots, 0, issues)

	// 反转边输入顺序后重新建图，结论必须一致。
	reversed := make([]model.Edge, 0, len(g.Edges))
	reversed = append(reversed, g.Edges...)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	compMap := g.Components
	g2 := closure.NewGraph(reversed, compMap)
	r2 := engine.New(g2, 32).Verify(b.Roots, 0, issues)
	a, _ := json.Marshal(r1)
	c, _ := json.Marshal(r2)
	if string(a) != string(c) {
		t.Fatalf("verdict depends on edge order:\n%s\n%s", a, c)
	}
}

func TestItemBudget(t *testing.T) {
	b := samples.Large(10)
	g, issues := buildGraph(t, b)
	result := engine.New(g, 32).Verify(b.Roots, 3, issues)
	if !hasCode(result, model.EItemBudget) {
		t.Fatalf("want E_ITEM_BUDGET, got %v", result.CodeSequence)
	}
	if result.ClosureSize != 3 {
		t.Fatalf("want closure size capped at 3, got %d", result.ClosureSize)
	}
}
