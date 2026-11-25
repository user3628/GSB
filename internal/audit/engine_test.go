package audit

import (
	"path/filepath"
	"testing"

	"configaudit/internal/fixedpoint"
	"configaudit/internal/parser"
	"configaudit/internal/sample"
)

func loadDataset(t *testing.T) *parser.Dataset {
	t.Helper()
	dir := t.TempDir()
	layout, err := sample.Generate(filepath.Join(dir, "samples"), sample.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := parser.Load(layout.SpecFile, layout.NodeFile, layout.Reference, layout.Sources)
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

func TestFullRunCompletenessAndBounds(t *testing.T) {
	ds := loadDataset(t)
	rep, err := NewEngine(ds).Run()
	if err != nil {
		t.Fatal(err)
	}
	if rep.CombinationCnt == 0 {
		t.Fatal("no combinations audited")
	}
	// Every reference combination must be visited exactly once.
	visited := map[string]bool{}
	for _, kr := range rep.Keys {
		for _, c := range kr.Combinations {
			if !c.WithinBound {
				t.Fatalf("item %s outside bound", c.ItemID)
			}
			if visited[c.ItemID] {
				t.Fatalf("duplicate combination %s", c.ItemID)
			}
			visited[c.ItemID] = true
		}
	}
	if len(visited) != len(ds.References) {
		t.Fatalf("visited %d combinations but references %d", len(visited), len(ds.References))
	}
}

func TestExclusiveChannelShadowsOthers(t *testing.T) {
	ds := loadDataset(t)
	rep, err := NewEngine(ds).Run()
	if err != nil {
		t.Fatal(err)
	}
	for _, kr := range rep.Keys {
		if kr.NodeID != "node-a" {
			continue
		}
		hasShadow := false
		for _, c := range kr.Contributions {
			if c.Shadowed {
				hasShadow = true
			}
		}
		if !hasShadow {
			t.Fatalf("expected shadow detail for %s/%s", kr.NodeID, kr.KeyPath)
		}
		if kr.WinnerChannel != "script" {
			t.Fatalf("winner = %s want script", kr.WinnerChannel)
		}
	}
}

func TestPriorityConflict(t *testing.T) {
	ds := loadDataset(t)
	// Force two node-b channels onto equal priority with distinct values.
	for i := range ds.Nodes {
		if ds.Nodes[i].ID == "node-b" {
			ds.Nodes[i].Channels[0].Priority = 20
			ds.Nodes[i].Channels[1].Priority = 20
		}
	}
	_, err := NewEngine(ds).Run()
	if fixedpoint.CodeOf(err) != fixedpoint.EPriorityConflict {
		t.Fatalf("code = %v err = %v", fixedpoint.CodeOf(err), err)
	}
}

func TestErrorBoundExceeded(t *testing.T) {
	ds := loadDataset(t)
	// Tighten one declared bound to zero so any non-zero deviation trips it.
	spec := ds.SpecByKey["runtime.timeout_ms"]
	spec.DeclaredBound = "0"
	ds.SpecByKey["runtime.timeout_ms"] = spec
	_, err := NewEngine(ds).Run()
	ce, ok := err.(*fixedpoint.Error)
	if !ok || ce.Code != fixedpoint.EErrorBoundExceeded || ce.ItemID == "" {
		t.Fatalf("err = %v", err)
	}
}
