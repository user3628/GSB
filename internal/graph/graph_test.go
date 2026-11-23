package graph_test

import (
	"testing"

	"recoveryengine/internal/graph"
	"recoveryengine/internal/model"
	"recoveryengine/internal/spec"
	"recoveryengine/samples"
)

func snapshot(t *testing.T) model.Snapshot {
	t.Helper()
	data, err := samples.Read("full-snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	raw, business := spec.DecodeSnapshot(data)
	if business != nil {
		t.Fatal(business)
	}
	normalized, business := spec.NormalizeSnapshot(raw, true)
	if business != nil {
		t.Fatal(business)
	}
	return normalized.Snapshot
}

func TestImpactAndRecoveryOrder(t *testing.T) {
	analysis := graph.Analyze(snapshot(t))
	if analysis.HasCycle {
		t.Fatal("valid sample must not contain a cycle")
	}
	impact := analysis.Impacts["gateway"]
	if impact.AffectedCount != 4 || impact.MaxHop != 3 {
		t.Fatalf("unexpected gateway impact: %#v", impact)
	}
	wantBatches := [][]string{{"gateway"}, {"telemetry"}, {"reporter"}, {"notifier"}}
	if len(impact.RecoveryBatches) != len(wantBatches) {
		t.Fatalf("unexpected recovery batches: %#v", impact.RecoveryBatches)
	}
	for i := range wantBatches {
		if impact.RecoveryBatches[i].Components[0] != wantBatches[i][0] {
			t.Fatalf("batch %d = %v, want %v", i, impact.RecoveryBatches[i].Components, wantBatches[i])
		}
	}
	if analysis.TopologyBatches["database"] >= analysis.TopologyBatches["worker"] {
		t.Fatal("dependent component must have a later batch than its dependency")
	}
}

func TestCycleReturnsPartialOrdering(t *testing.T) {
	raw := snapshot(t)
	for i := range raw.Components {
		if raw.Components[i].ID == "database" {
			raw.Components[i].DependsOn = append(raw.Components[i].DependsOn, "notifier")
		}
	}
	normalized, business := spec.NormalizeSnapshot(raw, false)
	if business != nil {
		t.Fatal(business)
	}
	analysis := graph.Analyze(normalized.Snapshot)
	if !analysis.HasCycle || len(analysis.Cycles) != 1 {
		t.Fatalf("expected one cycle, got %#v", analysis.Cycles)
	}
	if _, blocked := analysis.Blocked["database"]; !blocked {
		t.Fatal("cycle component database should be blocked")
	}
	if _, blocked := analysis.Blocked["worker"]; !blocked {
		t.Fatal("downstream worker should be blocked")
	}
	if _, blocked := analysis.Blocked["cache"]; blocked {
		t.Fatal("cache is not downstream of the cycle and must remain unblocked")
	}
	if level, ok := analysis.TopologyBatches["cache"]; !ok || level != 0 {
		t.Fatalf("unpolluted component cache should retain a batch, got %d/%t", level, ok)
	}
	impact := analysis.Impacts["cache"]
	if len(impact.RecoveryBatches) != 1 || impact.RecoveryBatches[0].Components[0] != "cache" {
		t.Fatalf("cache recovery ordering should retain the unpolluted root: %#v", impact)
	}
}
