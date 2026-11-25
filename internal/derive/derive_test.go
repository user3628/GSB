package derive_test

import (
	"testing"

	"deployverify/internal/demo"
	"deployverify/internal/derive"
	"deployverify/internal/model"
)

func TestTopologyLayersAndCycleDegradation(t *testing.T) {
	env := model.Environment{Name: "staging", Units: []model.Unit{
		{Service: "a", DependsOn: []string{"b"}},
		{Service: "b", DependsOn: []string{"a"}},
		{Service: "c", DependsOn: []string{"a"}},
		{Service: "base"},
		{Service: "leaf", DependsOn: []string{"base"}},
	}}
	topology := derive.BuildTopology(env)
	if topology.Status != "degraded_dependency_cycle" {
		t.Fatalf("status = %s, want degraded", topology.Status)
	}
	if len(topology.Cycle) != 2 {
		t.Fatalf("cycle = %v, want a/b", topology.Cycle)
	}
	if len(topology.Blocked) != 3 {
		t.Fatalf("blocked = %v, want a,b,c", topology.Blocked)
	}
	if len(topology.Layers) != 2 || topology.Layers[0].Services[0] != "base" || topology.Layers[1].Services[0] != "leaf" {
		t.Fatalf("clean layers = %+v, want base then leaf", topology.Layers)
	}
}

func TestCascadeFollowsReverseDependencies(t *testing.T) {
	snapshot := demo.SnapshotV1()
	report := derive.CascadeUnits(snapshot, []derive.Change{{Ref: model.Ref{Environment: "prod", Service: "identity"}}})
	seen := map[string]bool{}
	for _, ref := range report {
		seen[ref.Service] = true
	}
	for _, service := range []string{"identity", "gateway", "billing"} {
		if !seen[service] {
			t.Fatalf("cascade %v missing %s", report, service)
		}
	}
	if seen["catalog"] {
		t.Fatalf("catalog must not depend on identity: %v", report)
	}
}

func TestBundleRefsCoverUnitsAndConfigKeys(t *testing.T) {
	snapshot := demo.SnapshotV1()
	bundle := derive.BuildBundle(snapshot, nil, nil)
	configRefs := derive.BuildConfigRefs(snapshot.Version, snapshot)
	if len(bundle.Plans) != len(snapshot.Environments[0].Units)+len(snapshot.Environments[1].Units)+len(snapshot.Environments[2].Units) {
		t.Fatalf("unexpected plan count %d", len(bundle.Plans))
	}
	if len(bundle.Topologies) != 3 || len(bundle.Scores) != 5 {
		t.Fatalf("topologies=%d scores=%d", len(bundle.Topologies), len(bundle.Scores))
	}
	found := false
	for _, ref := range configRefs {
		if ref.UnitEnv == "prod" && ref.UnitService == "identity" && ref.ConfigKey == "SESSION_TTL" && ref.ArtifactKey == "plan:prod:identity" {
			found = true
		}
	}
	if !found {
		t.Fatal("config key reference to plan artifact not found")
	}
}
