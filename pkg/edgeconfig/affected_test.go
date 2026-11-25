package edgeconfig

import "testing"

func testGraph() *Graph {
	keys := []ConfigKey{
		{Path: "a.one", Type: "string"},
		{Path: "a.two", Type: "bool"},
		{Path: "b.three", Type: "number"},
	}
	nodes := []Node{
		{ID: "direct-1", Region: "east", SubscribedKeys: []string{"a.one", "a.two"}},
		{ID: "direct-2", Region: "west", SubscribedKeys: []string{"a.two"}},
		{ID: "all", Region: "south", SubscribedKeys: []string{"a.one", "a.two", "b.three"}},
		{ID: "down-1", Region: "east", DependsOn: []string{"direct-1"}},
		{ID: "down-2", Region: "north", DependsOn: []string{"down-1"}},
	}
	policy := DefaultPolicy()
	policy.Strategy.PerKeyExactMaxChangedKeys = 1
	policy.Strategy.RegionBucketMinChangedKeys = 2
	return NewGraph(keys, nodes, policy)
}

func TestAffectedPerKeyAndCascadeHops(t *testing.T) {
	graph := testGraph()
	report := graph.AnalyzeImpact(ImpactInput{ChangedKeys: []string{"a.two"}})
	if report.SelectedPath != "per_key_exact" {
		t.Fatalf("path=%s basis=%s", report.SelectedPath, report.SelectionBasis)
	}
	byID := make(map[string]ImpactedNode)
	for _, node := range report.Nodes {
		byID[node.NodeID] = node
	}
	for _, id := range []string{"direct-1", "direct-2", "all", "down-1", "down-2"} {
		if _, ok := byID[id]; !ok {
			t.Fatalf("expected %s in affected set: %+v", id, report.Nodes)
		}
	}
	if byID["direct-1"].Hop != 1 || byID["down-1"].Hop != 2 || byID["down-2"].Hop != 3 {
		t.Fatalf("unexpected hops: direct=%d down1=%d down2=%d", byID["direct-1"].Hop, byID["down-1"].Hop, byID["down-2"].Hop)
	}
	if report.Metrics.IndexLookups != 1 || report.Metrics.FullSubscriptionNodes != 1 {
		t.Fatalf("metrics=%+v", report.Metrics)
	}
}

func TestAffectedRegionBucketForMultipleKeys(t *testing.T) {
	graph := testGraph()
	report := graph.AnalyzeImpact(ImpactInput{ChangedKeys: []string{"a.one", "a.two"}})
	if report.SelectedPath != "region_bucket" {
		t.Fatalf("path=%s basis=%s", report.SelectedPath, report.SelectionBasis)
	}
	if report.Metrics.CandidateNodesScanned == 0 || report.Metrics.BucketRegions == 0 {
		t.Fatalf("region bucket metrics missing: %+v", report.Metrics)
	}
	if report.Metrics.NaiveScanProduct != 10 {
		t.Fatalf("naive product=%d", report.Metrics.NaiveScanProduct)
	}
	foundAll := false
	for _, node := range report.Nodes {
		if node.NodeID == "all" {
			foundAll = true
			if len(node.TriggerKeys) != 2 {
				t.Fatalf("full subscriber triggers=%v", node.TriggerKeys)
			}
		}
	}
	if !foundAll {
		t.Fatalf("full subscriber missing")
	}
}

func TestValidateRejectsUndeclaredSubscription(t *testing.T) {
	topology := Topology{
		Keys:  []ConfigKey{{Path: "valid.key", Type: "string"}},
		Nodes: []Node{{ID: "n1", Region: "r", Endpoint: "http://127.0.0.1:1/applied", SubscribedKeys: []string{"missing.key"}}},
	}
	err := ValidateTopology(topology)
	if err == nil || AsAPIError(err).Code != CodeUndeclaredKey {
		t.Fatalf("err=%v", err)
	}
}
