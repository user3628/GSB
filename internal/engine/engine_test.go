package engine

import (
	"testing"

	"elastic-scale/internal/model"
)

func TestNearestRankDeterministic(t *testing.T) {
	values := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120}
	rank, p90 := NearestRank(values, 90)
	if rank != 11 || p90 != 110 {
		t.Fatalf("rank=%d p90=%d, want 11/110", rank, p90)
	}
}

func TestBuildDecisionFixedPointBasis(t *testing.T) {
	policy := model.Policy{
		WindowMS: 60_000, TargetConcurrency: 10, LatencyBudgetMS: 100,
		MinReplicas: 1, MaxReplicas: 10, MinLatencySamples: 3, QuantilePercent: 90,
		SaturationWeightMicro: 600_000, LatencyRiskWeightMicro: 400_000,
	}.Normalize()
	svc := model.Service{ID: "api", Policy: policy}
	samples := []model.Sample{
		{ServiceID: "api", InstanceID: "api-0", Metric: MetricRequests, SampledAtMS: 1, Value: 200, CollectionWeight: 1},
		{ServiceID: "api", InstanceID: "api-0", Metric: MetricRequests, SampledAtMS: 2, Value: 400, CollectionWeight: 1},
		{ServiceID: "api", InstanceID: "api-0", Metric: MetricLatency, SampledAtMS: 3, Value: 100, CollectionWeight: 1},
		{ServiceID: "api", InstanceID: "api-0", Metric: MetricLatency, SampledAtMS: 4, Value: 200, CollectionWeight: 1},
		{ServiceID: "api", InstanceID: "api-0", Metric: MetricLatency, SampledAtMS: 5, Value: 300, CollectionWeight: 1},
	}
	decision, be := BuildDecision(svc, samples, 7)
	if be != nil {
		t.Fatalf("BuildDecision error: %v", be)
	}
	if decision.SampleSize != 3 || len(decision.Basis) != 1 {
		t.Fatalf("sample size=%d basis=%d", decision.SampleSize, len(decision.Basis))
	}
	basis := decision.Basis[0]
	if basis.QuantileRank != 3 || basis.P90LatencyMS != 300 {
		t.Fatalf("rank/p90=%d/%d want 3/300", basis.QuantileRank, basis.P90LatencyMS)
	}
	if basis.SaturationRatioMicro != 300_000 {
		t.Fatalf("saturation=%d want 300000", basis.SaturationRatioMicro)
	}
	if basis.LatencyRatioMicro != 3_000_000 {
		t.Fatalf("latency ratio=%d want 3000000", basis.LatencyRatioMicro)
	}
	if decision.LatencyDesiredReplicas != 3 || decision.DesiredReplicas != 3 {
		t.Fatalf("latencyDesired=%d desired=%d want 3", decision.LatencyDesiredReplicas, decision.DesiredReplicas)
	}
	if decision.RiskScoreMicro != 1_380_000 {
		t.Fatalf("risk=%d want 1380000", decision.RiskScoreMicro)
	}
}

func TestImpactHops(t *testing.T) {
	services := []model.Service{
		{ID: "a", Dependencies: []string{"b"}},
		{ID: "b", Dependencies: []string{"c"}},
		{ID: "c"},
	}
	state := NewState(1, services, nil)
	nodes := ImpactSet(state, "c")
	got := map[string]int{}
	for _, node := range nodes {
		got[node.ServiceID] = node.Hops
	}
	if got["b"] != 1 || got["a"] != 2 {
		t.Fatalf("impact hops=%v", got)
	}
}
