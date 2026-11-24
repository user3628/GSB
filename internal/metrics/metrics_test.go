package metrics

import "testing"

func TestAggregateDeterministic(t *testing.T) {
	c := NewCollector(8)
	// tenant t, codes A/B with known integer latencies.
	c.Observe(Sample{Seq: 1, TenantID: "t", Code: "A", LatencyNS: 100})
	c.Observe(Sample{Seq: 2, TenantID: "t", Code: "A", LatencyNS: 200})
	c.Observe(Sample{Seq: 3, TenantID: "t", Code: "A", LatencyNS: 300})
	c.Observe(Sample{Seq: 4, TenantID: "t", Code: "B", LatencyNS: 900})
	c.Stop()
	aggs := c.Aggregate()
	if len(aggs) != 2 {
		t.Fatalf("aggs=%d want 2", len(aggs))
	}
	a := aggs[0]
	if a.Code != "A" || a.Count != 3 || a.AvgNS != 200 || a.P50NS != 200 || a.MaxNS != 300 {
		t.Fatalf("unexpected agg A: %+v", a)
	}
	// nearest-rank P95 over 3 samples -> ceil(0.95*3)=3 -> max value.
	if a.P95NS != 300 {
		t.Fatalf("p95=%d want 300", a.P95NS)
	}
}
