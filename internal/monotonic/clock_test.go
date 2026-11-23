package monotonic

import (
	"testing"

	"vpnbench/internal/model"
)

func TestFixedClockIsDeterministicAndMonotonic(t *testing.T) {
	clock := NewFixed()
	first := clock.Now()
	if got := clock.Since(first); got != 250_000 {
		t.Fatalf("duration = %v", got)
	}
	second := clock.Now()
	if !second.After(first) {
		t.Fatalf("fixed clock is not monotonic: %v then %v", first, second)
	}
	if clock.Marker() != "T_MONO_FIXED_250US_V1" {
		t.Fatalf("marker = %s", clock.Marker())
	}
}

func TestAggregateUsesNearestRankP95(t *testing.T) {
	var samples []model.LatencySample
	for i := int64(1); i <= 20; i++ {
		samples = append(samples, model.LatencySample{TunnelID: "t", ResultCode: "OK", DurationNS: i})
	}
	samples = append(samples, model.LatencySample{TunnelID: "t", ResultCode: "E_NO_TUNNEL", DurationNS: 50})
	stats := Aggregate(samples)
	if len(stats) != 2 {
		t.Fatalf("stats = %+v", stats)
	}
	if stats[0].TunnelID != "t" || stats[0].ResultCode != "E_NO_TUNNEL" || stats[0].P95NS != 50 {
		t.Fatalf("first stat = %+v", stats[0])
	}
	if stats[1].SampleCount != 20 || stats[1].AverageNS != 10 || stats[1].P95NS != 19 || stats[1].MaxNS != 20 {
		t.Fatalf("OK stat = %+v", stats[1])
	}
	again := Aggregate(samples)
	if len(again) != len(stats) || again[1].P95NS != stats[1].P95NS || again[1].AverageNS != stats[1].AverageNS {
		t.Fatalf("aggregate is not deterministic: %+v vs %+v", stats, again)
	}
}
